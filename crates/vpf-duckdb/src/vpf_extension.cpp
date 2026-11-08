// DuckDB storage extension for VPF files: ATTACH 'data.vpf' AS db (TYPE vpf).
//
// A .vpf file holds one table. Reads and writes go through the VPF C API (vpf.h) over the
// Arrow C data interface. Each DuckDB transaction opens its own handle, reads a pinned
// snapshot, and turns its first write into a VPF transaction that commits or rolls back
// together with the DuckDB transaction.

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "vpf.h"

#include <sys/stat.h>

namespace duckdb {

static void Check(int rc, const char *what) {
	if (rc == VPF_OK) {
		return;
	}
	string msg = string(what) + ": " + vpf_last_error();
	if (rc == VPF_CONFLICT) {
		throw TransactionException("VPF write conflict - %s", msg);
	}
	throw IOException("VPF %s", msg);
}

//! Rows that INSERT buffers before handing them to VPF.
static constexpr idx_t INSERT_FLUSH_ROWS = 1 << 20;

class VpfCatalog;
class VpfTableEntry;

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//
class VpfTransaction : public Transaction {
public:
	VpfTransaction(VpfCatalog &catalog, TransactionManager &manager, ClientContext &context)
	    : Transaction(manager, context), catalog(catalog) {
	}
	~VpfTransaction() override;

	static VpfTransaction &Get(ClientContext &context, Catalog &catalog) {
		return Transaction::Get(context, catalog).Cast<VpfTransaction>();
	}

	vpf_file *File();
	//! The snapshot this transaction reads: pinned at first use, moved forward by its own writes.
	int64_t Snapshot();
	//! Starts the VPF transaction (taking the file's write lock) before the first write.
	void BeginWrite();
	//! Makes later reads see this transaction's own writes.
	void AfterWrite();
	void Commit();
	void Rollback();
	optional_ptr<VpfTableEntry> GetTable();

private:
	int64_t SnapshotLocked();

	VpfCatalog &catalog;
	mutex lock;
	vpf_file *file = nullptr;
	int64_t snapshot = -1;
	bool writing = false;
	unique_ptr<VpfTableEntry> table;
	// Entries replaced after a write; bound plans may still reference them.
	vector<unique_ptr<VpfTableEntry>> retired;
};

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//
class VpfCatalog : public Catalog {
public:
	VpfCatalog(AttachedDatabase &db, string path, string codec, bool read_only, int metadata_mode)
	    : Catalog(db), path(std::move(path)), codec(std::move(codec)), read_only(read_only),
	      metadata_mode(metadata_mode) {
	}

	string path;
	string codec;
	bool read_only;
	int metadata_mode;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "vpf";
	}
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
		throw BinderException("VPF files have a single schema");
	}
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override {
		callback(*main_schema);
	}
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		auto &name = schema_lookup.GetEntryName();
		if (name == DEFAULT_SCHEMA || name == INVALID_SCHEMA) {
			return main_schema.get();
		}
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw BinderException("VPF files only have the schema \"%s\"", string(DEFAULT_SCHEMA));
	}
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		DatabaseSize size;
		struct stat st;
		if (stat(path.c_str(), &st) == 0) {
			size.bytes = idx_t(st.st_size);
		}
		return size;
	}
	bool InMemory() override {
		return false;
	}
	string GetDBPath() override {
		return path;
	}
	SchemaCatalogEntry &Main() {
		return *main_schema;
	}
	void CheckWritable() const {
		if (read_only) {
			throw PermissionException("VPF file \"%s\" is attached read-only", path);
		}
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override {
		throw BinderException("VPF files have a single schema");
	}
	unique_ptr<SchemaCatalogEntry> main_schema;
};

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//
class VpfTableEntry : public TableCatalogEntry {
public:
	VpfTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	              shared_ptr<ArrowTableSchema> arrow_table, idx_t cardinality)
	    : TableCatalogEntry(catalog, schema, info), columns(std::move(info.columns)),
	      arrow_table(std::move(arrow_table)), cardinality(cardinality) {
	}

	ColumnList columns;
	shared_ptr<ArrowTableSchema> arrow_table;
	idx_t cardinality;

	const ColumnList &GetColumns() const override {
		return columns;
	}
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		TableStorageInfo info;
		info.cardinality = cardinality;
		return info;
	}
	void BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
	                           ClientContext &) override {
	}
};

VpfTransaction::~VpfTransaction() {
	if (file) {
		vpf_close(file);
	}
}

vpf_file *VpfTransaction::File() {
	if (!file) {
		Check(vpf_open(catalog.path.c_str(), &file), "open");
		Check(vpf_set_metadata_mode(file, catalog.metadata_mode), "metadata mode");
	}
	return file;
}

int64_t VpfTransaction::SnapshotLocked() {
	if (snapshot < 0) {
		Check(vpf_snapshot(File(), &snapshot, nullptr), "snapshot");
	}
	return snapshot;
}

int64_t VpfTransaction::Snapshot() {
	lock_guard<mutex> l(lock);
	return SnapshotLocked();
}

void VpfTransaction::BeginWrite() {
	catalog.CheckWritable();
	lock_guard<mutex> l(lock);
	if (!writing) {
		Check(vpf_begin(File()), "begin");
		writing = true;
	}
}

void VpfTransaction::AfterWrite() {
	lock_guard<mutex> l(lock);
	snapshot = -1;
	if (table) {
		retired.push_back(std::move(table));
	}
}

void VpfTransaction::Commit() {
	if (writing) {
		Check(vpf_commit(file), "commit");
		writing = false;
	}
}

void VpfTransaction::Rollback() {
	if (writing) {
		writing = false;
		Check(vpf_rollback(file), "rollback");
	}
}

optional_ptr<VpfTableEntry> VpfTransaction::GetTable() {
	lock_guard<mutex> l(lock);
	if (table) {
		return table.get();
	}
	auto f = File();
	char *name = nullptr;
	Check(vpf_table_name(f, &name), "table name");
	if (!name) {
		return nullptr;
	}
	string table_name(name);
	vpf_free_string(name);

	auto snapshot_id = SnapshotLocked();
	ArrowSchemaWrapper schema;
	Check(vpf_schema(f, snapshot_id, &schema.arrow_schema), "schema");
	auto arrow_table = make_shared_ptr<ArrowTableSchema>();
	ArrowTableFunction::PopulateArrowTableSchema(*context.lock(), *arrow_table, schema.arrow_schema);
	int64_t current = 0;
	int64_t num_rows = 0;
	Check(vpf_snapshot(f, &current, &num_rows), "snapshot");

	CreateTableInfo info(catalog.Main(), Identifier(table_name));
	auto &types = arrow_table->GetTypes();
	auto &names = arrow_table->GetNames();
	for (idx_t i = 0; i < types.size(); i++) {
		info.columns.AddColumn(ColumnDefinition(Identifier(names[i]), types[i]));
		if (!(schema.arrow_schema.children[i]->flags & ARROW_FLAG_NULLABLE)) {
			info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
		}
	}
	table = make_uniq<VpfTableEntry>(catalog, catalog.Main(), info, std::move(arrow_table), idx_t(num_rows));
	return table.get();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
//! `column <op> value`, used by VPF to skip row groups and pages (see vpf_scan).
struct VpfFilter {
	idx_t column;
	int32_t op;
	Value value;
	string name;
};

struct VpfScanData : public TableFunctionData {
	VpfScanData(TableCatalogEntry &table, shared_ptr<ArrowTableSchema> arrow_table)
	    : table(table), catalog(table.ParentCatalog()), arrow_table(std::move(arrow_table)) {
	}
	TableCatalogEntry &table;
	Catalog &catalog;
	shared_ptr<ArrowTableSchema> arrow_table;
	vector<VpfFilter> filters;
};

struct VpfScanGlobal : public GlobalTableFunctionState {
	ArrowArrayStreamWrapper stream;
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static bool IsTableColumn(const VpfScanData &data, column_t id) {
	return !IsVirtualColumn(id) && id < data.arrow_table->GetTypes().size();
}

// Filter pushdown: VPF prunes with Parquet statistics, DuckDB still applies every filter.
static int32_t VpfOp(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return VPF_EQ;
	case ExpressionType::COMPARE_NOTEQUAL:
		return VPF_NE;
	case ExpressionType::COMPARE_LESSTHAN:
		return VPF_LT;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VPF_LE;
	case ExpressionType::COMPARE_GREATERTHAN:
		return VPF_GT;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VPF_GE;
	case ExpressionType::OPERATOR_IS_NULL:
		return VPF_IS_NULL;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return VPF_IS_NOT_NULL;
	default:
		return 0;
	}
}

static const char *VpfOpName(int32_t op) {
	static const char *names[] = {"", "=", "<>", "<", "<=", ">", ">=", "IS NULL", "IS NOT NULL"};
	return op >= 1 && op <= 8 ? names[op] : "?";
}

//! Types whose DuckDB ordering matches Parquet statistics ordering.
static bool Prunable(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return true;
	default:
		return false;
	}
}

//! The table column `expr` reads in `get`, if it is a plain prunable column reference.
static optional_idx FilterColumn(const LogicalGet &get, const VpfScanData &data, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return optional_idx();
	}
	auto &ref = expr.Cast<BoundColumnRefExpression>();
	if (ref.Depth() != 0 || ref.Binding().table_index != get.table_index || !Prunable(expr.GetReturnType())) {
		return optional_idx();
	}
	auto &index = get.GetColumnIndex(ref.Binding());
	if (index.IsVirtualColumn() || !IsTableColumn(data, index.GetPrimaryIndex())) {
		return optional_idx();
	}
	return index.GetPrimaryIndex();
}

static void AddComparison(const LogicalGet &get, VpfScanData &data, const Expression &column, ExpressionType type,
                          const Expression &constant) {
	auto col = FilterColumn(get, data, column);
	auto op = VpfOp(type);
	if (!col.IsValid() || op == 0 || constant.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    constant.GetReturnType() != column.GetReturnType()) {
		return;
	}
	auto &value = constant.Cast<BoundConstantExpression>().GetValue();
	if (value.IsNull()) {
		return;
	}
	data.filters.push_back({col.GetIndex(), op, value, get.names[col.GetIndex()].GetIdentifierName()});
}

static void CollectFilter(const LogicalGet &get, VpfScanData &data, const Expression &expr) {
	auto type = expr.GetExpressionType();
	if (type == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			CollectFilter(get, data, *child);
		}
		return;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION &&
	    expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR) {
		return;
	}
	if (type == ExpressionType::OPERATOR_IS_NULL || type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &children = expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION
		                     ? expr.Cast<BoundFunctionExpression>().GetChildren()
		                     : expr.Cast<BoundOperatorExpression>().GetChildren();
		if (children.size() != 1) {
			return;
		}
		auto col = FilterColumn(get, data, *children[0]);
		if (col.IsValid()) {
			data.filters.push_back({col.GetIndex(), VpfOp(type), Value(), get.names[col.GetIndex()].GetIdentifierName()});
		}
		return;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (type == ExpressionType::COMPARE_BETWEEN) {
		auto &input = BoundBetweenExpression::Input(func);
		AddComparison(get, data, input, BoundBetweenExpression::LowerComparisonType(func),
		              BoundBetweenExpression::LowerBound(func));
		AddComparison(get, data, input, BoundBetweenExpression::UpperComparisonType(func),
		              BoundBetweenExpression::UpperBound(func));
		return;
	}
	if (VpfOp(type) == 0 || func.GetChildren().size() != 2) {
		return;
	}
	ExpressionType comparison;
	auto constant = ExpressionFilter::TryGetColumnConstantComparison(func, comparison);
	if (!constant) {
		return;
	}
	auto &children = func.GetChildren();
	auto &column = children[0].get() == static_cast<const Expression *>(constant.get()) ? *children[1] : *children[0];
	AddComparison(get, data, column, comparison, *constant);
}

static void VpfPushdownFilters(ClientContext &, LogicalGet &get, FunctionData *bind_data,
                               vector<unique_ptr<Expression>> &filters) {
	// Filters stay in `filters`, so DuckDB keeps evaluating them on the pruned rows.
	auto &data = bind_data->Cast<VpfScanData>();
	for (auto &filter : filters) {
		CollectFilter(get, data, *filter);
	}
}

static InsertionOrderPreservingMap<string> VpfScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &data = input.bind_data->Cast<VpfScanData>();
	result["Table"] = data.table.name.GetIdentifierName();
	if (!data.filters.empty()) {
		string text;
		for (auto &f : data.filters) {
			text += (text.empty() ? "" : "\n") + f.name + " " + VpfOpName(f.op) +
			        (f.value.IsNull() ? "" : " " + f.value.ToSQLString());
		}
		result["VPF Pruning"] = text;
	}
	return result;
}

static unique_ptr<GlobalTableFunctionState> VpfScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<VpfScanData>();
	auto &transaction = VpfTransaction::Get(context, data.catalog);
	vector<int64_t> columns;
	for (auto id : input.column_ids) {
		columns.push_back(IsTableColumn(data, id) ? int64_t(id) : VPF_ROWID);
	}
	// Each comparison value is exported as a one-row, one-column struct array.
	auto props = context.GetClientProperties();
	vector<ArrowSchemaWrapper> schemas(data.filters.size());
	vector<ArrowArrayWrapper> values(data.filters.size());
	vector<vpf_filter> filters;
	for (idx_t i = 0; i < data.filters.size(); i++) {
		auto &f = data.filters[i];
		vpf_filter filter {int64_t(f.column), f.op, nullptr, nullptr};
		if (!f.value.IsNull()) {
			vector<LogicalType> types {f.value.type()};
			vector<string> names {"v"};
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types, 1);
			chunk.SetValue(0, 0, f.value);
			chunk.SetCardinality(1);
			ArrowConverter::ToArrowSchema(&schemas[i].arrow_schema, types, names, props);
			ArrowConverter::ToArrowArray(chunk, &values[i].arrow_array, props, {});
			filter.schema = &schemas[i].arrow_schema;
			filter.value = &values[i].arrow_array;
		}
		filters.push_back(filter);
	}
	auto result = make_uniq<VpfScanGlobal>();
	auto snapshot = transaction.Snapshot();
	Check(vpf_scan(transaction.File(), snapshot, columns.data(), columns.size(), filters.data(), filters.size(),
	               STANDARD_VECTOR_SIZE, &result->stream.arrow_array_stream),
	      "scan");
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> VpfScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state) {
	auto &data = input.bind_data->Cast<VpfScanData>();
	auto result = make_uniq<ArrowScanLocalState>(make_uniq<ArrowArrayWrapper>(), context.client);
	// ArrowToDuckDB skips ROW_ID entries: VpfScan fills virtual columns itself.
	for (auto id : input.column_ids) {
		result->column_ids.push_back(IsTableColumn(data, id) ? id : COLUMN_IDENTIFIER_ROW_ID);
	}
	return std::move(result);
}

static void VpfScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VpfScanData>();
	auto &global = input.global_state->Cast<VpfScanGlobal>();
	auto &local = input.local_state->Cast<ArrowScanLocalState>();
	if (local.chunk_offset >= idx_t(local.chunk->arrow_array.length)) {
		if (global.done) {
			return;
		}
		local.Reset();
		auto chunk = global.stream.GetNextChunk();
		while (chunk->arrow_array.release && chunk->arrow_array.length == 0) {
			chunk = global.stream.GetNextChunk();
		}
		if (!chunk->arrow_array.release) {
			global.done = true;
			return;
		}
		local.chunk = std::move(chunk);
	}
	auto &array = local.chunk->arrow_array;
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, idx_t(array.length) - local.chunk_offset);
	output.SetChildCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(local, data.arrow_table->GetColumns(), output);
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		if (local.column_ids[i] != COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		if (output.data[i].GetType().id() == LogicalTypeId::BIGINT) {
			// Pruned scans skip rows, so row ids are copied rather than assumed consecutive.
			auto &child = *array.children[i];
			auto ids = static_cast<const int64_t *>(child.buffers[1]) + child.offset + array.offset;
			output.data[i].SetVectorType(VectorType::FLAT_VECTOR);
			memcpy(FlatVector::GetDataMutable<int64_t>(output.data[i]), ids + local.chunk_offset, count * sizeof(int64_t));
		} else {
			output.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(output.data[i], true);
		}
	}
	local.chunk_offset += count;
}

TableFunction VpfTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = make_uniq<VpfScanData>(*this, arrow_table);
	TableFunction function("vpf_scan", {}, VpfScan, nullptr, VpfScanInitGlobal, VpfScanInitLocal);
	function.projection_pushdown = true;
	function.pushdown_complex_filter = VpfPushdownFilters;
	function.to_string = VpfScanToString;
	function.get_table_entry = [](optional_ptr<const FunctionData> bind_data) -> optional_ptr<TableCatalogEntry> {
		return &bind_data->Cast<VpfScanData>().table;
	};
	return function;
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
class VpfSchemaEntry : public SchemaCatalogEntry {
public:
	VpfSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
	}

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup) override {
		if (lookup.GetCatalogType() != CatalogType::TABLE_ENTRY) {
			return nullptr;
		}
		auto table = transaction.transaction->Cast<VpfTransaction>().GetTable();
		if (!table || !StringUtil::CIEquals(table->name.GetIdentifierName(), lookup.GetEntryName())) {
			return nullptr;
		}
		return table.get();
	}
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
		if (type != CatalogType::TABLE_ENTRY) {
			return;
		}
		auto table = VpfTransaction::Get(context, catalog).GetTable();
		if (table) {
			callback(*table);
		}
	}
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
	}
	void DropEntry(ClientContext &context, DropInfo &info) override {
		throw NotImplementedException("VPF does not support DROP - delete the .vpf file instead");
	}
	void Alter(CatalogTransaction transaction, AlterInfo &info) override {
		throw NotImplementedException("VPF does not support ALTER");
	}
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction, CreateFunctionInfo &) override {
		throw NotImplementedException("VPF does not support functions");
	}
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) override {
		throw NotImplementedException("VPF does not support indexes");
	}
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction, CreateViewInfo &) override {
		throw NotImplementedException("VPF does not support views");
	}
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction, CreateSequenceInfo &) override {
		throw NotImplementedException("VPF does not support sequences");
	}
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) override {
		throw NotImplementedException("VPF does not support table functions");
	}
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) override {
		throw NotImplementedException("VPF does not support copy functions");
	}
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) override {
		throw NotImplementedException("VPF does not support pragma functions");
	}
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction, CreateCollationInfo &) override {
		throw NotImplementedException("VPF does not support collations");
	}
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction, CreateTypeInfo &) override {
		throw NotImplementedException("VPF does not support types");
	}
};

optional_ptr<CatalogEntry> VpfSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &vpf = catalog.Cast<VpfCatalog>();
	auto &txn = transaction.transaction->Cast<VpfTransaction>();
	auto &base = info.Base();
	auto existing = txn.GetTable();
	if (existing) {
		if (base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT &&
		    StringUtil::CIEquals(existing->name.GetIdentifierName(), base.GetTableName().GetIdentifierName())) {
			return existing.get();
		}
		throw CatalogException("\"%s\" already holds table \"%s\" (a VPF file holds one table)", vpf.path,
		                       existing->name.GetIdentifierName());
	}
	if (base.columns.LogicalColumnCount() != base.columns.PhysicalColumnCount()) {
		throw NotImplementedException("VPF does not support generated columns");
	}
	vector<LogicalType> types;
	vector<string> names;
	for (auto &col : base.columns.Physical()) {
		types.push_back(col.GetType());
		names.push_back(col.GetName().GetIdentifierName());
	}
	vector<bool> not_null(types.size(), false);
	for (auto &constraint : base.constraints) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			throw NotImplementedException("VPF supports only NOT NULL constraints");
		}
		not_null[constraint->Cast<NotNullConstraint>().index.index] = true;
	}
	auto props = transaction.GetContext().GetClientProperties();
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, types, names, props);
	for (idx_t i = 0; i < types.size(); i++) {
		if (not_null[i]) {
			schema.arrow_schema.children[i]->flags &= ~ARROW_FLAG_NULLABLE;
		}
	}
	txn.BeginWrite();
	Check(vpf_create(txn.File(), base.GetTableName().GetIdentifierName().c_str(), &schema.arrow_schema,
	                 vpf.codec.empty() ? nullptr : vpf.codec.c_str()),
	      "create table");
	txn.AfterWrite();
	return txn.GetTable().get();
}

void VpfCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	main_schema = make_uniq<VpfSchemaEntry>(*this, info);
}

//===--------------------------------------------------------------------===//
// Write operators
//===--------------------------------------------------------------------===//
static void ReturnCount(DataChunk &chunk, idx_t count) {
	chunk.SetChildCardinality(1);
	chunk.data[0].SetValue(0, Value::BIGINT(int64_t(count)));
	FlatVector::SetSize(chunk.data[0], count_t(1));
}

//! DataChunks converted to Arrow arrays (of one schema), waiting to be handed to VPF.
struct ArrowBuffer {
	ArrowSchemaWrapper schema;
	vector<ArrowArray> arrays;
	idx_t rows = 0;

	ArrowBuffer() = default;
	ArrowBuffer(const ArrowBuffer &) = delete;
	~ArrowBuffer() {
		for (auto &array : arrays) {
			if (array.release) {
				array.release(&array);
			}
		}
	}
	void Init(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names) {
		auto props = context.GetClientProperties();
		ArrowConverter::ToArrowSchema(&schema.arrow_schema, types, names, props);
	}
	void Add(ClientContext &context, DataChunk &chunk) {
		ArrowArray array;
		ArrowConverter::ToArrowArray(chunk, &array, context.GetClientProperties(), {});
		arrays.push_back(array);
		rows += chunk.size();
	}
	//! For the VPF calls, which take ownership of the arrays (even when they fail).
	void Released() {
		arrays.clear();
		rows = 0;
	}
};

class VpfInsertState : public GlobalSinkState {
public:
	VpfTableEntry *table = nullptr;
	ArrowBuffer buffer;
	idx_t count = 0;

	void Flush(ClientContext &context) {
		if (buffer.arrays.empty()) {
			return;
		}
		auto &txn = VpfTransaction::Get(context, table->catalog);
		uint64_t appended = 0;
		auto rc = vpf_append(txn.File(), &buffer.schema.arrow_schema, buffer.arrays.data(), buffer.arrays.size(),
		                     &appended);
		buffer.Released();
		Check(rc, "insert");
		count += appended;
	}
};

class VpfInsert : public PhysicalOperator {
public:
	VpfInsert(PhysicalPlan &plan, LogicalOperator &op, TableCatalogEntry &table)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table) {
	}
	VpfInsert(PhysicalPlan &plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	          unique_ptr<BoundCreateTableInfo> info)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, 1), schema(&schema),
	      info(std::move(info)) {
	}

	optional_ptr<TableCatalogEntry> table;
	optional_ptr<SchemaCatalogEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto state = make_uniq<VpfInsertState>();
		if (table) {
			state->table = &table.get_mutable()->Cast<VpfTableEntry>();
		} else {
			auto &s = *schema.get_mutable();
			state->table = &s.CreateTable(s.GetCatalogTransaction(context), *info)->Cast<VpfTableEntry>();
		}
		VpfTransaction::Get(context, state->table->catalog).BeginWrite();
		vector<string> names;
		for (auto &col : state->table->GetColumns().Physical()) {
			names.push_back(col.GetName().GetIdentifierName());
		}
		state->buffer.Init(context, state->table->GetTypes(), names);
		return std::move(state);
	}
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<VpfInsertState>();
		state.buffer.Add(context.client, chunk);
		if (state.buffer.rows >= INSERT_FLUSH_ROWS) {
			state.Flush(context.client);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &state = input.global_state.Cast<VpfInsertState>();
		auto &txn = VpfTransaction::Get(context, state.table->catalog);
		state.Flush(context);
		txn.AfterWrite();
		return SinkFinalizeType::READY;
	}
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		ReturnCount(chunk, sink_state->Cast<VpfInsertState>().count);
		return SourceResultType::FINISHED;
	}
	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	string GetName() const override {
		return table ? "VPF_INSERT" : "VPF_CREATE_TABLE_AS";
	}
};

class VpfDeleteState : public GlobalSinkState {
public:
	vector<int64_t> rows;
	idx_t count = 0;
};

class VpfDelete : public PhysicalOperator {
public:
	VpfDelete(PhysicalPlan &plan, LogicalOperator &op, TableCatalogEntry &table, idx_t row_id_index)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
	      row_id_index(row_id_index) {
	}

	TableCatalogEntry &table;
	idx_t row_id_index;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<VpfDeleteState>();
	}
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<VpfDeleteState>();
		chunk.Flatten();
		auto ids = FlatVector::GetData<int64_t>(chunk.data[row_id_index]);
		state.rows.insert(state.rows.end(), ids, ids + chunk.size());
		return SinkResultType::NEED_MORE_INPUT;
	}
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &state = input.global_state.Cast<VpfDeleteState>();
		if (state.rows.empty()) {
			return SinkFinalizeType::READY;
		}
		auto &txn = VpfTransaction::Get(context, table.catalog);
		auto base = txn.Snapshot();
		txn.BeginWrite();
		uint64_t deleted = 0;
		Check(vpf_delete_rows(txn.File(), base, state.rows.data(), state.rows.size(), &deleted), "delete");
		state.count = deleted;
		txn.AfterWrite();
		return SinkFinalizeType::READY;
	}
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		ReturnCount(chunk, sink_state->Cast<VpfDeleteState>().count);
		return SourceResultType::FINISHED;
	}
	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	string GetName() const override {
		return "VPF_DELETE";
	}
};

class VpfUpdateState : public GlobalSinkState {
public:
	ArrowBuffer values;
	vector<int64_t> rows;
	unordered_set<int64_t> seen;
	idx_t count = 0;
};

class VpfUpdate : public PhysicalOperator {
public:
	VpfUpdate(PhysicalPlan &plan, LogicalOperator &op, TableCatalogEntry &table, vector<PhysicalIndex> columns)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
	      columns(std::move(columns)) {
	}

	TableCatalogEntry &table;
	vector<PhysicalIndex> columns;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto state = make_uniq<VpfUpdateState>();
		vector<LogicalType> types;
		vector<string> names;
		for (auto &index : columns) {
			auto &col = table.GetColumns().GetColumn(index);
			types.push_back(col.GetType());
			names.push_back(col.GetName().GetIdentifierName());
		}
		state->values.Init(context, types, names);
		return std::move(state);
	}
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &state = input.global_state.Cast<VpfUpdateState>();
		if (chunk.ColumnCount() != columns.size() + 1) {
			throw InternalException("VPF update: expected %llu input columns, got %llu", columns.size() + 1,
			                        chunk.ColumnCount());
		}
		chunk.Flatten();
		auto ids = FlatVector::GetData<int64_t>(chunk.data[columns.size()]);
		// UPDATE ... FROM can match a row more than once: the first match wins.
		SelectionVector sel(chunk.size());
		idx_t kept = 0;
		for (idx_t i = 0; i < chunk.size(); i++) {
			if (state.seen.insert(ids[i]).second) {
				state.rows.push_back(ids[i]);
				sel.set_index(kept++, i);
			}
		}
		if (kept == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		vector<column_t> value_columns;
		vector<LogicalType> value_types;
		for (idx_t i = 0; i < columns.size(); i++) {
			value_columns.push_back(i);
			value_types.push_back(chunk.data[i].GetType());
		}
		DataChunk values;
		values.InitializeEmpty(value_types);
		values.ReferenceColumns(chunk, value_columns);
		if (kept < chunk.size()) {
			values.Slice(sel, kept);
			values.Flatten();
		}
		state.values.Add(context.client, values);
		return SinkResultType::NEED_MORE_INPUT;
	}
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &state = input.global_state.Cast<VpfUpdateState>();
		if (state.rows.empty()) {
			return SinkFinalizeType::READY;
		}
		auto &txn = VpfTransaction::Get(context, table.catalog);
		auto base = txn.Snapshot();
		txn.BeginWrite();
		uint64_t updated = 0;
		auto &values = state.values;
		auto rc = vpf_update_rows(txn.File(), base, state.rows.data(), state.rows.size(), &values.schema.arrow_schema,
		                          values.arrays.data(), values.arrays.size(), &updated);
		values.Released();
		Check(rc, "update");
		state.count = updated;
		txn.AfterWrite();
		return SinkFinalizeType::READY;
	}
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		ReturnCount(chunk, sink_state->Cast<VpfUpdateState>().count);
		return SourceResultType::FINISHED;
	}
	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	string GetName() const override {
		return "VPF_UPDATE";
	}
};

PhysicalOperator &VpfCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	CheckWritable();
	if (op.return_chunk) {
		throw BinderException("RETURNING is not supported for VPF tables");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT is not supported for VPF tables");
	}
	if (!plan) {
		throw InternalException("VPF insert without a child plan");
	}
	auto &insert = planner.Make<VpfInsert>(op, op.table);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &VpfCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	CheckWritable();
	auto &insert = planner.Make<VpfInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &VpfCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	CheckWritable();
	if (op.return_chunk) {
		throw BinderException("RETURNING is not supported for VPF tables");
	}
	auto &row_id = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto &del = planner.Make<VpfDelete>(op, op.table, row_id.Index());
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &VpfCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	CheckWritable();
	if (op.return_chunk) {
		throw BinderException("RETURNING is not supported for VPF tables");
	}
	for (auto &expr : op.expressions) {
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not supported for VPF tables");
		}
	}
	auto &update = planner.Make<VpfUpdate>(op, op.table, op.columns);
	update.children.push_back(plan);
	return update;
}

//===--------------------------------------------------------------------===//
// Transaction manager
//===--------------------------------------------------------------------===//
class VpfTransactionManager : public TransactionManager {
public:
	VpfTransactionManager(AttachedDatabase &db, VpfCatalog &catalog) : TransactionManager(db), catalog(catalog) {
	}

	Transaction &StartTransaction(ClientContext &context) override {
		auto transaction = make_uniq<VpfTransaction>(catalog, *this, context);
		auto &result = *transaction;
		lock_guard<mutex> l(lock);
		transactions[result] = std::move(transaction);
		return result;
	}
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override {
		ErrorData error;
		auto &txn = transaction.Cast<VpfTransaction>();
		try {
			txn.Commit();
		} catch (std::exception &ex) {
			error = ErrorData(ex);
			try {
				txn.Rollback();
			} catch (std::exception &) {
			}
		}
		Remove(transaction);
		return error;
	}
	void RollbackTransaction(Transaction &transaction) override {
		auto &txn = transaction.Cast<VpfTransaction>();
		try {
			txn.Rollback();
		} catch (...) {
			Remove(transaction);
			throw;
		}
		Remove(transaction);
	}
	void Checkpoint(ClientContext &context, bool force = false) override {
	}

private:
	//! Closes the transaction's handle outside the lock.
	void Remove(Transaction &transaction) {
		unique_ptr<VpfTransaction> closing;
		lock_guard<mutex> l(lock);
		auto entry = transactions.find(transaction);
		if (entry != transactions.end()) {
			closing = std::move(entry->second);
			transactions.erase(entry);
		}
	}

	VpfCatalog &catalog;
	mutex lock;
	reference_map_t<Transaction, unique_ptr<VpfTransaction>> transactions;
};

//===--------------------------------------------------------------------===//
// Storage extension
//===--------------------------------------------------------------------===//
static unique_ptr<Catalog> VpfAttach(optional_ptr<StorageExtensionInfo>, ClientContext &context, AttachedDatabase &db,
                                     const string &name, AttachInfo &info, AttachOptions &options) {
	string codec;
	int metadata_mode = VPF_METADATA_LAZY;
	for (auto &entry : options.options) {
		if (StringUtil::CIEquals(entry.first, "codec")) {
			codec = entry.second.ToString();
		} else if (StringUtil::CIEquals(entry.first, "metadata_mode")) {
			auto mode = entry.second.ToString();
			if (StringUtil::CIEquals(mode, "lazy")) {
				metadata_mode = VPF_METADATA_LAZY;
			} else if (StringUtil::CIEquals(mode, "eager")) {
				metadata_mode = VPF_METADATA_EAGER;
			} else {
				throw BinderException("VPF METADATA_MODE must be 'lazy' or 'eager'");
			}
		} else {
			throw NotImplementedException("Unsupported option for VPF ATTACH: %s", entry.first);
		}
	}
	auto read_only = options.access_mode == AccessMode::READ_ONLY;
	if (!read_only) {
		// Creates the file if it does not exist, and reports a bad path at ATTACH.
		vpf_file *file = nullptr;
		Check(vpf_open(info.path.c_str(), &file), "open");
		vpf_close(file);
	}
	return make_uniq<VpfCatalog>(db, info.path, codec, read_only, metadata_mode);
}

static unique_ptr<TransactionManager> VpfCreateTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<VpfTransactionManager>(db, catalog.Cast<VpfCatalog>());
}

class VpfStorageExtension : public StorageExtension {
public:
	VpfStorageExtension() {
		attach = VpfAttach;
		create_transaction_manager = VpfCreateTransactionManager;
	}
};

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(vpf, loader) {
	auto &config = duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance());
	duckdb::StorageExtension::Register(config, "vpf", duckdb::make_shared_ptr<duckdb::VpfStorageExtension>());
}
}
