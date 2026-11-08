/*
 * vpf.h - C API for VPF files.
 *
 * A VPF file (`*.vpf`) is a single file holding one table, like a one-table SQLite
 * database. Data crosses this API as Arrow (C data / C stream interface), so any engine
 * that speaks Arrow can read and write it.
 *
 * Conventions
 *  - Functions returning int return VPF_OK (0) on success, or an error code; the message
 *    is available from vpf_last_error() on the same thread until the next call.
 *  - Row ids are 0-based positions in a snapshot (file order). Writes by row id name the
 *    snapshot the ids were read from and fail with VPF_CONFLICT if the table changed since.
 *  - A handle is not thread-safe for writes; streams produced by vpf_scan may be consumed
 *    on any thread and stay valid (pinned to their snapshot) after other writes.
 */
#ifndef VPF_H
#define VPF_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4
struct ArrowSchema {
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;
	void (*release)(struct ArrowSchema *);
	void *private_data;
};
struct ArrowArray {
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;
	void (*release)(struct ArrowArray *);
	void *private_data;
};
#endif

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE
struct ArrowArrayStream {
	int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
	int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);
	const char *(*get_last_error)(struct ArrowArrayStream *);
	void (*release)(struct ArrowArrayStream *);
	void *private_data;
};
#endif

#define VPF_OK 0
#define VPF_ERROR 1
#define VPF_CONFLICT 2
#define VPF_METADATA_LAZY 0
#define VPF_METADATA_EAGER 1

/* Column index that selects the row id (int64) in vpf_scan. */
#define VPF_ROWID (-1)

/* Filter operators (vpf_filter.op). Comparisons follow SQL: NULLs never match. */
#define VPF_EQ 1
#define VPF_NE 2
#define VPF_LT 3
#define VPF_LE 4
#define VPF_GT 5
#define VPF_GE 6
#define VPF_IS_NULL 7
#define VPF_IS_NOT_NULL 8
#define VPF_AND 9
#define VPF_OR 10

/* `column <op> value` on a table column. For comparisons, `value` is a one-row struct
 * array with one field (e.g. an exported one-column batch) described by `schema`; both
 * are NULL for VPF_IS_NULL / VPF_IS_NOT_NULL. Both scan functions take ownership
 * of `value`. */
typedef struct vpf_filter {
	int64_t column;
	int32_t op;
	const struct ArrowSchema *schema;
	struct ArrowArray *value;
} vpf_filter;

typedef struct vpf_file vpf_file;

/* Last error message of this thread (never NULL). */
const char *vpf_last_error(void);
void vpf_free_string(char *s);

/* Opens a .vpf file, creating an empty one if it does not exist. */
int vpf_open(const char *path, vpf_file **out);
void vpf_close(vpf_file *file);

/* Per-handle write policy (default LAZY). EAGER persists each new snapshot's footer
 * and offset mapping atomically with its data. Either mode reads existing layouts.
 * Changing the mode applies to subsequent writes, not existing snapshots. */
int vpf_set_metadata_mode(vpf_file *file, int mode);

/* The table's name (free with vpf_free_string), or NULL if no table was created yet. */
int vpf_table_name(vpf_file *file, char **out);

/* Current snapshot id and row count. Fails if there is no table. */
int vpf_snapshot(vpf_file *file, int64_t *snapshot_id, int64_t *num_rows);

/* Arrow schema (a struct of the table's columns) of a snapshot (-1 = current).
 * The caller releases `out`. */
int vpf_schema(vpf_file *file, int64_t snapshot_id, struct ArrowSchema *out);

/* Creates the file's table. `schema` is a struct schema; non-nullable fields become
 * required columns. `codec` is e.g. "zstd", "snappy", "none" (NULL = zstd). The caller
 * keeps ownership of `schema`. */
int vpf_create(vpf_file *file, const char *name, const struct ArrowSchema *schema, const char *codec);

/* Streams a snapshot (-1 = current). `columns` lists table column indexes in output
 * order (VPF_ROWID for the row id); NULL selects all columns. Batches have at most
 * `batch_rows` rows (0 = 2048). The caller releases `out`.
 *
 * `filters` (ANDed, may be NULL) let the scan skip row groups and pages whose statistics
 * show that no row can match. This is pruning, not filtering: returned rows may still
 * fail the filters, so apply them to the result. Row ids remain snapshot positions. */
int vpf_scan(vpf_file *file, int64_t snapshot_id, const int64_t *columns, size_t n_columns,
             const vpf_filter *filters, size_t n_filters, size_t batch_rows, struct ArrowArrayStream *out);

/* Like vpf_scan, but `filters` is a postfix expression: each comparison/null check
 * pushes a predicate; VPF_AND / VPF_OR combine the preceding two predicates.
 * Boolean operators ignore `column` and require NULL `schema` and `value`.
 * A nonempty expression must leave exactly one predicate. NULL or zero filters
 * means no pruning. Unknown statistics conservatively retain candidate rows. */
int vpf_scan_expr(vpf_file *file, int64_t snapshot_id, const int64_t *columns, size_t n_columns,
                  const vpf_filter *filters, size_t n_filters, size_t batch_rows, struct ArrowArrayStream *out);

/* Appends rows. `arrays` are struct arrays (one per batch) matching `schema`, whose
 * columns map to the table's columns by position and are cast to the table's types.
 * Takes ownership of every array. */
int vpf_append(vpf_file *file, const struct ArrowSchema *schema, struct ArrowArray *arrays, size_t n_arrays,
               uint64_t *rows_appended);

/* Deletes rows (by id) of snapshot `base`. Duplicate ids are ignored. */
int vpf_delete_rows(vpf_file *file, int64_t base, const int64_t *rows, size_t n_rows, uint64_t *rows_deleted);

/* Sets columns on rows (by id) of snapshot `base`. `arrays` are struct arrays whose
 * fields are named after the table columns to set; together they hold one row per id,
 * in the order of `rows`. Takes ownership of every array. */
int vpf_update_rows(vpf_file *file, int64_t base, const int64_t *rows, size_t n_rows,
                    const struct ArrowSchema *schema, struct ArrowArray *arrays, size_t n_arrays,
                    uint64_t *rows_updated);

/* Explicit transactions: begin takes the file's write lock; until commit or rollback,
 * reads and writes through this handle see the transaction's changes and other
 * handles do not. */
int vpf_begin(vpf_file *file);
int vpf_commit(vpf_file *file);
int vpf_rollback(vpf_file *file);

#ifdef __cplusplus
}
#endif

#endif /* VPF_H */
