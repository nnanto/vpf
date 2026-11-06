//! Randomized model-based test: random appends, deletes, updates, schema changes and
//! flushes are applied to the store and to an in-memory Arrow model; every snapshot
//! (current and historical) must read back identical to the model.

mod common;


use arrow::array::*;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use common::*;
use parquet::basic::{Compression, Type as PhysicalType};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use vpf::ops::alter::AlterOp;
use vpf::ops::append::AppendMode;
use vpf::predicate::{Expr, literal_array, parse_assignments, parse_literal};
use vpf::schema::spec_for_type_name;
use vpf::store::Store;

fn rows(store: &Store, snap: Option<i64>) -> RecordBatch {
    read_virtual(&store.open_snapshot("f", snap).unwrap())
}

fn mask(pred: &str, b: &RecordBatch) -> BooleanArray {
    BooleanArray::from(Expr::parse(pred).unwrap().matches(b).unwrap())
}

fn model_update(b: &RecordBatch, set: &str, pred: &str) -> RecordBatch {
    let m = mask(pred, b);
    let mut cols = b.columns().to_vec();
    for (col, lit) in parse_assignments(set).unwrap() {
        let i = b.schema().index_of(&col).unwrap();
        let v = literal_array(&lit, cols[i].data_type()).unwrap();
        cols[i] = zip(&m, &Scalar::new(v), &cols[i]).unwrap();
    }
    RecordBatch::try_new(b.schema(), cols).unwrap()
}

fn model_delete(b: &RecordBatch, pred: &str) -> RecordBatch {
    let keep = arrow::compute::not(&mask(pred, b)).unwrap();
    arrow::compute::filter_record_batch(b, &keep).unwrap()
}

struct Fuzz {
    rng: StdRng,
    store: Store,
    model: RecordBatch,
    alters: Vec<AlterOp>,
    next_id: i64,
    next_col: usize,
    history: Vec<(i64, RecordBatch)>,
}

impl Fuzz {
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.rng.random_range(0..v.len())]
    }

    fn sid(&self) -> i64 {
        self.store.view("f", None).unwrap().snapshot.snapshot_id
    }

    fn non_id_columns(&self) -> Vec<(String, DataType, bool)> {
        self.model
            .schema()
            .fields()
            .iter()
            .filter(|f| f.name() != "id")
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect()
    }

    fn predicate(&mut self) -> String {
        let hi = self.next_id.max(1);
        let a = self.rng.random_range(0..hi);
        let b = a + self.rng.random_range(0..hi / 4 + 2);
        let nullable: Vec<String> =
            self.non_id_columns().into_iter().filter(|c| c.2).map(|c| format!("\"{}\"", c.0)).collect();
        let strings: Vec<String> =
            self.non_id_columns().into_iter().filter(|c| c.1 == DataType::Utf8).map(|c| c.0).collect();
        match self.rng.random_range(0..6) {
            0 => format!("id < {}", a / 8),
            1 => format!("id BETWEEN {a} AND {b}"),
            2 => {
                let ids: Vec<String> = (0..5).map(|_| self.rng.random_range(0..hi).to_string()).collect();
                format!("id IN ({})", ids.join(", "))
            }
            3 if !nullable.is_empty() => format!("{} IS NULL AND id > {a}", self.pick(&nullable).clone()),
            4 if !strings.is_empty() => format!("\"{}\" LIKE 'name-{}%'", self.pick(&strings).clone(), a % 10),
            _ => format!("id >= {} OR id = {a}", hi - self.rng.random_range(1..20)),
        }
    }

    fn literal_for(&mut self, ty: &DataType, nullable: bool) -> Option<String> {
        if nullable && self.rng.random_bool(0.2) {
            return Some("NULL".into());
        }
        let n: i32 = self.rng.random_range(-50..50);
        Some(match ty {
            DataType::Int32 | DataType::Int64 | DataType::UInt16 => n.abs().to_string(),
            DataType::Float32 | DataType::Float64 => format!("{n}.25"),
            DataType::Utf8 => format!("'v{n}'"),
            DataType::Boolean => (n > 0).to_string().to_uppercase(),
            DataType::Date32 => "'2023-05-17'".into(),
            DataType::Decimal128(..) => format!("{n}.5"),
            _ => return None,
        })
    }

    fn append(&mut self) {
        let n = if self.rng.random_bool(0.1) { self.rng.random_range(500..3000) } else { self.rng.random_range(1..60) };
        let fresh = sample_batch(self.next_id, n);
        self.next_id += n as i64;
        let expected = self.alters.iter().fold(fresh, |b, op| alter_model(&b, op));
        // Randomly omit columns that were added after creation, relying on their defaults.
        let original: Vec<String> = sample_batch(0, 1).schema().fields().iter().map(|f| f.name().clone()).collect();
        let s = expected.schema();
        let keep: Vec<usize> = (0..s.fields().len())
            .filter(|&i| original.contains(s.field(i).name()) || s.field(i).name().ends_with("_r") || self.rng.random_bool(0.5))
            .collect();
        let input = expected.project(&keep).unwrap();
        let mode = *self.pick(&[AppendMode::Buffered, AppendMode::Direct, AppendMode::Auto]);
        self.store.append("f", &input, mode).unwrap();
        self.model = concat(&[self.model.clone(), expected]);
    }

    fn alter(&mut self) -> Option<AlterOp> {
        let cols = self.non_id_columns();
        let op = match self.rng.random_range(0..6) {
            0 => {
                self.next_col += 1;
                let ty = *self.pick(&["int64", "string", "double", "int32", "boolean"]);
                let nullable = self.rng.random_bool(0.6);
                let spec = spec_for_type_name(0, &format!("c{}", self.next_col), ty, nullable).unwrap();
                let arrow_ty = match ty {
                    "int64" => DataType::Int64,
                    "string" => DataType::Utf8,
                    "double" => DataType::Float64,
                    "int32" => DataType::Int32,
                    _ => DataType::Boolean,
                };
                let default = if !nullable || self.rng.random_bool(0.5) {
                    self.literal_for(&arrow_ty, false).map(|l| parse_literal(&l).unwrap())
                } else {
                    None
                };
                AlterOp::AddColumn { spec, default }
            }
            1 if cols.len() > 2 => AlterOp::DropColumn(self.pick(&cols).0.clone()),
            2 => {
                let c = self.pick(&cols).0.clone();
                if c.ends_with("_r") {
                    return None;
                }
                AlterOp::RenameColumn { to: format!("{c}_r"), from: c }
            }
            3 => {
                let widenable: Vec<_> = cols
                    .iter()
                    .filter(|c| matches!(c.1, DataType::Int32 | DataType::Float32))
                    .cloned()
                    .collect();
                if widenable.is_empty() {
                    return None;
                }
                let (c, ty, _) = self.pick(&widenable).clone();
                let to = if ty == DataType::Int32 { PhysicalType::INT64 } else { PhysicalType::DOUBLE };
                AlterOp::Widen { column: c, to }
            }
            4 => {
                let req: Vec<_> = self.model.schema().fields().iter().filter(|f| !f.is_nullable()).map(|f| f.name().clone()).collect();
                if req.is_empty() {
                    return None;
                }
                AlterOp::MakeOptional(self.pick(&req).clone())
            }
            _ => {
                let mut names: Vec<String> = self.model.schema().fields().iter().map(|f| f.name().clone()).collect();
                for i in (1..names.len()).rev() {
                    names.swap(i, self.rng.random_range(0..=i));
                }
                AlterOp::Reorder(names)
            }
        };
        Some(op)
    }

    fn step(&mut self) -> String {
        let r = self.rng.random_range(0..100);
        let desc = if r < 30 {
            self.append();
            "append".to_string()
        } else if r < 45 {
            let p = self.predicate();
            self.store.delete_where("f", &p).unwrap();
            self.model = model_delete(&self.model, &p);
            format!("delete where {p}")
        } else if r < 65 {
            let cols = self.non_id_columns();
            let k = self.rng.random_range(1..3);
            let mut sets = Vec::new();
            for _ in 0..k {
                let (c, ty, nullable) = self.pick(&cols).clone();
                if sets.iter().any(|s: &String| s.starts_with(&format!("\"{c}\" ="))) {
                    continue;
                }
                if let Some(l) = self.literal_for(&ty, nullable) {
                    sets.push(format!("\"{c}\" = {l}"));
                }
            }
            if sets.is_empty() {
                return "skip".into();
            }
            let set = sets.join(", ");
            let p = self.predicate();
            self.store.update_where("f", &set, &p).unwrap();
            self.model = model_update(&self.model, &set, &p);
            format!("update set {set} where {p}")
        } else if r < 85 {
            let Some(op) = self.alter() else { return "skip".into() };
            self.store.alter("f", op.clone()).unwrap();
            self.model = alter_model(&self.model, &op);
            self.alters.push(op.clone());
            format!("{op:?}")
        } else {
            self.store.flush("f").unwrap();
            "flush".into()
        };
        self.history.push((self.sid(), self.model.clone()));
        desc
    }
}

fn run(seed: u64, steps: usize, codec: Compression) {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let base = sample_batch(0, 1500);
    let mut rng = StdRng::seed_from_u64(seed);
    let page_rows = rng.random_range(50..400);
    let rg_rows = rng.random_range(300..1500);
    let dict = rng.random_bool(0.5);
    let v2 = rng.random_bool(0.5);
    store.import("f", &write_parquet(&[base.clone()], props(codec, v2, dict, page_rows, rg_rows))).unwrap();
    let mut f = Fuzz { rng, store, model: base.clone(), alters: vec![], next_id: 1500, next_col: 0, history: vec![] };
    f.history.push((f.sid(), base));
    let mut log = Vec::new();
    for i in 0..steps {
        let desc = f.step();
        log.push(desc.clone());
        let got = rows(&f.store, None);
        assert_eq!(got, f.model, "seed {seed} step {i}: {desc}\nlog: {log:#?}");
    }
    let kinds = ["append", "delete", "update", "Add", "Drop", "Rename", "Widen", "MakeOptional", "Reorder", "flush", "skip"];
    let counts: Vec<String> =
        kinds.iter().map(|k| format!("{k}={}", log.iter().filter(|l| l.starts_with(k)).count())).collect();
    eprintln!("seed {seed}: rows={} cols={} {}", f.model.num_rows(), f.model.num_columns(), counts.join(" "));
    for (sid, expected) in &f.history {
        assert_eq!(&rows(&f.store, Some(*sid)), expected, "seed {seed}: time travel to snapshot {sid}");
    }
    if python().is_some() && f.model.num_columns() > 0 {
        let out = d.path().join("final.parquet");
        let exp = d.path().join("expected.parquet");
        f.store.export("f", None, &out).unwrap();
        let p = props(Compression::SNAPPY, false, true, 1000, 100_000);
        std::fs::write(&exp, write_parquet(&[f.model.clone()], p)).unwrap();
        run_python(&[&py_script("check_equal.py"), exp.to_str().unwrap(), out.to_str().unwrap()]);
    }
}

#[test]
fn fuzz_snappy() {
    let n = std::env::var("VPF_FUZZ_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(6);
    for seed in 0..n {
        run(seed, 50, Compression::SNAPPY);
    }
}

#[test]
fn fuzz_other_codecs() {
    let cs = codecs();
    for (i, c) in cs.into_iter().enumerate() {
        run(100 + i as u64, 30, c);
    }
}

#[test]
fn fuzz_long() {
    let steps = std::env::var("VPF_FUZZ_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(150);
    run(9999, steps, Compression::ZSTD(Default::default()));
}

