#[path = "../../../tests/common/mod.rs"]
mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use common::*;
use parquet::basic::Compression;
use tower::ServiceExt;
use vpf::ops::append::AppendMode;
use vpf::store::Store;

fn fixture() -> (tempfile::TempDir, Arc<Store>, i64) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let imported = store
        .import(
            "f",
            &write_parquet(
                &[sample_batch(0, 1000)],
                props(Compression::SNAPPY, false, true, 100, 400),
            ),
        )
        .unwrap();
    (dir, store, imported.snapshot_id)
}

#[tokio::test]
async fn http_ranges_and_snapshot_consistency() {
    let (_dir, store, id) = fixture();
    let path = format!("/files/f/snapshots/{id}.parquet");
    let file = store.open_snapshot("f", Some(id)).unwrap();
    let expected = file.to_bytes().unwrap();
    let app = vpf_http::router(store.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(&path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-length"],
        file.size().to_string()
    );
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    for (range, start, end) in [
        ("bytes=0-3", 0, 4),
        ("bytes=-8", expected.len() - 8, expected.len()),
        ("bytes=200-299", 200, 300),
        ("bytes=100-", 100, expected.len()),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header("range", range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes {start}-{}/{size}", end - 1, size = expected.len())
        );
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
            expected[start..end]
        );
    }
    for range in [
        "bytes=999999999-",
        "bytes=2-1",
        "bytes=0-1,4-5",
        "bytes=-0",
        "invalid",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header("range", range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes */{}", file.size())
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&path)
                .header("if-match", "\"wrong\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&path)
                .header("range", "bytes=0-3")
                .header("if-range", "\"wrong\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        expected
    );

    // Acquire the response before a concurrent update, then consume its pinned body.
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    store
        .update_where("f", "name = 'changed'", "id < 100")
        .unwrap();
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        expected
    );
    let again = app
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        to_bytes(again.into_body(), usize::MAX).await.unwrap(),
        expected
    );
    let latest = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/files/f/latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(latest.headers()["cache-control"], "no-store");
    let latest: serde_json::Value =
        serde_json::from_slice(&to_bytes(latest.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_ne!(latest["snapshot_id"].as_i64().unwrap(), id);
    for (path, status) in [
        ("/files/missing/latest", StatusCode::NOT_FOUND),
        ("/files/f/snapshots/999999.parquet", StatusCode::NOT_FOUND),
        (
            "/files/f/snapshots/invalid.parquet",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            status
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_reads_http_snapshots_without_export() {
    let Some(python) = python() else {
        eprintln!("skipping DuckDB HTTP check: VPF_PYTHON not set");
        return;
    };
    let (dir, store, id) = fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let range_requests = Arc::new(AtomicUsize::new(0));
    let counts = range_requests.clone();
    let app = vpf_http::router(store.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let counts = counts.clone();
            async move {
                if request.headers().contains_key("range") {
                    counts.fetch_add(1, Ordering::Relaxed);
                }
                next.run(request).await
            }
        },
    ));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result = tokio::task::spawn_blocking(move || {
        let mut child = Command::new(python)
            .arg(format!(
                "{}/tests/check_duckdb_http.py",
                env!("CARGO_MANIFEST_DIR")
            ))
            .arg(url)
            .arg(id.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        if line.trim() == "mutate" {
            store
                .append("f", &sample_batch(1000, 200), AppendMode::Buffered)
                .unwrap();
            store
                .update_where("f", "name = 'changed'", "id >= 100 AND id < 150")
                .unwrap();
            store.delete_where("f", "id < 30").unwrap();
            writeln!(
                input,
                "{}",
                store.view("f", None).unwrap().snapshot.snapshot_id
            )
            .unwrap();
            input.flush().unwrap();
        }
        drop(input);
        let success = child.wait().unwrap().success();
        assert!(success, "DuckDB HTTP check failed");
        assert_eq!(
            line.trim(),
            "mutate",
            "DuckDB did not complete its initial HTTP query"
        );
        assert!(!dir.path().join("out.parquet").exists());
    })
    .await;
    server.abort();
    result.unwrap();
    assert!(
        range_requests.load(Ordering::Relaxed) > 0,
        "DuckDB must issue HTTP range requests"
    );
}
