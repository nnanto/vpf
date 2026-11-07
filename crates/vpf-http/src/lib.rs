use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream;
use vpf::catalog::Catalog;
use vpf::layout::VirtualFile;

/// Serves every `<name>.vpf` file of a catalog directory as `/files/<name>/...`.
pub fn router(catalog: Arc<Catalog>) -> Router {
    Router::new()
        .route("/files/{name}/latest", get(latest))
        .route("/files/{name}/snapshots/{snapshot}", get(snapshot))
        .with_state(catalog)
}

fn failure(error: vpf::Error) -> Response {
    let status = match error {
        vpf::Error::NotFound(_) => StatusCode::NOT_FOUND,
        vpf::Error::Invalid(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    eprintln!("VPF HTTP: {error}");
    (
        status,
        status.canonical_reason().unwrap_or("request failed"),
    )
        .into_response()
}

async fn latest(State(catalog): State<Arc<Catalog>>, Path(name): Path<String>) -> Response {
    let result = tokio::task::spawn_blocking(move || {
        let (store, table) = catalog.table(&name)?;
        store.view(&table, None)
    })
    .await;
    match result {
        Ok(Ok(view)) => {
            let mut response = Json(serde_json::json!({
                "snapshot_id": view.snapshot.snapshot_id
            }))
            .into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
            response
        }
        Ok(Err(error)) => failure(error),
        Err(error) => {
            eprintln!("VPF HTTP worker: {error}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// A single HTTP range, represented by an exclusive end.
fn byte_range(value: &str, size: u64) -> Option<(u64, u64)> {
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    if size == 0 {
        return None;
    }
    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?;
        return (suffix > 0).then_some((size.saturating_sub(suffix), size));
    }
    let start = start.parse::<u64>().ok()?;
    if start >= size {
        return None;
    }
    let end = if end.is_empty() {
        size
    } else {
        let inclusive = end.parse::<u64>().ok()?;
        if inclusive < start {
            return None;
        }
        inclusive.saturating_add(1).min(size)
    };
    Some((start, end))
}

fn body(file: VirtualFile, start: u64, end: u64) -> Body {
    Body::from_stream(stream::try_unfold(
        (file, start),
        move |(file, offset)| async move {
            if offset >= end {
                return Ok::<_, std::io::Error>(None);
            }
            let length = (end - offset).min(64 * 1024) as usize;
            let (file, bytes) = tokio::task::spawn_blocking(move || {
                let bytes = file.read_range(offset, length);
                (file, bytes)
            })
            .await
            .map_err(std::io::Error::other)?;
            let bytes = bytes.map_err(|error| {
                eprintln!("VPF HTTP range read: {error}");
                std::io::Error::other(error)
            })?;
            Ok(Some((bytes, (file, offset + length as u64))))
        },
    ))
}

async fn snapshot(
    State(catalog): State<Arc<Catalog>>,
    Path((name, snapshot)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(id) = snapshot
        .strip_suffix(".parquet")
        .and_then(|id| id.parse::<i64>().ok())
        .filter(|id| *id > 0)
    else {
        return (
            StatusCode::BAD_REQUEST,
            "expected a positive snapshot ID ending in .parquet",
        )
            .into_response();
    };
    let file = match tokio::task::spawn_blocking(move || {
        let (store, table) = catalog.table(&name)?;
        store.open_snapshot(&table, Some(id))
    })
    .await
    {
        Ok(Ok(file)) => file,
        Ok(Err(error)) => return failure(error),
        Err(error) => {
            eprintln!("VPF HTTP worker: {error}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let size = file.size();
    let etag = format!("\"snapshot-{id}\"");
    if let Some(value) = headers.get(header::IF_MATCH) {
        let matches = value
            .to_str()
            .is_ok_and(|value| value == "*" || value.split(',').any(|item| item.trim() == etag));
        if !matches {
            return StatusCode::PRECONDITION_FAILED.into_response();
        }
    }
    let use_range = method != Method::HEAD
        && headers
            .get(header::IF_RANGE)
            .is_none_or(|value| value.to_str().ok() == Some(&etag));
    let range = headers.get(header::RANGE).filter(|_| use_range);
    let (start, end) = if let Some(range) = range {
        let Some(bounds) = range
            .to_str()
            .ok()
            .and_then(|value| byte_range(value, size))
        else {
            let mut response = (
                StatusCode::RANGE_NOT_SATISFIABLE,
                "invalid or unsatisfiable byte range",
            )
                .into_response();
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes */{size}").parse().unwrap(),
            );
            return response;
        };
        bounds
    } else {
        (0, size)
    };
    let mut response = if method == Method::HEAD {
        Body::empty().into_response()
    } else {
        body(file, start, end).into_response()
    };
    if range.is_some() {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{size}", end - 1).parse().unwrap(),
        );
    }
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        "application/vnd.apache.parquet".parse().unwrap(),
    );
    headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    headers.insert(
        header::CONTENT_LENGTH,
        (end - start).to_string().parse().unwrap(),
    );
    headers.insert(header::ETAG, etag.parse().unwrap());
    headers.insert(
        header::CACHE_CONTROL,
        "private, max-age=31536000, immutable".parse().unwrap(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::byte_range;

    #[test]
    fn ranges() {
        for (value, expected) in [
            ("bytes=0-0", Some((0, 1))),
            ("bytes=3-", Some((3, 10))),
            ("bytes=-4", Some((6, 10))),
            ("bytes=-20", Some((0, 10))),
            ("bytes=3-99", Some((3, 10))),
            ("bytes=0-18446744073709551615", Some((0, 10))),
            ("bytes=10-", None),
            ("bytes=5-3", None),
            ("bytes=-0", None),
            ("bytes=0-1,3-4", None),
            ("garbage", None),
        ] {
            assert_eq!(byte_range(value, 10), expected, "{value}");
        }
        assert_eq!(byte_range("bytes=0-", 0), None);
    }
}
