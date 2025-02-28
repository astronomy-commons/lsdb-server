use axum::{
    body::Bytes,
    extract::{OriginalUri, Query},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::loaders;

/// Handles HTTP GET requests with Range support for Parquet files.
pub async fn entry_route(
    uri: OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let path = uri.0.path().trim_start_matches("/");
    let base_path = PathBuf::from("/storage2/splus");
    let file_path = base_path.join(path);

    // println!("Processing file: {:?}", file_path);

    // Check for Range header
    if let Some(range_header) = headers.get("Range") {
        // println!("Range header: {:?}", range_header);
        if let Ok(range_str) = range_header.to_str() {
            let full_bytes = match loaders::parquet::parquet::process_and_return_parquet_file(&file_path.to_str().unwrap(), &params).await {
                Ok(data) => data,
                Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load file").into_response(),
            };

            let file_size = full_bytes.len() as u64;

            if let Some((start, end)) = parse_range(range_str, file_size) {
                let end = end.unwrap_or(file_size - 1);
                if start >= file_size || end >= file_size {
                    return (
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        [("Content-Range", format!("bytes */{}", file_size))],
                    )
                        .into_response();
                }

                let chunk = &full_bytes[start as usize..=end as usize];
                let content_range = format!("bytes {}-{}/{}", start, end, file_size);

                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        ("Content-Range", content_range),
                        ("Accept-Ranges", "bytes".to_string()),
                        ("Content-Length", chunk.len().to_string()),
                    ],
                    Bytes::from(chunk.to_vec()),
                )
                    .into_response();
            }
        }
    }

    // No Range header: Process and return the full Parquet file as before
    match loaders::parquet::parquet::process_and_return_parquet_file(&file_path.to_str().unwrap(), &params).await {
        Ok(bytes) => Bytes::from(bytes).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load file").into_response(),
    }
}

/// Parses the `Range` header value and returns `(start, end)` positions.
fn parse_range(range: &str, file_size: u64) -> Option<(u64, Option<u64>)> {
    if !range.starts_with("bytes=") {
        return None;
    }

    let range = &range[6..]; // Remove "bytes="
    let parts: Vec<&str> = range.split('-').collect();

    if parts.len() != 2 {
        return None;
    }

    let start = parts[0].parse::<u64>().ok();
    let end = parts[1].parse::<u64>().ok();

    match (start, end) {
        (Some(s), Some(e)) if s <= e => Some((s, Some(e))),  // Valid range
        (Some(s), None) => Some((s, None)),                  // Open-ended range
        _ => None,                                           // Invalid range
    }
}