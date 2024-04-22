use axum::{
    body::Bytes, extract::{OriginalUri, Query}, response::IntoResponse
};

use std::collections::HashMap;
use std::path::PathBuf;

use crate::loaders;



/// Handles all incoming HTTP GET requests that do not match any other routes.
///
/// Captures the URI of the request and any query parameters, loads the parquet file. It processes the Parquet file
/// lazily based on the query parameters and returns the content as a byte stream.
///
/// # Arguments
/// * `uri` - The original URI of the request, used to determine the file to be accessed.
/// * `params` - A hash map containing query parameters which may affect how the Parquet file is processed.
///
/// # Returns
/// This function returns a byte stream that can be directly used as an HTTP response body.
pub async fn catch_all(uri: OriginalUri, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let path = uri.0.path().trim_start_matches("/");
    let base_path = PathBuf::from("/storage2/splus");

    let file_path = base_path.join(path);
    let bytes = loaders::parquet::process_and_return_parquet_file_lazy(&file_path.to_str().unwrap(), &params).await.unwrap();

    Bytes::from(bytes)
}

