use axum::{
    body::Bytes, extract::{OriginalUri, Query}, response::IntoResponse, routing::{any, get}, Router
};

use std::collections::HashMap;
use std::path::PathBuf;

mod aux;
mod parquet;

async fn catch_all(uri: OriginalUri, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let path = uri.0.path().trim_start_matches("/");
    let base_path = PathBuf::from("/storage2/splus");

    let file_path = base_path.join(path);
    let bytes = parquet::process_and_return_parquet_file_lazy(&file_path.to_str().unwrap(), &params).await.unwrap();

    Bytes::from(bytes)
}


#[tokio::main]
async fn main() {
    println!("Starting Server");

    let app = Router::new()
        .route("/", get(|| async { "online" }))
        .fallback(any(catch_all));  // This will catch all other paths

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}