use axum::{
    body::Bytes, extract::{OriginalUri, Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::{any, get}, Router
};
use std::collections::HashMap;
use polars::prelude::*;

use std::path::PathBuf;

async fn catch_all(uri: OriginalUri, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    
    let path = uri.0.path().trim_start_matches("/");
    let base_path = PathBuf::from("/storage2/splus");

    println!("Path {:?}", path);
    let file_path = base_path.join(path);
    
    println!("Path {:?}", file_path);
    
    if let Some(query) = params.get("query"){
        println!("Query: {:?}", query);
    }
    if let Some(cols) = params.get("cols") {
        println!("Cols {:?}", cols);
    }

    println!("File Path {:?}", file_path);
    let bytes = process_and_return_parquet_file(&file_path.to_str().unwrap(), &params).await.unwrap();

    Bytes::from(bytes)
}

async fn process_and_return_parquet_file(file_path: &str, params: &HashMap<String, String>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Load the Parquet file lazily
    let args = ScanArgsParquet::default();
    let lf = LazyFrame::scan_parquet(file_path, args)?;

    // Load only the columns that are needed
    let cols = params.get("cols").unwrap().split(",").collect::<Vec<_>>();
    let mut df = lf.lazy().select(&cols)?;

    // apply filters from params query the queries are like "r_auto<0.02,DEC<9"
    if let Some(query) = params.get("query") {
        let query = query.split(",").collect::<Vec<_>>();
        for q in query {
            let q = q.split("<").collect::<Vec<_>>();
            let col = q[0];
            let val = q[1];
            df.filter(&format!("{} < {}", col, val))?;
        }
    }
    // Serialize DataFrame to Parquet bytes
    let mut buf = Vec::new();
    df.write_parquet(&mut buf)?;

    Ok(buf)
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