use axum::{
    body::Bytes, extract::{OriginalUri, Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::{any, get}, Router
};
use std::collections::HashMap;
use polars::{lazy::dsl::col, prelude::*};
use polars::io::HiveOptions;

use std::path::PathBuf;

use memory_stats::memory_stats;

fn usage() {
    if let Some(usage) = memory_stats() {
        println!("Current physical memory usage: {}", usage.physical_mem);
        println!("Current virtual memory usage: {}", usage.virtual_mem);
    } else {
        println!("Couldn't get the current memory usage :(");
    }
}

async fn catch_all(uri: OriginalUri, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let path = uri.0.path().trim_start_matches("/");
    let base_path = PathBuf::from("/storage2/splus");

    let file_path = base_path.join(path);
    let bytes = process_and_return_parquet_file(&file_path.to_str().unwrap(), &params).await.unwrap();

    Bytes::from(bytes)
}

async fn process_and_return_parquet_file(file_path: &str, params: &HashMap<String, String>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut args = ScanArgsParquet::default();
    
    args.hive_options = HiveOptions{enabled:false, schema: None};

    let lf = LazyFrame::scan_parquet(file_path, args).unwrap();

    let mut select_cols = Vec::new();
    if let Some(cols) = params.get("cols") {
        let cols = cols.split(",").collect::<Vec<_>>();
        select_cols = cols.iter().map(|x| col(x)).collect::<Vec<_>>();
    }

    let mut queries = Vec::new();
    if let Some(query) = params.get("query") {
        queries = query.split(",").collect::<Vec<_>>();
    }

    let conditions: Result<Vec<Expr>, _> = 
        queries.iter()
        .map(|condition: &&str| parse_condition(*condition))
        .collect();

    let combined_condition = conditions?.into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .ok_or(""); // Handle case where no conditions are provided

    let mut df;
    if combined_condition.is_ok() && select_cols.len() > 0{
        df = lf
            .select(select_cols)
            .filter(
                // only if combined_condition is not empty
                combined_condition?
            )
            .collect().unwrap();
    }
    else if combined_condition.is_ok() {
        df = lf
            //TODO: Remove later
            .drop(["_hipscat_index"])
            .filter(
                // only if combined_condition is not empty
                combined_condition?
            )
            .collect().unwrap();
    }
    else if select_cols.len() > 0 {
        df = lf
            .select(select_cols)
            .collect().unwrap();
    }
    else {
        df = lf.collect().unwrap();
    }

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;

    Ok(buf)
}

fn parse_condition(condition: &str) -> Result<Expr, Box<dyn std::error::Error>> {
    use regex::Regex;
    let re = Regex::new(r"([a-zA-Z_]+)([<>=]+)([0-9]+)").unwrap();

    let parts = re.captures(condition).unwrap();

    if parts.len() == 4 {
        let column = parts.get(1).unwrap().as_str();
        let operator = parts.get(2).unwrap().as_str();
        let value = parts.get(3).unwrap().as_str();

        match operator {
            "<" => Ok(col(column).lt(lit(value.parse::<f64>()?))),
            "<=" => Ok(col(column).lt_eq(lit(value.parse::<f64>()?))),
            ">" => Ok(col(column).gt(lit(value.parse::<f64>()?))),
            ">=" => Ok(col(column).gt_eq(lit(value.parse::<f64>()?))),
            "=" => Ok(col(column).eq(lit(value.parse::<f64>()?))),
            _ => Err("Unsupported operator".into()),
        }
    } else {
        Err("Invalid condition format".into())
    }
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