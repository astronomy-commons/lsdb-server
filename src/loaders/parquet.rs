
use std::collections::HashMap;

use polars::{lazy::dsl::col, prelude::*};
use polars::io::HiveOptions;

use crate::parser::parser;

pub async fn process_and_return_parquet_file_lazy(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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
        .map(|condition: &&str| parser::parse_condition(*condition))
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
            .collect()?;
    }
    else if combined_condition.is_ok() {
        df = lf
            //TODO: Remove later
            .drop(["_hipscat_index"])
            .filter(
                // only if combined_condition is not empty
                combined_condition?
            )
            .collect()?;
    }
    else if select_cols.len() > 0 {
        df = lf
            .select(select_cols)
            .collect()?;
    }
    else {
        df = lf.collect()?;
    }

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;

    Ok(buf)
}