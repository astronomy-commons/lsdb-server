
use std::collections::HashMap;

use polars::prelude::*;
use polars::io::HiveOptions;
use crate::loaders::parsers::parse_params;


pub async fn process_and_return_parquet_file_lazy(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut args = ScanArgsParquet::default();

    // TODO: fix the parquet reader hive_options with _hipscat_index
    args.hive_options = HiveOptions{enabled:false, schema: None};

    let lf = LazyFrame::scan_parquet(file_path, args).unwrap();
    let mut selected_cols = parse_params::parse_columns_from_params(&params).unwrap_or(Vec::new());
    selected_cols = parse_params::parse_exclude_columns_from_params(&params, &lf).unwrap_or(selected_cols);

    //println!("{:?}", &params.get("filters").unwrap());
    let filters = parse_params::parse_filters_from_params(&params);

    // HACK: Find a better way to handle each combination of selected params
    let mut df;
    //In case we have selected columns and filters
    if filters.is_ok() && selected_cols.len() > 0{
        df = lf
            .drop(["_hipscat_index"])
            .select(selected_cols)
            .filter(
                // only if combined_condition is not empty
                filters?
            )
            .collect()?;
    }
    // In case we have only filters
    else if filters.is_ok() {
        df = lf
            //TODO: fix the parquet reader hive_options with _hipscat_index
            .drop(["_hipscat_index"])
            .filter(
                // only if combined_condition is not empty
                filters?
            )
            .collect()?;
    }
    // In case we have only selected columns
    else if selected_cols.len() > 0 {
        df = lf
            .select(selected_cols)
            .collect()?;
    }
    // In case we have no selected columns or filters, return whole dataframe
    else {
        df = lf.drop(["_hipscat_index"]).collect()?;
    }

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf)
        .finish(&mut df)?;
    Ok(buf)
}