
use std::collections::HashMap;

use polars::{lazy::dsl::col, prelude::*};
use polars::io::HiveOptions;

use crate::parsers::helpers;
use crate::parsers::parse_filters;

pub async fn process_and_return_parquet_file_lazy(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut args = ScanArgsParquet::default();

    args.hive_options = HiveOptions{enabled:false, schema: None};

    let lf = LazyFrame::scan_parquet(file_path, args).unwrap();

    let mut selected_cols = helpers::parse_columns_from_params(&params).unwrap_or(Vec::new());
    selected_cols = helpers::parse_exclude_columns_from_params(&params, &lf).unwrap_or(selected_cols);

    let combined_condition = parse_filters::parse_querie_from_params(&params);
    

    let mut df;
    //In case we have selected columns and a combined condition
    if combined_condition.is_ok() && selected_cols.len() > 0{
        df = lf
            .select(selected_cols)
            .filter(
                // only if combined_condition is not empty
                combined_condition?
            )
            .collect()?;
    }
    // In case we have only a combined condition
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
    // In case we have only selected columns
    else if selected_cols.len() > 0 {
        df = lf
            .select(selected_cols)
            .collect()?;
    }
    // In case we have no selected columns or combined condition
    else {
        df = lf.collect()?;
    }

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;

    Ok(buf)
}