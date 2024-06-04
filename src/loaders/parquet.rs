
use std::collections::HashMap;
use std::os::fd::FromRawFd;

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

    // Retrieve the schema of the LazyFrame
    let schema = lf.schema()?;
    let all_columns: Vec<(String, DataType)> = schema
        .iter_fields()
        .map(|field| (field.name().to_string(), field.data_type().clone()))
        .collect();

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
            .filter(
                // only if combined_condition is not empty
                filters?
            )
            .select(selected_cols)
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

    for (col, dtype) in &all_columns {
        if !df.get_column_names().contains(&col.as_str()) {
            let series = Series::full_null(col, df.height(), &dtype);
            df.with_column(series)?;
        }
    }
    df = df.select(&all_columns.iter().map(|(col, _)| col.as_str()).collect::<Vec<_>>())?;

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf)
        .finish(&mut df)?;
    Ok(buf)
}

use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_writer::ArrowWriter;
use futures_util::stream::StreamExt;

pub async fn process_and_return_parquet_file(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {

    // Open async file containing parquet data
    let std_file = std::fs::File::open(file_path)?;
    let mut file = tokio::fs::File::from_std(std_file);
    
    // Construct the reader
    let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        meta.clone(),

    ).with_row_filter(filter).with_batch_size(8192).build()?;
    
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, stream.schema().clone(), None)?;
    
    // Collect all batches and write them to the buffer
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        writer.write(&batch)?;
    }
    
    writer.finish()?;
    let _ = writer.close();
    Ok(buf)
}