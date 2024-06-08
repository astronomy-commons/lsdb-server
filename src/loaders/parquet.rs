
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

    let col_names = df.get_column_names_owned();
    for (index, name) in col_names.iter().enumerate() {
        let col = df.column(&name).unwrap();
        if col.dtype() == &ArrowDataType::LargeUtf8 {
            //modifying the column to categorical
            // modify the schema to be categorica
            // df.try_apply(name, |s| s.categorical().cloned())?;
        }
    }
    //println!("{:?}", df.schema());

    // Checking if anything changed
    // for (index, name) in col_names.iter().enumerate() {
    //     let col = df.column(&name).unwrap();
    //     if col.dtype() == &ArrowDataType::LargeUtf8 {
    //         println!("Column in LargeUtf8 {:?}", name);
    //     }
    // }

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf)
        .finish(&mut df)?;
    Ok(buf)
}


use arrow::array::{ArrayRef, Float64Array, NullArray};
use arrow::array::{BooleanArray, Float32Array, new_null_array};
use arrow::array::{make_array, ArrayDataBuilder};
use arrow::record_batch::RecordBatch;
use futures_util::stream::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use parquet::file::properties::WriterProperties;

fn create_boolean_mask(batch: &RecordBatch, original_schema: &Arc<arrow::datatypes::Schema>) -> arrow::error::Result<Arc<BooleanArray>> {
    // Extract the "PROB_GAL" column and downcast it to Float32Array
    let prob_gal = batch.column(original_schema.index_of("PROB_GAL")?);
    // Downcast to original schema type 
    let prob_gal = prob_gal.as_any().downcast_ref::<Float64Array>().unwrap();
    
    // Create a boolean mask where true is prob_gal > 0.8
    let mut builder = BooleanArray::builder(prob_gal.len());
    for value in prob_gal.iter() {
        builder.append_value(value.map_or(false, |v| v > 0.8));
    }
    
    let filter_mask = builder.finish();
    Ok(Arc::new(filter_mask))
}


pub async fn process_and_return_parquet_file(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn Error>> {
    // Open async file containing parquet data
    let std_file = std::fs::File::open(file_path)?;
    let mut file = File::from_std(std_file);
    
    // Parse selected columns from params
    let selected_cols = parse_params::parse_columns_from_params_to_str(&params).unwrap_or(Vec::new());

    let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;

    let stream_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        meta.clone()
    );
    let original_metadata = meta.metadata();
    let metadata_keys = original_metadata.file_metadata().key_value_metadata().unwrap().clone();
    let original_schema = stream_builder.schema().clone();

    // Construct the reader
    let mut stream = stream_builder
        .with_batch_size(8192)
        .build()?;
    
    let mut buf = Vec::new();

    // Set writer properties with the original metadata
    let writer_properties = WriterProperties::builder()
        .set_key_value_metadata(Some(metadata_keys))
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, original_schema.clone(), Some(writer_properties))?;
    
    // Collect all batches and write them to the buffer
    while let Some(batch) = stream.next().await {
        let mut batch = batch?;

        //let predicate = arrow::compute::FilterBuilder::new(&batch, &projection)?;
        batch = arrow::compute::filter_record_batch(&batch, &create_boolean_mask(&batch, &original_schema).unwrap())?;
        
        let selected_arrays = original_schema.fields().iter()
            .map(|field| {
                if let Ok(index) = batch.schema().index_of(field.name()) {
                    if selected_cols.contains(&field.name().to_string()) || &field.name().to_string() == "_hipscat_index" {
                        batch.column(index).clone()
                    } else {
                        new_null_array(field.data_type(), batch.num_rows())
                    }
                } else {
                    Arc::new(NullArray::new(batch.num_rows())) as ArrayRef
                }
            })
            .collect::<Vec<_>>();
        
        let selected_batch = RecordBatch::try_new(original_schema.clone(), selected_arrays)?;
        writer.write(&selected_batch)?;
    }

    writer.finish()?;
    let _ = writer.close();
    Ok(buf)
}
