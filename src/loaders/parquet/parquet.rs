
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, NullArray};
use arrow::array::new_null_array;
use arrow::record_batch::RecordBatch;

use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use futures_util::stream::StreamExt;
use tokio::fs::File;

use crate::loaders::parquet::parse_params;
use crate::loaders::parquet::helpers::create_boolean_mask;

/// Process a Parquet file and return the content as a byte stream.
/// 
/// # Arguments
/// 
/// * `file_path` - A reference to a string containing the path to the Parquet file.
/// * `params` - A reference to a HashMap of parameters containing 'columns' and 'filters' keys.
/// 
/// # Returns
/// 
/// This function returns a byte stream that can be directly used as an HTTP response body.
pub async fn process_and_return_parquet_file(
    file_path: &str, 
    params: &HashMap<String, String>
) -> Result<Vec<u8>, Box<dyn Error>> {
    // Open async file containing parquet data
    let std_file = std::fs::File::open(file_path)?;
    let mut file = File::from_std(std_file);

    let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let stream_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        meta.clone()
    );
    let original_metadata = meta.metadata();
    let metadata_keys = original_metadata
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .clone();

    let original_schema = stream_builder
        .schema()
        .clone();

    let all_columns = original_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();

    // Parse selected columns from params
    let columns = parse_params::parse_columns_from_params_to_str(&params)
        .unwrap_or(all_columns);

    let filters = parse_params::parse_filters(&params);

    // Construct the reader stream
    let mut stream = stream_builder
        .with_batch_size(8192)
        .build()?;
    
    // Set writer properties with the original metadata
    let writer_properties = WriterProperties::builder()
        .set_key_value_metadata(Some(metadata_keys))
        .build();

    let mut out_buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut out_buffer, 
        original_schema.clone(), 
        Some(writer_properties)
    )?;
    
    // Collect all batches and write them to the buffer
    while let Some(batch) = stream.next().await {
        let mut batch = batch?;

        //let predicate = arrow::compute::FilterBuilder::new(&batch, &projection)?;
        if filters.is_some() {
            let filter_mask = &create_boolean_mask(
                &batch, 
                &original_schema, 
                filters.clone().unwrap()
            ).unwrap();
            batch = arrow::compute::filter_record_batch(
                &batch, 
                &filter_mask
            )?;
        }
        
        let selected_arrays = original_schema.fields().iter()
            .map(|field| {
                if let Ok(index) = batch.schema().index_of(field.name()) {
                    if columns.contains(&field.name().to_string()) || &field.name().to_string() == "_hipscat_index" {
                        batch.column(index).clone()
                    } else {
                        new_null_array(
                            field.data_type(), 
                            batch.num_rows()
                        )
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
    Ok(out_buffer)
}
