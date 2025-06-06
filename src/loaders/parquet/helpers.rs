use arrow::array::{Float64Array, Float32Array, Int16Array, Int32Array, Int64Array, Int8Array, BooleanArray, Datum};
use arrow::record_batch::RecordBatch;
use arrow::array::BooleanBuilder;
use arrow::datatypes::Schema;
use std::sync::Arc;

/// Create a boolean mask based on the filters provided.
/// 
/// # Arguments
/// 
/// * `batch` - A reference to a RecordBatch that will be filtered.
/// * `original_schema` - A reference to the original schema of the RecordBatch.
/// * `filters` - A vector of tuples containing the column name, the comparison operator and the value to compare.
/// 
/// # Returns
/// 
/// This function returns an Arrow Result with the boolean mask.
pub fn create_boolean_mask(batch: &RecordBatch, original_schema: &Arc<Schema>, filters: Vec<Vec<(&str, &str, &str)>>) -> arrow::error::Result<Arc<BooleanArray>> {
    let num_rows = batch.num_rows();
    let mut boolean_builder = BooleanBuilder::new();
    boolean_builder.append_n(num_rows, false);
    let mut boolean_mask = boolean_builder.finish();
    for conjunction in filters.iter() {
        let mut conj_boolean_builder = BooleanBuilder::new();
        conj_boolean_builder.append_n(num_rows, true);
        let mut conj_boolean_mask = conj_boolean_builder.finish();
        for filter in conjunction.iter() {
            let column = batch.column(original_schema.index_of(filter.0).unwrap());

            if column.data_type() == &arrow::datatypes::DataType::Float32 {
                let column = column.as_any().downcast_ref::<Float32Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Float64 {
                let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Int16 {
                let column = column.as_any().downcast_ref::<Int16Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Int32 {
                let column = column.as_any().downcast_ref::<Int32Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Int64 {
                let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Int8 {
                let column = column.as_any().downcast_ref::<Int8Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else if column.data_type() == &arrow::datatypes::DataType::Boolean {
                let column = column.as_any().downcast_ref::<Int16Array>().unwrap();
                apply_filter(&mut conj_boolean_mask, column, filter)?;
            } else {
                return Err(arrow::error::ArrowError::NotYetImplemented(format!("Data type {:?} not yet implemented", column.data_type())));
            }
        }
        let mut new_mask = BooleanBuilder::new();
        for (index, val) in conj_boolean_mask.iter().enumerate(){
            new_mask.append_value(boolean_mask.value(index) || val.unwrap());
        }
        boolean_mask = new_mask.finish();
    }
    Ok(Arc::new(boolean_mask))
}

/// Apply a filter to a column and update the boolean mask.
/// 
/// # Arguments
/// 
/// * `boolean_mask` - A mutable reference to a BooleanArray that will be updated with the filter results.
/// * `column` - A reference to a PrimitiveArray that will be filtered.
/// * `filter` - A tuple containing the column name, the comparison operator and the value to compare.
/// 
/// # Returns
/// 
/// This function returns an Arrow Result.
fn apply_filter<T>(boolean_mask: &mut BooleanArray, column: &arrow::array::PrimitiveArray<T>, filter: &(&str, &str, &str)) -> arrow::error::Result<()>
where 
    T: arrow::datatypes::ArrowPrimitiveType,
    T::Native: std::cmp::PartialOrd + std::str::FromStr,
    <T::Native as std::str::FromStr>::Err: std::fmt::Debug,
{
    let filter_value = filter.2.parse::<T::Native>().unwrap();
    let mut new_mask = BooleanBuilder::new();

    for (index, value) in column.iter().enumerate() {
        let current_mask = boolean_mask.value(index);
        let result = match filter.1 {
            ">" => value.map_or(false, |v| v > filter_value),
            "<" => value.map_or(false, |v| v < filter_value),
            "=" => value.map_or(false, |v| v == filter_value),
            "!=" => value.map_or(false, |v| v != filter_value),
            ">=" => value.map_or(false, |v| v >= filter_value),
            "<=" => value.map_or(false, |v| v <= filter_value),
            "==" => value.map_or(false, |v| v == filter_value),
            _ => false,
        };
        new_mask.append_value(current_mask && result);
    }

    *boolean_mask = new_mask.finish();
    Ok(())
}