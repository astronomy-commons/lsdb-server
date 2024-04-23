use polars::prelude::*;
use std::collections::HashMap;

/// Returns the column names of a LazyFrame.
/// 
/// # Arguments
/// 
/// * `lf` - A reference to a LazyFrame.
/// 
/// # Returns
/// 
/// A vector of strings representing the column names of the DataFrame.
pub fn get_lazyframe_column_names(lf : &LazyFrame) -> Vec<String> {
    let df = lf.clone().first().collect().unwrap();
    df.get_column_names().iter().map(|x| x.to_string()).collect()
}

/// Parses a list of columns from a HashMap of parameters.
/// 
/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters.
/// 
/// # Returns
/// 
/// A vector of Polars expressions representing the columns to select.
pub fn parse_columns_from_params( params: &HashMap<String, String> ) -> Option<Vec<Expr>> {
    // Parse columns from params
    if let Some(cols) = params.get("cols") {
        let cols = cols.split(",").collect::<Vec<_>>();
        let select_cols = cols.iter().map(|x| col(x)).collect::<Vec<_>>();
        return Some(select_cols);
    }
    None
}

/// Parses a list of columns to exclude from a HashMap of parameters.
///
/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters.
/// * `lf` - A reference to a LazyFrame.
/// 
/// # Returns
/// 
/// A vector of Polars expressions representing the columns to exclude.
pub fn parse_exclude_columns_from_params( params: &HashMap<String, String>, lf : &LazyFrame ) -> Option<Vec<Expr>> {
    // Parse columns from params
    if let Some(exclude_cols) = params.get("exclude_cols") {
        let exclude_cols = exclude_cols.split(",").collect::<Vec<_>>();
        let exclude_cols = exclude_cols.iter().map(|&x| x).collect::<Vec<_>>();
        
        let cols = get_lazyframe_column_names(&lf);

        let select_cols = cols.iter()
            .filter(|&x| !exclude_cols.contains( &x.as_str() ))
            .map(|x| col(x))
            .collect::<Vec<_>>();

        return Some(select_cols);
    }
    None
}



