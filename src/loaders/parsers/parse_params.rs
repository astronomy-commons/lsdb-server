use polars::{lazy::dsl::col, prelude::*};
use std::collections::HashMap;
use crate::loaders::parsers::helpers;

/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters containing 'columns' key.
/// 
/// # Returns
/// 
/// A vector of Polars with the selected columns.
pub fn parse_columns_from_params( params: &HashMap<String, String> ) -> Option<Vec<Expr>> {
    // Parse columns from params
    if let Some(cols) = params.get("columns") {
        let cols = cols.split(",").collect::<Vec<_>>();
        let select_cols = cols.iter().map(|x| col(x)).collect::<Vec<_>>();
        return Some(select_cols);
    }
    None
}

/// Parses a list of filter conditions from query parameter of hashmap.
/// 
/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters.
/// 
/// # Returns
/// 
/// A Polars expression representing the combined filter conditions.
pub fn parse_filters_from_params(params: &HashMap<String, String>) -> Result<Expr, Box<dyn std::error::Error>> {
    let mut filters = Vec::new();
    if let Some(query) = params.get("filters") {
        filters = query.split(",").collect::<Vec<_>>();
    }

    //TODO: DEPRECATE
    let conditions: Result<Vec<Expr>, _> = filters.iter()
        .map(|condition: &&str| helpers::str_filter_to_expr(*condition))
        .collect();

    let combined_condition  = conditions?.into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .ok_or(""); // Handle case where no conditions are provided

    match combined_condition {
        Ok(_) => { Ok(combined_condition.unwrap()) },
        Err(_) => { Err( "Couldnt parse queries".into() ) },
    }
}


/// # Arguments
/// 
/// * `params` - The client request HashMap of parameters.
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
        
        let cols = helpers::get_lazyframe_column_names(&lf);

        let select_cols = cols.iter()
            .filter(|&x| !exclude_cols.contains( &x.as_str() ))
            .map(|x| col(x))
            .collect::<Vec<_>>();

        return Some(select_cols);
    }
    None
}
