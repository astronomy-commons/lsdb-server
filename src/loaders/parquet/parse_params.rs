use std::collections::HashMap;
use regex::Regex;

/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters containing 'columns' key.
/// 
/// # Returns
/// 
/// A vector of Polars with the selected columns.
pub fn parse_columns_from_params_to_str( params: &HashMap<String, String> ) -> Option<Vec<String>> {
    // Parse columns from params
    if let Some(cols) = params.get("columns") {
        let cols = cols.split(",").collect::<Vec<_>>();
        let select_cols = cols.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        return Some(select_cols);
    }
    None
}

/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters containing 'filters' key.
/// 
/// # Returns
/// 
/// A vector of tuples containing the column name, the comparison operator and the value to compare.
pub fn parse_filters(params: &HashMap<String, String>) -> Option<Vec<(&str, &str, &str)>> {
    let mut filters = Vec::new();
    if let Some(query) = params.get("filters") {
        filters = query.split(",").collect::<Vec<_>>();
    }

    if filters.len() == 0 {
        return None
    }

    let re = Regex::new(r"([a-zA-Z_]+)([<>=]+)([-+]?[0-9]*\.?[0-9]*)").unwrap();
    let mut filter_vec = Vec::new();
    for filter in filters {
        let f_vec = re.captures(filter).unwrap();
        filter_vec.push((f_vec.get(1).unwrap().as_str(), f_vec.get(2).unwrap().as_str(), f_vec.get(3).unwrap().as_str()));
    }

    Some(filter_vec)
}