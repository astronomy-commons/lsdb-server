use std::collections::HashMap;
use regex::Regex;

/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters containing 'columns' key.
/// 
/// # Returns
/// 
/// A vector of Polars with the selected columns.
pub fn parse_columns_from_params_to_str(params: &HashMap<String, String>) -> Option<Vec<String>> {
    // Parse columns from params
    println!("Params: {:?}", params);
    
    // Initialize a set of columns to return
    let mut select_cols = if let Some(cols) = params.get("columns") {
        cols.split(",").map(|x| x.to_string()).collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // If filters exist, extract and add filter columns if not already present
    if let Some(query) = params.get("filters") {
        let re = Regex::new(r"([0-9a-zA-Z_]+)([!<>=]+)([-+]?[0-9]*\.?[0-9]*)").unwrap();
        
        for filter in query.split(",") {
            if let Some(captures) = re.captures(filter) {
                let filter_col = captures.get(1).unwrap().as_str();
                
                // Add filter column only if it's not already in select_cols
                if !select_cols.contains(&filter_col.to_string()) {
                    select_cols.push(filter_col.to_string());
                }
            }
        }
    }

    // Return Some(select_cols) if not empty, otherwise None
    if !select_cols.is_empty() {
        Some(select_cols)
    } else {
        None
    }
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

    let re = Regex::new(r"([0-9a-zA-Z_]+)([!<>=]+)([-+]?[0-9]*\.?[0-9]*)").unwrap();
    let mut filter_vec = Vec::new();
    for filter in filters {
        let f_vec = re.captures(filter).unwrap();
        filter_vec.push((f_vec.get(1).unwrap().as_str(), f_vec.get(2).unwrap().as_str(), f_vec.get(3).unwrap().as_str()));
    }

    Some(filter_vec)
}