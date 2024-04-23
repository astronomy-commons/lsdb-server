use polars::{lazy::dsl::col, prelude::*};
use std::collections::HashMap;

/// Parses a list of filter conditions from query parameter of hashmap.
/// 
/// # Arguments
/// 
/// * `params` - A reference to a HashMap of parameters.
/// 
/// # Returns
/// 
/// A Polars expression representing the combined filter conditions.
pub fn parse_querie_from_params(params: &HashMap<String, String>) -> Result<Expr, Box<dyn std::error::Error>> {
    let mut queries = Vec::new();
    if let Some(query) = params.get("query") {
        queries = query.split(",").collect::<Vec<_>>();
    }

    let conditions: Result<Vec<Expr>, _> = 
        queries.iter()
        .map(|condition: &&str| parse_condition(*condition))
        .collect();

    let combined_condition  = conditions?.into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .ok_or(""); // Handle case where no conditions are provided

    match combined_condition {
        Ok(_) => { Ok(combined_condition.unwrap()) },
        Err(_) => { Err( "Couldnt parse queries".into() ) },
    }
}

/// Parses a filter condition from a string into a Polars expression.
///
/// The expected format for `condition` is "{column_name} {operator} {value}", where:
/// - `column_name` identifies a DataFrame column.
/// - `operator` is one of `<`, `<=`, `>`, `>=`, or `=`.
/// - `value` is a number compared against the column's values.
///
/// # Parameters
/// * `condition` - A string slice representing the filter condition.
///
/// # Returns
/// A `Result` containing either:
/// - `Ok(Expr)`: A Polars expression if the parsing succeeds.
/// - `Err(Box<dyn Error>)`: An error if the format is incorrect or parsing fails.
pub fn parse_condition(condition: &str) -> Result<Expr, Box<dyn std::error::Error>> {
    use regex::Regex;

    // Regex to catch "{column_name} {operator} {value}"
    let re = Regex::new(r"([a-zA-Z_]+)([<>=]+)([-+]?[0-9]*\.?[0-9]*)").unwrap();

    let parts = re.captures(condition).unwrap();

    if parts.len() == 4 {
        let column = parts.get(1).unwrap().as_str();
        let operator = parts.get(2).unwrap().as_str();
        let value = parts.get(3).unwrap().as_str();

        match operator {
            "<" => Ok(col(column).lt(lit(value.parse::<f64>()?))),
            "<=" => Ok(col(column).lt_eq(lit(value.parse::<f64>()?))),
            ">" => Ok(col(column).gt(lit(value.parse::<f64>()?))),
            ">=" => Ok(col(column).gt_eq(lit(value.parse::<f64>()?))),
            "=" => Ok(col(column).eq(lit(value.parse::<f64>()?))),
            _ => Err("Unsupported operator".into()),
        }
    } else {
        Err("Invalid condition format".into())
    }
}