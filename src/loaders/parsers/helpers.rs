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
pub fn str_filter_to_expr(condition: &str) -> Result<Expr, Box<dyn std::error::Error>> {
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


/// Parses filter conditions from a list of tuples into Polars expressions.
///
/// The expected format for each tuple in `filters` is (column_name, operator, value), where:
/// - `column_name` identifies a DataFrame column.
/// - `operator` is one of "==", "=", ">", ">=", "<", "<=", "!=", "in", "not in".
/// - `value` is a number or a list of values compared against the column's values.
///
/// # Parameters
/// * `filters` - An optional vector of tuples representing the filter conditions.
///
/// # Returns
/// A `Result` containing either:
/// - `Ok(Vec<Expr>)`: A vector of Polars expressions if parsing succeeds.
/// - `Err(Box<dyn Error>)`: An error if the format is incorrect or parsing fails.
pub fn filters_to_expr(filters: Option<Vec<(String, String, Vec<f64>)>>) -> Result<Vec<Expr>, Box<dyn std::error::Error>> {
    let mut expressions = Vec::new();

    if let Some(conditions) = filters {
        for (column, operator, values) in conditions {
            let expression = match operator.as_str() {
                "=" | "==" => col(&column).eq(lit(values[0])),
                "!=" => col(&column).neq(lit(values[0])),
                ">" => col(&column).gt(lit(values[0])),
                ">=" => col(&column).gt_eq(lit(values[0])),
                "<" => col(&column).lt(lit(values[0])),
                "<=" => col(&column).lt_eq(lit(values[0])),
                _ => return Err("Unsupported operator".into()),
            };
            expressions.push(expression);
        }
    }

    Ok(expressions)
}

