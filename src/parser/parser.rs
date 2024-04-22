use polars::{lazy::dsl::col, prelude::*};

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
///
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