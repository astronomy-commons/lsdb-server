#[cfg(test)]
mod parser {
    use lsdb_server::parser::parser::*;
    use polars::{lazy::dsl::col, prelude::*};

    #[test]
    fn test_parse_condition() {
        let expr = parse_condition("ra>=30.1241").unwrap();
        assert_eq!(expr, col("ra").gt_eq(lit(30.1241)));

        let expr = parse_condition("dec<=-30.3").unwrap();
        assert_eq!(expr, col("dec").lt_eq(lit(-30.3)));

        let expr = parse_condition("dec>4").unwrap();
        assert_eq!(expr, col("dec").gt(lit(4)));
    }
}