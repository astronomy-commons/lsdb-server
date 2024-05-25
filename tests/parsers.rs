#[cfg(test)]
mod parser {
    use lsdb_server::loaders::parsers::helpers;
    use polars::{lazy::dsl::col, prelude::*};

    #[test]
    fn test_parse_condition() {
        let expr = helpers::str_filter_to_expr("ra>=30.1241").unwrap();
        assert_eq!(expr, col("ra").gt_eq(lit(30.1241)));

        let expr = helpers::str_filter_to_expr("dec<=-30.3").unwrap();
        assert_eq!(expr, col("dec").lt_eq(lit(-30.3)));

        let expr = helpers::str_filter_to_expr("dec>4").unwrap();
        assert_eq!(expr, col("dec").gt(lit(4.0)));

        let expr = helpers::str_filter_to_expr("dec=4").unwrap();
        assert_eq!(expr, col("dec").eq(lit(4.0)));
    }
    

}