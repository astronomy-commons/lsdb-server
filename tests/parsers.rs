#[cfg(test)]
mod parser {
    use lsdb_server::loaders::parquet;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_parse_filters() {
        let mut params = HashMap::new();

        params.insert("filters".to_string(), "RA>=30.1241,DEC<=-30.3,RA>30,DEC<=30;RA==1;RA=1,RA!=0".to_string());

        let filters = parquet::parse_params::parse_filters(&params);
        println!("{:#?}", filters);
        // TODO: Add assertions here to verify the result
    }
}