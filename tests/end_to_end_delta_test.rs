/// End-to-end test for the full Delta table workflow
///
/// This test verifies:
/// 1. Delta tables can be loaded from disk
/// 2. GraphQL schema can be built from Delta tables
/// 3. Queries work against Delta table data

mod delta_tests {
    use nouninator::config::EntityConfig;
    use nouninator::schema::SchemaBuilder;
    use std::path::Path;

    #[tokio::test]
    async fn test_delta_tables_exist() {
        // Verify Delta tables were created
        let delta_dir = "examples/delta";
        
        let tables = vec!["nouns", "verbs", "adjectives", "sentences", "synonyms", "word_frequency"];
        
        for table in tables {
            let table_path = format!("{}/{}", delta_dir, table);
            let path = Path::new(&table_path);
            
            assert!(path.exists(), "Delta table {} should exist", table);
            
            // Check for _delta_log directory
            let log_path = format!("{}/_delta_log", table_path);
            let log_path = Path::new(&log_path);
            assert!(log_path.exists(), "Delta log for {} should exist", table);
        }
    }

    #[tokio::test]
    async fn test_load_delta_table_into_schema() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register a Delta table
        let delta_path = "examples/delta/nouns";
        
        // Skip test if Delta table doesn't exist
        if !Path::new(delta_path).exists() {
            eprintln!("Skipping test: Delta table not found at {}", delta_path);
            eprintln!("Run: cargo run -- init --example");
            return;
        }

        builder
            .register_table_from_path("nouns", delta_path)
            .await
            .expect("Failed to register Delta table");

        // Create entity config
        let entity = EntityConfig {
            table: "nouns".to_string(),
            graphql_name: "Noun".to_string(),
            primary_key: "noun_id".to_string(),
            description: Some("Nouns from Delta table".to_string()),
            storage_location: None,
        };

        // Build schema
        let _schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema from Delta table");

        // Verify schema was built successfully
        assert!(true, "Schema built successfully from Delta table");
    }

    #[tokio::test]
    async fn test_query_delta_table() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let delta_path = "examples/delta/word_frequency";
        
        // Skip test if Delta table doesn't exist
        if !Path::new(delta_path).exists() {
            eprintln!("Skipping test: Delta table not found");
            return;
        }

        let mut builder = SchemaBuilder::new();

        builder
            .register_table_from_path("word_frequency", delta_path)
            .await
            .expect("Failed to register Delta table");

        let entity = EntityConfig {
            table: "word_frequency".to_string(),
            graphql_name: "WordFrequency".to_string(),
            primary_key: "word_id".to_string(),
            description: None,
            storage_location: None,
        };

        let schema = builder.build_schema(vec![entity]).await.expect("Failed to build schema");

        // Query the Delta table data
        let query = r#"
            query {
                word_frequency(word_id: "1") {
                    word_id
                    word
                    part_of_speech
                    frequency_per_million
                }
            }
        "#;

        let request = Request::new(query);
        let response = schema.execute(request).await;

        assert!(response.errors.is_empty(), "Query should execute without errors");

        let data = response.data.into_json().expect("Should have data");
        let word_freq = data.get("word_frequency").expect("Should have word_frequency field");
        
        assert!(!word_freq.is_null(), "Should return data from Delta table");

        println!("Delta table query result: {}", serde_json::to_string_pretty(&data).unwrap());
    }

    #[tokio::test]
    async fn test_multiple_delta_tables() {
        let _ = tracing_subscriber::fmt::try_init();

        // Skip if Delta tables don't exist
        if !Path::new("examples/delta/nouns").exists() {
            eprintln!("Skipping test: Delta tables not found");
            return;
        }

        let mut builder = SchemaBuilder::new();

        // Register multiple Delta tables
        builder.register_table_from_path("nouns", "examples/delta/nouns").await.expect("Failed to register nouns");
        builder.register_table_from_path("verbs", "examples/delta/verbs").await.expect("Failed to register verbs");

        let entities = vec![
            EntityConfig {
                table: "nouns".to_string(),
                graphql_name: "Noun".to_string(),
                primary_key: "noun_id".to_string(),
                description: None,
                storage_location: None,
            },
            EntityConfig {
                table: "verbs".to_string(),
                graphql_name: "Verb".to_string(),
                primary_key: "verb_id".to_string(),
                description: None,
                storage_location: None,
            },
        ];

        let schema = builder.build_schema(entities).await.expect("Failed to build multi-table schema");

        // Query both tables
        let query = r#"
            query {
                noun(noun_id: "1") {
                    word
                    type
                }
                verb(verb_id: "1") {
                    word
                    tense
                }
            }
        "#;

        let request = async_graphql::Request::new(query);
        let response = schema.execute(request).await;

        assert!(response.errors.is_empty(), "Multi-table query should work");

        let data = response.data.into_json().unwrap();
        assert!(data.get("noun").is_some());
        assert!(data.get("verb").is_some());

        println!("Multi-table query result: {}", serde_json::to_string_pretty(&data).unwrap());
    }
}

