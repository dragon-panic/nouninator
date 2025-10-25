/// Integration tests for schema generation using CSV test data
///
/// These tests verify that the schema builder can:
/// - Load CSV files as tables
/// - Generate GraphQL schemas from Arrow schemas
/// - Execute queries against the data
/// - Handle various data types (strings, integers, timestamps, etc.)

mod schema_tests {
    use nouninator::config::EntityConfig;
    use nouninator::schema::SchemaBuilder;
    use std::path::PathBuf;

    /// Helper to get the path to test CSV files
    fn get_csv_path(filename: &str) -> String {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("examples");
        path.push("data");
        path.push(filename);
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn test_word_frequency_schema_generation() {
        // Initialize tracing for debugging
        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register the CSV file as a table
        let csv_path = get_csv_path("word_frequency.csv");
        builder
            .register_table_from_path("word_frequency", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "word_frequency".to_string(),
            graphql_name: "WordFrequency".to_string(),
            primary_key: "word_id".to_string(),
            description: Some("Word frequency data from corpus".to_string()),
            storage_location: None,
        };

        // Build schema
        let _schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Verify schema is created successfully (no assertion needed, just that it didn't error)
        assert!(true, "Schema created successfully");
    }

    #[tokio::test]
    async fn test_nouns_schema_generation() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register the CSV file
        let csv_path = get_csv_path("nouns.csv");
        builder
            .register_table_from_path("nouns", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "nouns".to_string(),
            graphql_name: "Noun".to_string(),
            primary_key: "noun_id".to_string(),
            description: Some("Noun definitions and examples".to_string()),
            storage_location: None,
        };

        // Build schema
        let _schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        assert!(true, "Schema created successfully");
    }

    #[tokio::test]
    async fn test_verbs_schema_generation() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register the CSV file
        let csv_path = get_csv_path("verbs.csv");
        builder
            .register_table_from_path("verbs", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "verbs".to_string(),
            graphql_name: "Verb".to_string(),
            primary_key: "verb_id".to_string(),
            description: Some("Verb definitions and examples".to_string()),
            storage_location: None,
        };

        // Build schema
        let _schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        assert!(true, "Schema created successfully");
    }

    #[tokio::test]
    async fn test_multiple_entities() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register multiple CSV files
        let nouns_path = get_csv_path("nouns.csv");
        builder
            .register_table_from_path("nouns", &nouns_path)
            .await
            .expect("Failed to register nouns CSV");

        let verbs_path = get_csv_path("verbs.csv");
        builder
            .register_table_from_path("verbs", &verbs_path)
            .await
            .expect("Failed to register verbs CSV");

        // Create entity configs
        let entities = vec![
            EntityConfig {
                table: "nouns".to_string(),
                graphql_name: "Noun".to_string(),
                primary_key: "noun_id".to_string(),
                description: Some("Noun definitions".to_string()),
                storage_location: None,
            },
            EntityConfig {
                table: "verbs".to_string(),
                graphql_name: "Verb".to_string(),
                primary_key: "verb_id".to_string(),
                description: Some("Verb definitions".to_string()),
                storage_location: None,
            },
        ];

        // Build schema with multiple entities
        let _schema = builder
            .build_schema(entities)
            .await
            .expect("Failed to build schema with multiple entities");

        assert!(true, "Multi-entity schema created successfully");
    }

    #[tokio::test]
    async fn test_query_execution_get_by_id() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file
        let csv_path = get_csv_path("word_frequency.csv");
        builder
            .register_table_from_path("word_frequency", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "word_frequency".to_string(),
            graphql_name: "WordFrequency".to_string(),
            primary_key: "word_id".to_string(),
            description: None,
            storage_location: None,
        };

        // Build schema
        let schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Execute a GraphQL query to get a specific word by ID
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

        // Check that the query executed successfully
        assert!(response.errors.is_empty(), "Query had errors: {:?}", response.errors);

        // Check that we got data back
        let data = response.data.into_json().expect("Failed to get data");
        assert!(!data.is_null(), "Query returned no data");

        println!("Query result: {}", serde_json::to_string_pretty(&data).unwrap());

        // Verify the structure of the returned data
        let word_freq = data.get("word_frequency").expect("Missing word_frequency field");
        assert!(!word_freq.is_null(), "word_frequency should not be null");

        let word = word_freq.get("word").expect("Missing word field");
        assert_eq!(word.as_str().unwrap(), "the", "Expected word 'the'");
    }

    #[tokio::test]
    async fn test_query_execution_list() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file
        let csv_path = get_csv_path("nouns.csv");
        builder
            .register_table_from_path("nouns", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "nouns".to_string(),
            graphql_name: "Noun".to_string(),
            primary_key: "noun_id".to_string(),
            description: None,
            storage_location: None,
        };

        // Build schema
        let schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Execute a GraphQL query to list nouns with pagination
        let query = r#"
            query {
                list_noun(limit: 5, offset: 0) {
                    noun_id
                    word
                    type
                    definition
                }
            }
        "#;

        let request = Request::new(query);
        let response = schema.execute(request).await;

        // Check that the query executed successfully
        assert!(response.errors.is_empty(), "Query had errors: {:?}", response.errors);

        // Check that we got data back
        let data = response.data.into_json().expect("Failed to get data");
        assert!(!data.is_null(), "Query returned no data");

        println!("List query result: {}", serde_json::to_string_pretty(&data).unwrap());

        // Verify the structure
        let nouns = data.get("list_noun").expect("Missing list_noun field");
        let nouns_array = nouns.as_array().expect("list_noun should be an array");

        assert!(nouns_array.len() > 0, "Expected at least one noun");
        assert!(nouns_array.len() <= 5, "Expected at most 5 nouns");

        // Check first noun has expected fields
        let first_noun = &nouns_array[0];
        assert!(first_noun.get("noun_id").is_some(), "Missing noun_id");
        assert!(first_noun.get("word").is_some(), "Missing word");
        assert!(first_noun.get("type").is_some(), "Missing type");
    }

    #[tokio::test]
    async fn test_timestamp_handling() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file with timestamp column
        let csv_path = get_csv_path("verbs.csv");
        builder
            .register_table_from_path("verbs", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "verbs".to_string(),
            graphql_name: "Verb".to_string(),
            primary_key: "verb_id".to_string(),
            description: None,
            storage_location: None,
        };

        // Build schema
        let schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Execute a GraphQL query that includes the timestamp field
        let query = r#"
            query {
                verb(verb_id: "1") {
                    verb_id
                    word
                    created_at
                }
            }
        "#;

        let request = Request::new(query);
        let response = schema.execute(request).await;

        // Check that the query executed successfully
        assert!(response.errors.is_empty(), "Query had errors: {:?}", response.errors);

        let data = response.data.into_json().expect("Failed to get data");
        println!("Timestamp query result: {}", serde_json::to_string_pretty(&data).unwrap());

        let verb = data.get("verb").expect("Missing verb field");
        let created_at = verb.get("created_at").expect("Missing created_at field");

        // Verify the timestamp is in ISO 8601 format
        let timestamp_str = created_at.as_str().expect("created_at should be a string");
        assert!(timestamp_str.contains("T"), "Timestamp should contain 'T' separator");
        assert!(timestamp_str.contains("Z") || timestamp_str.contains("+"), 
            "Timestamp should contain timezone indicator");
    }

    #[tokio::test]
    async fn test_pagination_offset() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file
        let csv_path = get_csv_path("word_frequency.csv");
        builder
            .register_table_from_path("word_frequency", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "word_frequency".to_string(),
            graphql_name: "WordFrequency".to_string(),
            primary_key: "word_id".to_string(),
            description: None,
            storage_location: None,
        };

        // Build schema
        let schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Query first page
        let query1 = r#"
            query {
                list_word_frequency(limit: 3, offset: 0) {
                    word_id
                    word
                }
            }
        "#;

        let request1 = Request::new(query1);
        let response1 = schema.execute(request1).await;
        assert!(response1.errors.is_empty());

        let data1 = response1.data.into_json().unwrap();
        let page1 = data1.get("list_word_frequency").unwrap().as_array().unwrap();

        // Query second page
        let query2 = r#"
            query {
                list_word_frequency(limit: 3, offset: 3) {
                    word_id
                    word
                }
            }
        "#;

        let request2 = Request::new(query2);
        let response2 = schema.execute(request2).await;
        assert!(response2.errors.is_empty());

        let data2 = response2.data.into_json().unwrap();
        let page2 = data2.get("list_word_frequency").unwrap().as_array().unwrap();

        // Verify pages are different
        assert_eq!(page1.len(), 3, "First page should have 3 items");
        assert_eq!(page2.len(), 3, "Second page should have 3 items");

        let first_word_page1 = page1[0].get("word").unwrap().as_str().unwrap();
        let first_word_page2 = page2[0].get("word").unwrap().as_str().unwrap();

        assert_ne!(first_word_page1, first_word_page2, "Pages should have different data");
    }

    #[tokio::test]
    async fn test_null_handling() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file
        let csv_path = get_csv_path("nouns.csv");
        builder
            .register_table_from_path("nouns", &csv_path)
            .await
            .expect("Failed to register CSV table");

        // Create entity config
        let entity = EntityConfig {
            table: "nouns".to_string(),
            graphql_name: "Noun".to_string(),
            primary_key: "noun_id".to_string(),
            description: None,
            storage_location: None,
        };

        // Build schema
        let schema = builder
            .build_schema(vec![entity])
            .await
            .expect("Failed to build schema");

        // Query a non-existent ID - should return null
        let query = r#"
            query {
                noun(noun_id: "99999") {
                    noun_id
                    word
                }
            }
        "#;

        let request = Request::new(query);
        let response = schema.execute(request).await;

        assert!(response.errors.is_empty(), "Query should not error on missing ID");

        let data = response.data.into_json().unwrap();
        let noun = data.get("noun").unwrap();

        assert!(noun.is_null(), "Non-existent noun should return null");
    }

    #[tokio::test]
    async fn test_type_mapping_integers() {
        use async_graphql::Request;

        let _ = tracing_subscriber::fmt::try_init();

        let mut builder = SchemaBuilder::new();

        // Register CSV file with integer fields
        let csv_path = get_csv_path("word_frequency.csv");
        builder
            .register_table_from_path("word_frequency", &csv_path)
            .await
            .expect("Failed to register CSV table");

        let entity = EntityConfig {
            table: "word_frequency".to_string(),
            graphql_name: "WordFrequency".to_string(),
            primary_key: "word_id".to_string(),
            description: None,
            storage_location: None,
        };

        let schema = builder.build_schema(vec![entity]).await.unwrap();

        // Query with integer fields
        let query = r#"
            query {
                word_frequency(word_id: "1") {
                    word_id
                    frequency_per_million
                    rank
                }
            }
        "#;

        let request = Request::new(query);
        let response = schema.execute(request).await;

        assert!(response.errors.is_empty());

        let data = response.data.into_json().unwrap();
        let word_freq = data.get("word_frequency").unwrap();

        // Verify integer fields are present and properly typed
        let word_id = word_freq.get("word_id").unwrap();
        assert!(word_id.is_string(), "word_id should be string (ID type)");

        let frequency = word_freq.get("frequency_per_million").unwrap();
        assert!(frequency.is_number(), "frequency_per_million should be a number");

        let rank = word_freq.get("rank").unwrap();
        assert!(rank.is_number(), "rank should be a number");
    }
}

