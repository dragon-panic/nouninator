use nouninator::config::EntityConfig;
use nouninator::error::Result;



/// Table metadata - descriptions and primary keys for each example table
struct TableMetadata {
    csv_file: &'static str,
    catalog: &'static str,
    schema: &'static str,
    graphql_name: &'static str,
    description: &'static str,
}

/// Create example entities based on grammar/linguistics theme
/// Reads actual CSV files to get real column names
pub fn create_example_entities() -> Vec<EntityConfig> {
    let tables = get_table_metadata();
    let mut entities = Vec::new();
    
    for table in tables {
        let csv_path = format!("examples/data/{}", table.csv_file);
        
        // Read CSV headers to get actual columns
        match read_csv_headers(&csv_path) {
            Ok(columns) => {
                // Infer primary key from actual columns
                let primary_key = infer_primary_key_from_columns(&columns, table.csv_file);
                
                let table_name = table.csv_file.replace(".csv", "");
                let full_table_path = table_name.to_string();  // e.g., "nouns", "verbs"
                
                // Set storage_location to local Delta table path
                let storage_location = format!("examples/delta/{}", table_name);
                
                entities.push(EntityConfig {
                    table: full_table_path,
                    graphql_name: table.graphql_name.to_string(),
                    primary_key,
                    description: Some(table.description.to_string()),
                    storage_location: Some(storage_location),
                });
                
                tracing::debug!(
                    "Loaded {} with columns: {:?}",
                    table.csv_file,
                    columns
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to read CSV {}: {}. Using fallback.",
                    table.csv_file,
                    e
                );
                // Fallback: create entity without reading CSV
                let table_name = table.csv_file.replace(".csv", "");
                let full_table_path = format!("{}.{}.{}", table.catalog, table.schema, table_name);
                
                // Set storage_location to local Delta table path
                let storage_location = format!("examples/delta/{}", table_name);
                
                entities.push(EntityConfig {
                    table: full_table_path,
                    graphql_name: table.graphql_name.to_string(),
                    primary_key: infer_primary_key_from_name(table.csv_file),
                    description: Some(table.description.to_string()),
                    storage_location: Some(storage_location),
                });
            }
        }
    }
    
    entities
}

/// Get metadata for all example tables
fn get_table_metadata() -> Vec<TableMetadata> {
    vec![
        TableMetadata {
            csv_file: "nouns.csv",
            catalog: "language",
            schema: "parts_of_speech",
            graphql_name: "Noun",
            description: "A person, place, thing, or idea. Core entities in language.",
        },
        TableMetadata {
            csv_file: "verbs.csv",
            catalog: "language",
            schema: "parts_of_speech",
            graphql_name: "Verb",
            description: "Action words that express what someone or something does.",
        },
        TableMetadata {
            csv_file: "adjectives.csv",
            catalog: "language",
            schema: "parts_of_speech",
            graphql_name: "Adjective",
            description: "Descriptive words that modify nouns and pronouns.",
        },
        TableMetadata {
            csv_file: "sentences.csv",
            catalog: "language",
            schema: "syntax",
            graphql_name: "Sentence",
            description: "Complete grammatical units expressing a complete thought.",
        },
        TableMetadata {
            csv_file: "synonyms.csv",
            catalog: "language",
            schema: "relationships",
            graphql_name: "Synonym",
            description: "Words with similar meanings in specific contexts.",
        },
        TableMetadata {
            csv_file: "word_frequency.csv",
            catalog: "language",
            schema: "metrics",
            graphql_name: "WordFrequency",
            description: "Usage statistics for words across different corpora.",
        },
    ]
}

/// Read CSV file headers
fn read_csv_headers(path: &str) -> Result<Vec<String>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    
    let file = File::open(path)
        .map_err(nouninator::error::NouninatorError::Io)?;
    
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();
    reader.read_line(&mut first_line)
        .map_err(nouninator::error::NouninatorError::Io)?;
    
    let headers: Vec<String> = first_line
        .trim()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    Ok(headers)
}

/// Infer primary key from actual column names
fn infer_primary_key_from_columns(columns: &[String], filename: &str) -> String {
    // Strategy 1: Look for exact match with table name + _id
    let table_name = filename.replace(".csv", "");
    let expected_pk = format!("{}_id", table_name.trim_end_matches('s')); // Remove plural 's'
    
    if columns.iter().any(|c| c == &expected_pk) {
        return expected_pk;
    }
    
    // Strategy 2: Look for any column ending with _id
    if let Some(id_col) = columns.iter().find(|c| c.ends_with("_id")) {
        return id_col.clone();
    }
    
    // Strategy 3: Look for column named "id"
    if columns.iter().any(|c| c == "id") {
        return "id".to_string();
    }
    
    // Strategy 4: Use first column as fallback
    columns.first()
        .cloned()
        .unwrap_or_else(|| "id".to_string())
}

/// Fallback: infer primary key just from filename
fn infer_primary_key_from_name(filename: &str) -> String {
    let table_name = filename.replace(".csv", "");
    
    // Try singular form + _id (e.g., nouns -> noun_id)
    if table_name.ends_with('s') {
        format!("{}_id", table_name.trim_end_matches('s'))
    } else {
        format!("{}_id", table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_example_entities() {
        let entities = create_example_entities();
        
        // Should have 6 entities (matching CSV files)
        assert_eq!(entities.len(), 6);
        
        // All should be valid
        for entity in &entities {
            assert!(entity.validate().is_ok(), "Entity {} failed validation", entity.graphql_name);
        }
        
        // Check specific entities exist
        let noun = entities.iter().find(|e| e.graphql_name == "Noun");
        assert!(noun.is_some());
        assert_eq!(noun.unwrap().table, "language.parts_of_speech.nouns");
        assert_eq!(noun.unwrap().primary_key, "noun_id");
        
        let verb = entities.iter().find(|e| e.graphql_name == "Verb");
        assert!(verb.is_some());
        assert_eq!(verb.unwrap().primary_key, "verb_id");
        
        // Synonyms uses "id" not "synonym_id"
        let synonym = entities.iter().find(|e| e.graphql_name == "Synonym");
        assert!(synonym.is_some());
        assert_eq!(synonym.unwrap().primary_key, "id");
        
        // All should have descriptions
        for entity in &entities {
            assert!(entity.description.is_some(), "Entity {} missing description", entity.graphql_name);
        }
    }

    #[test]
    fn test_entity_naming_conventions() {
        let entities = create_example_entities();
        
        for entity in &entities {
            // Should use PascalCase
            assert!(entity.graphql_name.chars().next().unwrap().is_uppercase(),
                "Entity {} not PascalCase", entity.graphql_name);
            
            // Should have valid table format
            assert_eq!(entity.table.split('.').count(), 3,
                "Entity {} has invalid table format", entity.table);
            
            // Should have valid primary key
            assert!(!entity.primary_key.is_empty(),
                "Entity {} has empty primary key", entity.graphql_name);
        }
    }

    #[test]
    fn test_infer_primary_key_from_columns() {
        // Test with noun_id column
        let columns = vec!["noun_id".to_string(), "word".to_string(), "type".to_string()];
        let pk = infer_primary_key_from_columns(&columns, "nouns.csv");
        assert_eq!(pk, "noun_id");
        
        // Test with id column
        let columns = vec!["id".to_string(), "word1".to_string(), "word2".to_string()];
        let pk = infer_primary_key_from_columns(&columns, "synonyms.csv");
        assert_eq!(pk, "id");
        
        // Test fallback to first column
        let columns = vec!["custom_key".to_string(), "data".to_string()];
        let pk = infer_primary_key_from_columns(&columns, "test.csv");
        assert_eq!(pk, "custom_key");
    }

    #[test]
    fn test_infer_primary_key_from_name() {
        assert_eq!(infer_primary_key_from_name("nouns.csv"), "noun_id");
        assert_eq!(infer_primary_key_from_name("verbs.csv"), "verb_id");
        assert_eq!(infer_primary_key_from_name("word_frequency.csv"), "word_frequency_id");
    }

    #[test]
    fn test_read_csv_headers() {
        // Test reading actual CSV file
        let result = read_csv_headers("examples/data/nouns.csv");
        assert!(result.is_ok(), "Failed to read nouns.csv");
        
        let headers = result.unwrap();
        assert!(headers.contains(&"noun_id".to_string()));
        assert!(headers.contains(&"word".to_string()));
        assert!(headers.len() > 5); // Should have multiple columns
    }

    #[test]
    fn test_get_table_metadata() {
        let metadata = get_table_metadata();
        
        // Should have 6 tables
        assert_eq!(metadata.len(), 6);
        
        // Check specific tables exist
        assert!(metadata.iter().any(|m| m.csv_file == "nouns.csv"));
        assert!(metadata.iter().any(|m| m.csv_file == "verbs.csv"));
        assert!(metadata.iter().any(|m| m.csv_file == "adjectives.csv"));
        
        // All should have required fields
        for meta in &metadata {
            assert!(!meta.csv_file.is_empty());
            assert!(!meta.catalog.is_empty());
            assert!(!meta.schema.is_empty());
            assert!(!meta.graphql_name.is_empty());
            assert!(!meta.description.is_empty());
        }
    }
}

