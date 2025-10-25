use crate::config::EntityConfig;
use crate::unity::{TableInfo, UnityClient};
use crate::error::Result;

/// Discover entities from Unity Catalog and convert to entity configurations
pub async fn discover_entities(
    client: &UnityClient,
    catalog: &str,
    schema: &str,
) -> Result<Vec<EntityConfig>> {
    // List all tables in the schema
    let tables = client.list_tables(catalog, schema).await?;
    
    let mut entities = Vec::new();
    
    for table in tables {
        // Skip non-Delta tables
        if table.data_source_format != "DELTA" {
            tracing::warn!(
                "Skipping non-Delta table: {}.{}.{} (format: {})",
                table.catalog_name,
                table.schema_name,
                table.name,
                table.data_source_format
            );
            continue;
        }
        
        let full_name = format!(
            "{}.{}.{}",
            table.catalog_name,
            table.schema_name,
            table.name
        );
        
        // Fetch detailed metadata
        let metadata = client.get_table(&full_name).await?;
        
        // Infer primary key
        let primary_key = infer_primary_key(&metadata, &table)?;
        
        // Convert to entity config
        let entity = EntityConfig {
            table: full_name,
            graphql_name: to_pascal_case(&table.name),
            primary_key,
            description: table.comment.or(metadata.comment),
            storage_location: table.storage_location.or(metadata.storage_location),
        };
        
        entities.push(entity);
    }
    
    Ok(entities)
}

/// Infer primary key from table metadata
fn infer_primary_key(
    metadata: &crate::unity::types::TableMetadata,
    table: &TableInfo,
) -> Result<String> {
    // 1. Check table properties for explicit primary_key
    if let Some(pk) = table.properties.get("primary_key") {
        return Ok(pk.clone());
    }
    
    if let Some(pk) = metadata.properties.get("primary_key") {
        return Ok(pk.clone());
    }
    
    // 2. Look for column named "id"
    if let Some(col) = metadata.columns.iter().find(|c| c.name == "id") {
        return Ok(col.name.clone());
    }
    
    // 3. Look for first column ending with "_id"
    if let Some(col) = metadata.columns.iter().find(|c| c.name.ends_with("_id")) {
        return Ok(col.name.clone());
    }
    
    // 4. Fall back to first column
    if let Some(first_col) = metadata.columns.first() {
        tracing::warn!(
            "No obvious primary key found for {}.{}.{}, using first column: {}",
            metadata.catalog_name,
            metadata.schema_name,
            metadata.name,
            first_col.name
        );
        return Ok(first_col.name.clone());
    }
    
    // This shouldn't happen for valid tables
    Err(crate::error::NouninatorError::Config(
        format!("Table {}.{}.{} has no columns",
            metadata.catalog_name,
            metadata.schema_name,
            metadata.name
        )
    ))
}

/// Convert snake_case to PascalCase
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    let mut result = first.to_uppercase().collect::<String>();
                    result.push_str(chars.as_str());
                    result
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("customer"), "Customer");
        assert_eq!(to_pascal_case("customer_orders"), "CustomerOrders");
        assert_eq!(to_pascal_case("order_line_items"), "OrderLineItems");
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn test_to_pascal_case_with_multiple_underscores() {
        assert_eq!(to_pascal_case("my__table"), "MyTable");
    }

    #[test]
    fn test_infer_primary_key_from_properties() {
        let metadata = crate::unity::types::TableMetadata {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            columns: vec![
                crate::unity::types::ColumnInfo {
                    name: "id".to_string(),
                    type_text: "bigint".to_string(),
                    type_name: "bigint".to_string(),
                    position: 0,
                    nullable: false,
                    comment: None,
                }
            ],
            storage_location: None,
            properties: vec![("primary_key".to_string(), "id".to_string())]
                .into_iter()
                .collect(),
            comment: None,
        };
        
        let table = TableInfo {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            storage_location: None,
            comment: None,
            properties: std::collections::HashMap::new(),
        };
        
        let pk = infer_primary_key(&metadata, &table).unwrap();
        assert_eq!(pk, "id");
    }

    #[test]
    fn test_infer_primary_key_from_id_column() {
        let metadata = crate::unity::types::TableMetadata {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            columns: vec![
                crate::unity::types::ColumnInfo {
                    name: "id".to_string(),
                    type_text: "bigint".to_string(),
                    type_name: "bigint".to_string(),
                    position: 0,
                    nullable: false,
                    comment: None,
                }
            ],
            storage_location: None,
            properties: std::collections::HashMap::new(),
            comment: None,
        };
        
        let table = TableInfo {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            storage_location: None,
            comment: None,
            properties: std::collections::HashMap::new(),
        };
        
        let pk = infer_primary_key(&metadata, &table).unwrap();
        assert_eq!(pk, "id");
    }

    #[test]
    fn test_infer_primary_key_from_id_suffix() {
        let metadata = crate::unity::types::TableMetadata {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            columns: vec![
                crate::unity::types::ColumnInfo {
                    name: "customer_id".to_string(),
                    type_text: "bigint".to_string(),
                    type_name: "bigint".to_string(),
                    position: 0,
                    nullable: false,
                    comment: None,
                }
            ],
            storage_location: None,
            properties: std::collections::HashMap::new(),
            comment: None,
        };
        
        let table = TableInfo {
            name: "test".to_string(),
            catalog_name: "main".to_string(),
            schema_name: "test".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            storage_location: None,
            comment: None,
            properties: std::collections::HashMap::new(),
        };
        
        let pk = infer_primary_key(&metadata, &table).unwrap();
        assert_eq!(pk, "customer_id");
    }
}

