use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from listing tables in a schema
#[derive(Debug, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Option<Vec<TableInfo>>,
}

/// Basic table information from list operation
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String, // MANAGED, EXTERNAL
    pub data_source_format: String, // DELTA
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Detailed table metadata from get operation
#[derive(Debug, Clone, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub columns: Vec<ColumnInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Column information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_text: String, // Full type definition
    pub type_name: String, // "bigint", "string", "timestamp", etc.
    pub position: i32,
    #[serde(default)]
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_table_info() {
        let json = r#"{
            "name": "customers",
            "catalog_name": "main",
            "schema_name": "sales",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/path",
            "comment": "Customer data",
            "properties": {
                "primary_key": "customer_id"
            }
        }"#;
        
        let table_info: TableInfo = serde_json::from_str(json).unwrap();
        assert_eq!(table_info.name, "customers");
        assert_eq!(table_info.catalog_name, "main");
        assert_eq!(table_info.schema_name, "sales");
    }

    #[test]
    fn test_deserialize_column_info() {
        let json = r#"{
            "name": "customer_id",
            "type_text": "bigint",
            "type_name": "bigint",
            "position": 0,
            "nullable": false,
            "comment": "Primary key"
        }"#;
        
        let column_info: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(column_info.name, "customer_id");
        assert_eq!(column_info.type_name, "bigint");
        assert_eq!(column_info.position, 0);
        assert!(!column_info.nullable);
    }
}

