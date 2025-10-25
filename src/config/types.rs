use serde::{Deserialize, Serialize};

/// Top-level configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Optional Databricks configuration (not needed for local Delta tables)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub databricks: Option<DatabricksConfig>,
    pub server: ServerConfig,
    pub entity: Vec<EntityConfig>,
}

/// Databricks connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabricksConfig {
    /// Databricks workspace URL (e.g., "https://dbc-xxx-yyy.cloud.databricks.com")
    pub host: String,
    // Token is read from DATABRICKS_TOKEN environment variable
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Port to bind the server to
    #[serde(default = "default_port")]
    pub port: u16,
    
    /// Interface to bind the server to
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_port() -> u16 {
    4000
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}

/// Entity (table) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityConfig {
    /// Unity Catalog table path (format: "catalog.schema.table")
    pub table: String,
    
    /// GraphQL type name (PascalCase)
    pub graphql_name: String,
    
    /// Primary key column name
    pub primary_key: String,
    
    /// Optional description for GraphQL schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    
    /// Optional storage location (e.g., s3://bucket/path, abfss://container@account/path)
    /// If not provided, the system will attempt to determine it from the table name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
}

impl EntityConfig {
    /// Validate entity configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate table format - allow either:
        // 1. Three-part name: catalog.schema.table (for Unity Catalog)
        // 2. Simple name: table_name (for local files/testing)
        let parts: Vec<&str> = self.table.split('.').collect();
        if parts.len() != 3 && parts.len() != 1 {
            return Err(format!(
                "Table '{}' must be either a simple name or in format 'catalog.schema.table'",
                self.table
            ));
        }
        
        // Validate GraphQL name (PascalCase, alphanumeric)
        if !self.graphql_name.chars().all(|c| c.is_alphanumeric()) {
            return Err(format!(
                "GraphQL name '{}' must be alphanumeric",
                self.graphql_name
            ));
        }
        
        if !self.graphql_name.chars().next().unwrap_or('_').is_uppercase() {
            return Err(format!(
                "GraphQL name '{}' must start with uppercase letter (PascalCase)",
                self.graphql_name
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_validation_valid() {
        let entity = EntityConfig {
            table: "main.sales.customers".to_string(),
            graphql_name: "Customer".to_string(),
            primary_key: "customer_id".to_string(),
            description: None,
            storage_location: None,
        };
        
        assert!(entity.validate().is_ok());
    }

    #[test]
    fn test_entity_validation_invalid_table_format() {
        // Two-part names should be invalid (only 1 or 3 parts allowed)
        let entity = EntityConfig {
            table: "schema.table".to_string(),
            graphql_name: "Customer".to_string(),
            primary_key: "id".to_string(),
            description: None,
            storage_location: None,
        };
        
        assert!(entity.validate().is_err());
    }

    #[test]
    fn test_entity_validation_single_part_table_name() {
        // Single-part names should be valid (for local files/testing)
        let entity = EntityConfig {
            table: "customers".to_string(),
            graphql_name: "Customer".to_string(),
            primary_key: "id".to_string(),
            description: None,
            storage_location: None,
        };
        
        assert!(entity.validate().is_ok());
    }

    #[test]
    fn test_entity_validation_invalid_graphql_name() {
        let entity = EntityConfig {
            table: "main.sales.customers".to_string(),
            graphql_name: "customer".to_string(), // Should be PascalCase
            primary_key: "id".to_string(),
            description: None,
            storage_location: None,
        };
        
        assert!(entity.validate().is_err());
    }

    #[test]
    fn test_entity_validation_non_alphanumeric_graphql_name() {
        let entity = EntityConfig {
            table: "main.sales.customers".to_string(),
            graphql_name: "Customer-Type".to_string(),
            primary_key: "id".to_string(),
            description: None,
            storage_location: None,
        };
        
        assert!(entity.validate().is_err());
    }
}

