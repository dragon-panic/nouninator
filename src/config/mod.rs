mod types;

pub use types::{Config, DatabricksConfig, EntityConfig, ServerConfig};

use crate::error::{NouninatorError, Result};
use std::fs;

/// Load configuration from a TOML file
pub fn load_config(path: &str) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .map_err(|e| NouninatorError::Config(format!("Failed to read config file '{}': {}", path, e)))?;
    
    let config: Config = toml::from_str(&contents)?;
    
    // Validate all entities
    for entity in &config.entity {
        entity.validate()
            .map_err(NouninatorError::Config)?;
    }
    
    // Validate Databricks host is a valid URL (if present)
    if let Some(ref databricks) = config.databricks {
        if !databricks.host.starts_with("http://") && !databricks.host.starts_with("https://") {
            return Err(NouninatorError::Config(
                format!("Databricks host '{}' must be a valid URL (http:// or https://)", databricks.host)
            ));
        }
    }
    
    Ok(config)
}

/// Save configuration to a TOML file
pub fn save_config(config: &Config, path: &str) -> Result<()> {
    // Validate all entities before saving
    for entity in &config.entity {
        entity.validate()
            .map_err(NouninatorError::Config)?;
    }
    
    let toml_string = toml::to_string_pretty(config)?;
    fs::write(path, toml_string)
        .map_err(|e| NouninatorError::Config(format!("Failed to write config file '{}': {}", path, e)))?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_load_valid_config() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
[databricks]
host = "https://dbc-xxx-yyy.cloud.databricks.com"

[server]
port = 4000
bind = "0.0.0.0"

[[entity]]
table = "main.sales.customers"
graphql_name = "Customer"
primary_key = "customer_id"
description = "Customer data"
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        
        let config = load_config(temp_file.path().to_str().unwrap());
        assert!(config.is_ok());
        
        let config = config.unwrap();
        assert!(config.databricks.is_some());
        assert_eq!(config.databricks.as_ref().unwrap().host, "https://dbc-xxx-yyy.cloud.databricks.com");
        assert_eq!(config.server.port, 4000);
        assert_eq!(config.entity.len(), 1);
        assert_eq!(config.entity[0].table, "main.sales.customers");
    }

    #[test]
    fn test_load_invalid_databricks_host() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
[databricks]
host = "invalid-url"

[server]
port = 4000
bind = "0.0.0.0"

[[entity]]
table = "main.sales.customers"
graphql_name = "Customer"
primary_key = "customer_id"
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        
        let config = load_config(temp_file.path().to_str().unwrap());
        assert!(config.is_err());
    }

    #[test]
    fn test_save_and_load_config() {
        let config = Config {
            databricks: Some(DatabricksConfig {
                host: "https://test.databricks.com".to_string(),
            }),
            server: ServerConfig {
                port: 4000,
                bind: "0.0.0.0".to_string(),
            },
            entity: vec![
                EntityConfig {
                    table: "main.test.table1".to_string(),
                    graphql_name: "Table1".to_string(),
                    primary_key: "id".to_string(),
                    description: Some("Test table".to_string()),
                    storage_location: None,
                }
            ],
        };
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        
        save_config(&config, path).unwrap();
        let loaded_config = load_config(path).unwrap();
        
        assert_eq!(loaded_config.databricks.as_ref().unwrap().host, config.databricks.as_ref().unwrap().host);
        assert_eq!(loaded_config.entity.len(), 1);
        assert_eq!(loaded_config.entity[0].table, "main.test.table1");
    }

    #[test]
    fn test_load_config_without_databricks() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
[server]
port = 4000
bind = "0.0.0.0"

[[entity]]
table = "local_table"
graphql_name = "LocalTable"
primary_key = "id"
description = "A local table"
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        
        let config = load_config(temp_file.path().to_str().unwrap());
        assert!(config.is_ok());
        
        let config = config.unwrap();
        assert!(config.databricks.is_none());
        assert_eq!(config.server.port, 4000);
        assert_eq!(config.entity.len(), 1);
        assert_eq!(config.entity[0].table, "local_table");
    }
}

