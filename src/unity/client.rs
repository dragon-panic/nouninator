use crate::error::{NouninatorError, Result};
use crate::unity::types::{ListTablesResponse, TableInfo, TableMetadata};
use reqwest::{Client, StatusCode};

/// Unity Catalog client for interacting with Databricks metadata APIs.
///
/// # Example
///
/// ```no_run
/// use nouninator::unity::UnityClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = UnityClient::new(
///     "https://workspace.databricks.com".to_string(),
///     "dapi_token_here".to_string()
/// );
///
/// let tables = client.list_tables("main", "sales").await?;
/// # Ok(())
/// # }
/// ```
pub struct UnityClient {
    base_url: String,
    token: String,
    client: Client,
}

impl UnityClient {
    /// Create a new Unity Catalog client
    ///
    /// # Arguments
    ///
    /// * `host` - Databricks workspace URL (e.g., "https://dbc-xxx-yyy.cloud.databricks.com")
    /// * `token` - Databricks access token
    pub fn new(host: String, token: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            base_url: host.trim_end_matches('/').to_string(),
            token,
            client,
        }
    }
    
    /// List tables in a schema
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog name
    /// * `schema` - Schema name
    ///
    /// # Returns
    ///
    /// A vector of table information
    ///
    /// # API Endpoint
    ///
    /// `GET /api/2.1/unity-catalog/tables`
    /// Query params: catalog_name, schema_name
    pub async fn list_tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<TableInfo>> {
        let url = format!(
            "{}/api/2.1/unity-catalog/tables",
            self.base_url
        );
        
        tracing::debug!("Listing tables in {}.{}", catalog, schema);
        
        let response = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .query(&[("catalog_name", catalog), ("schema_name", schema)])
            .send()
            .await?;
        
        self.handle_response_error(&response)?;
        
        let list_response: ListTablesResponse = response.json().await
            .map_err(|e| NouninatorError::UnityApi(format!("Failed to parse response: {}", e)))?;
        
        Ok(list_response.tables.unwrap_or_default())
    }
    
    /// Get detailed table metadata
    ///
    /// # Arguments
    ///
    /// * `full_name` - Full table name in format "catalog.schema.table"
    ///
    /// # Returns
    ///
    /// Detailed table metadata including columns
    ///
    /// # API Endpoint
    ///
    /// `GET /api/2.1/unity-catalog/tables/{full_name}`
    pub async fn get_table(
        &self,
        full_name: &str,
    ) -> Result<TableMetadata> {
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.base_url,
            full_name
        );
        
        tracing::debug!("Getting table metadata for {}", full_name);
        
        let response = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;
        
        self.handle_response_error(&response)?;
        
        let metadata: TableMetadata = response.json().await
            .map_err(|e| NouninatorError::UnityApi(format!("Failed to parse response: {}", e)))?;
        
        Ok(metadata)
    }
    
    /// Handle HTTP error responses
    fn handle_response_error(&self, response: &reqwest::Response) -> Result<()> {
        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(NouninatorError::Unauthorized(
                    "Invalid or expired Databricks token".to_string()
                ))
            }
            StatusCode::NOT_FOUND => {
                Err(NouninatorError::TableNotFound(
                    "Catalog, schema, or table not found".to_string()
                ))
            }
            status => {
                Err(NouninatorError::UnityApi(
                    format!("API request failed with status {}", status)
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unity_client_creation() {
        let client = UnityClient::new(
            "https://test.databricks.com".to_string(),
            "test_token".to_string()
        );
        
        assert_eq!(client.base_url, "https://test.databricks.com");
        assert_eq!(client.token, "test_token");
    }

    #[test]
    fn test_unity_client_trims_trailing_slash() {
        let client = UnityClient::new(
            "https://test.databricks.com/".to_string(),
            "test_token".to_string()
        );
        
        assert_eq!(client.base_url, "https://test.databricks.com");
    }
}

