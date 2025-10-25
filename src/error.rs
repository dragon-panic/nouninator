use thiserror::Error;

#[derive(Error, Debug)]
pub enum NouninatorError {
    #[error("Unity Catalog API error: {0}")]
    UnityApi(String),
    
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    
    #[error("Authentication failed: {0}")]
    Unauthorized(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),
    
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Schema generation error: {0}")]
    SchemaGeneration(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<toml::de::Error> for NouninatorError {
    fn from(err: toml::de::Error) -> Self {
        NouninatorError::Config(format!("TOML parse error: {}", err))
    }
}

impl From<toml::ser::Error> for NouninatorError {
    fn from(err: toml::ser::Error) -> Self {
        NouninatorError::Serialization(format!("TOML serialization error: {}", err))
    }
}

pub type Result<T> = std::result::Result<T, NouninatorError>;

