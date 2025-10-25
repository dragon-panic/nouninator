use nouninator::error::Result;
use nouninator::schema::SchemaBuilder;
use axum::{routing::get, routing::post, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Run the serve command to start the GraphQL server
pub async fn run(config_path: String, port: u16) -> Result<()> {
    
    tracing::info!("📖 Loading configuration from {}", config_path);
    
    // Load config
    let config = nouninator::config::load_config(&config_path)?;
    
    // Use provided port or default from config
    let server_port = if port != 4000 { port } else { config.server.port };
    
    tracing::info!("🔧 Building GraphQL schema for {} entities...", config.entity.len());
    
    // Create schema builder
    let mut builder = SchemaBuilder::new();
    
    // Register all tables
    for entity in &config.entity {
        let table_path = determine_table_path(entity);
        tracing::info!("   Registering {} from {}", entity.graphql_name, table_path);
        
        builder.register_table_from_path(&entity.table, &table_path).await?;
    }
    
    // Build the GraphQL schema
    let schema = builder.build_schema(config.entity).await?;
    
    tracing::info!("✅ Schema built successfully");
    tracing::info!("🚀 GraphQL server running on http://localhost:{}", server_port);
    tracing::info!("📊 Playground: http://localhost:{}/graphql", server_port);
    tracing::info!("💡 Press Ctrl+C to stop the server");
    
    // Start the HTTP server
    start_http_server(schema, server_port).await
}

fn determine_table_path(entity: &nouninator::config::EntityConfig) -> String {
    // Storage location should always be explicitly set in config
    entity.storage_location
        .clone()
        .unwrap_or_else(|| {
            tracing::warn!(
                "Entity '{}' does not have storage_location set. Using table name as path.",
                entity.graphql_name
            );
            entity.table.clone()
        })
}

async fn start_http_server(
    schema: async_graphql::dynamic::Schema,
    port: u16,
) -> Result<()> {
    
    // Wrap schema in Arc for sharing across handlers
    let schema = Arc::new(schema);
    
    // Create the router with GraphQL endpoints
    let app = Router::new()
        .route("/graphql", post(graphql_handler).get(graphql_playground))
        .route("/health", get(health_check))
        .with_state(schema)
        .layer(CorsLayer::permissive());
    
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| nouninator::error::NouninatorError::Config(
            format!("Failed to bind to port {}: {}. Port may be in use.", port, e)
        ))?;
    
    axum::serve(listener, app)
        .await
        .map_err(|e| nouninator::error::NouninatorError::Config(
            format!("Server error: {}", e)
        ))?;
    
    Ok(())
}

async fn graphql_handler(
    axum::extract::State(schema): axum::extract::State<std::sync::Arc<async_graphql::dynamic::Schema>>,
    axum::Json(request): axum::Json<async_graphql::Request>,
) -> axum::Json<async_graphql::Response> {
    axum::Json(schema.execute(request).await)
}

async fn graphql_playground() -> axum::response::Html<String> {
    axum::response::Html(
        async_graphql::http::playground_source(
            async_graphql::http::GraphQLPlaygroundConfig::new("/graphql")
        )
    )
}

async fn health_check() -> &'static str {
    "OK"
}

