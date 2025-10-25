use nouninator::config::{Config, DatabricksConfig, ServerConfig};
use nouninator::error::Result;
use nouninator::unity::{UnityClient, discovery};

/// Run the init command to discover entities from Unity Catalog or generate example configuration
pub async fn run(
    example: bool,
    host: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    output: Option<String>,
) -> Result<()> {
    if example {
        run_example(output).await
    } else {
        run_unity_catalog(
            host.expect("host required for Unity Catalog"),
            catalog.expect("catalog required for Unity Catalog"),
            schema.expect("schema required for Unity Catalog"),
            output,
        ).await
    }
}

/// Generate example configuration
async fn run_example(output: Option<String>) -> Result<()> {
    tracing::info!("🎨 Setting up example environment with grammar-themed tables...");
    tracing::info!("");
    
    // Step 1: Convert CSV files to Delta tables
    let output_dir = "examples/delta";
    crate::cli::convert::convert_example_data(output_dir.to_string()).await?;
    tracing::info!("");
    
    // Step 2: Generate configuration pointing to the Delta tables
    tracing::info!("📝 Generating configuration...");
    let entities = crate::cli::example::create_example_entities();
    
    tracing::info!("✨ Created {} example entities:", entities.len());
    for entity in &entities {
        tracing::info!("   • {} ({})", entity.graphql_name, entity.table);
        if let Some(desc) = &entity.description {
            tracing::info!("     {}", desc);
        }
    }
    
    let config = Config {
        databricks: None,
        server: ServerConfig {
            port: 4000,
            bind: "0.0.0.0".to_string(),
        },
        entity: entities,
    };
    
    // Output to stdout or file
    let wrote_to_file = if let Some(output_path) = output {
        nouninator::config::save_config(&config, &output_path)?;
        tracing::info!("📝 Generated example configuration: {}", output_path);
        true
    } else {
        // Output to stdout
        let toml_string = toml::to_string_pretty(&config)?;
        println!("{}", toml_string);
        false
    };
    
    tracing::info!("");
    tracing::info!("🎯 This example showcases:");
    tracing::info!("   • Multiple related entities (parts of speech)");
    tracing::info!("   • Primary key inference");
    tracing::info!("   • GraphQL naming conventions");
    tracing::info!("   • Table descriptions");
    tracing::info!("   • Local Delta tables (no Databricks required!)");
    tracing::info!("");
    tracing::info!("💡 Next steps:");
    if wrote_to_file {
        tracing::info!("   1. Review the generated configuration file");
        tracing::info!("   2. Start server with 'cargo run -- serve --config <file>'");
    } else {
        tracing::info!("   1. Save the configuration to a file: cargo run -- init --example --output nouninator.toml");
        tracing::info!("   2. Start server with 'cargo run -- serve'");
    }
    
    Ok(())
}

/// Discover entities from Unity Catalog
async fn run_unity_catalog(
    host: String,
    catalog: String,
    schema: String,
    output: Option<String>,
) -> Result<()> {
    tracing::info!("🔍 Discovering entities in {}.{}...", catalog, schema);
    
    // 1. Get token from environment
    let token = std::env::var("DATABRICKS_TOKEN")
        .map_err(|_| nouninator::error::NouninatorError::Config(
            "DATABRICKS_TOKEN environment variable not set".to_string()
        ))?;
    
    // 2. Create Unity Catalog client
    let client = UnityClient::new(host.clone(), token);
    
    // 3. Discover entities
    let entities = discovery::discover_entities(&client, &catalog, &schema).await?;
    
    if entities.is_empty() {
        tracing::warn!("No Delta tables found in {}.{}", catalog, schema);
        return Ok(());
    }
    
    tracing::info!("✅ Found {} Delta table(s)", entities.len());
    
    // Log discovered entities
    for entity in &entities {
        tracing::info!(
            "   • {} -> {}",
            entity.table,
            entity.graphql_name
        );
    }
    
    // 4. Build config
    let config = Config {
        databricks: Some(DatabricksConfig {
            host,
        }),
        server: ServerConfig {
            port: 4000,
            bind: "0.0.0.0".to_string(),
        },
        entity: entities,
    };
    
    // 5. Write to file or stdout
    if let Some(output_path) = output {
        nouninator::config::save_config(&config, &output_path)?;
        tracing::info!("📝 Generated {}", output_path);
        tracing::info!("🚀 Ready to serve! Run: nouninator serve --config {}", output_path);
    } else {
        // Output to stdout
        let toml_string = toml::to_string_pretty(&config)?;
        println!("{}", toml_string);
        tracing::info!("💡 Tip: Add --output <file> to save to a file instead of stdout");
    }
    
    Ok(())
}

