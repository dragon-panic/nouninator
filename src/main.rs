use clap::{Parser, Subcommand};
use nouninator::error::Result;

mod cli;

#[derive(Parser)]
#[command(name = "nouninator")]
#[command(version = "0.1.0")]
#[command(about = "Turn Delta tables into GraphQL APIs", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize configuration from Unity Catalog or generate example configuration
    Init {
        /// Generate example configuration (no Unity Catalog needed!)
        #[arg(long)]
        example: bool,
        
        /// Databricks workspace URL (required unless --example is used)
        #[arg(long, required_unless_present = "example")]
        host: Option<String>,
        
        /// Unity Catalog name (required unless --example is used)
        #[arg(long, required_unless_present = "example")]
        catalog: Option<String>,
        
        /// Schema name (required unless --example is used)
        #[arg(long, required_unless_present = "example")]
        schema: Option<String>,
        
        /// Output config file path (if not specified, outputs to stdout)
        #[arg(long)]
        output: Option<String>,
    },
    
    /// Start GraphQL server
    Serve {
        /// Config file path
        #[arg(long, default_value = "nouninator.toml")]
        config: String,
        
        /// Server port
        #[arg(long, default_value_t = 4000)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();
    
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Init { example, host, catalog, schema, output } => {
            cli::init::run(example, host, catalog, schema, output).await?;
        }
        Commands::Serve { config, port } => {
            cli::serve::run(config, port).await?;
        }
    }
    
    Ok(())
}

