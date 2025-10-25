# Nouninator PRD: Open Source Edition

**Version:** 0.1.0  
**Target Audience:** Claude Sonnet (code generation)  
**Goal:** Generate production-ready Rust codebase for MVP

---

## 1. Product Overview

**What:** A Rust CLI tool and server that automatically generates type-safe GraphQL APIs from Databricks Unity Catalog Delta tables.

**Why:** Data teams build excellent Gold layer data products but frontend teams struggle to consume them. Nouninator bridges this gap by treating Delta tables as first-class API entities.

**Core Value:** From `catalog.schema.table` to queryable GraphQL API in under 60 seconds.

---

## 2. Technical Architecture

### 2.1 System Components

```
┌─────────────────────────────────────────────────────────┐
│                    Nouninator Server                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌────────────────┐         ┌──────────────────┐        │
│  │ GraphQL Layer  │ ───────▶│  Schema Builder  │       │
│  │ (async-graphql)│         │                  │        │
│  └────────┬───────┘         └────────┬─────────┘        │
│           │                          │                  │
│           ▼                          ▼                  │
│  ┌────────────────┐         ┌──────────────────┐        │
│  │    Resolver    │────────▶│ Unity Catalog    │       │
│  │    Engine      │         │     Client       │        │
│  └────────┬───────┘         └──────────────────┘        │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────────────────────────────┐            │
│  │         DataFusion Query Engine          │           │
│  └─────────────────┬───────────────────────┘            │
│                    │                                    │
│                    ▼                                    │
│  ┌─────────────────────────────────────────┐            │
│  │          Delta-RS (delta-io)             │           │
│  └─────────────────┬───────────────────────┘            │
└────────────────────┼────────────────────────────────────┘
                     │
                     ▼
           ┌──────────────────┐
           │  Delta Tables    │
           │  (S3/Azure/GCS)  │
           └──────────────────┘
```

### 2.2 Technology Stack

**Core Dependencies:**
```toml
[dependencies]
# GraphQL Server
async-graphql = "7.0"
async-graphql-axum = "7.0"

# Web Framework
axum = "0.7"
tower = "0.4"
tower-http = "0.5"

# Delta Lake & Query Engine
deltalake = { version = "0.19", features = ["datafusion", "s3", "azure", "gcs"] }
datafusion = "42.0"

# Arrow (comes with datafusion/deltalake)
arrow = "53.0"
arrow-schema = "53.0"

# HTTP Client (Unity Catalog API)
reqwest = { version = "0.12", features = ["json"] }

# Async Runtime
tokio = { version = "1.40", features = ["full"] }

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Configuration
config = "0.14"
toml = "0.8"

# CLI
clap = { version = "4.5", features = ["derive"] }

# Error Handling
anyhow = "1.0"
thiserror = "1.0"

# Logging
tracing = "0.1"
tracing-subscriber = "0.3"

# Testing
[dev-dependencies]
tokio-test = "0.4"
```

---

## 3. Core Features (MVP Scope)

### 3.1 CLI: Initialize Configuration

**Command:**
```bash
nouninator init \
  --host <databricks-workspace-url> \
  --catalog <catalog-name> \
  --schema <schema-name> \
  --output nouninator.toml
```

**Behavior:**
1. Connect to Unity Catalog REST API using token from `DATABRICKS_TOKEN` env var
2. Discover all tables in specified `catalog.schema`
3. For each table, fetch:
   - Column names and data types
   - Table properties/comments
   - Storage location
   - Primary key (from table properties or infer from `id` column)
4. Generate `nouninator.toml` configuration file

**Exit Conditions:**
- Success: Config file written, summary printed
- Error: Authentication failure, network error, invalid catalog/schema

---

### 3.2 CLI: Start GraphQL Server

**Command:**
```bash
nouninator serve [--config nouninator.toml] [--port 4000]
```

**Behavior:**
1. Load configuration from `nouninator.toml`
2. For each table in config:
   - Open Delta table using `deltalake` crate
   - Register with DataFusion `SessionContext`
   - Generate GraphQL type from Arrow schema
   - Create query resolvers
3. Build dynamic GraphQL schema using `async-graphql::dynamic`
4. Start Axum HTTP server with:
   - POST `/graphql` - GraphQL query endpoint
   - GET `/graphql` - GraphQL Playground UI
   - GET `/health` - Health check endpoint

**Exit Conditions:**
- Success: Server running, log startup message
- Error: Port in use, invalid config, table access denied

---

### 3.3 GraphQL Schema Generation

**Input:** Delta Table with Arrow Schema
```rust
Schema {
    fields: [
        Field { name: "customer_id", data_type: Int64, nullable: false },
        Field { name: "email", data_type: Utf8, nullable: false },
        Field { name: "first_name", data_type: Utf8, nullable: true },
        Field { name: "created_at", data_type: Timestamp, nullable: false },
        Field { name: "lifetime_value", data_type: Float64, nullable: true },
    ]
}
```

**Output:** GraphQL Type
```graphql
type Customer {
  customer_id: ID!
  email: String!
  first_name: String
  created_at: DateTime!
  lifetime_value: Float
}

type Query {
  customer(customer_id: ID!): Customer
  customers(limit: Int = 100, offset: Int = 0): [Customer!]!
}
```

**Type Mapping Rules:**
| Arrow Type | GraphQL Type | Notes |
|------------|--------------|-------|
| `Int8, Int16, Int32, Int64` | `Int` | If column name ends with `_id`, use `ID` |
| `UInt8, UInt16, UInt32, UInt64` | `Int` | Same ID inference |
| `Float32, Float64` | `Float` | |
| `Utf8, LargeUtf8` | `String` | |
| `Boolean` | `Boolean` | |
| `Date32, Date64` | `Date` | Custom scalar |
| `Timestamp` | `DateTime` | Custom scalar |
| `List(inner)` | `[T]` | Recursive mapping |
| `Struct(fields)` | Object type | Recursive mapping |
| Other | Skip | Log warning |

**Nullability:** Arrow `nullable: true` → GraphQL optional (no `!`)

---

### 3.4 Query Resolvers

#### 3.4.1 Get by Primary Key

**GraphQL:**
```graphql
query {
  customer(customer_id: "C12345") {
    email
    first_name
  }
}
```

**Resolution Strategy:**
1. Extract primary key value from arguments
2. Generate SQL: `SELECT * FROM <table> WHERE <pk_column> = <value>`
3. Execute via DataFusion `SessionContext`
4. Convert `RecordBatch` to GraphQL `Value`
5. Return single result or `null`

**Performance Optimization:**
- Use Delta table statistics for file pruning
- DataFusion leverages Delta min/max stats automatically

#### 3.4.2 List Query with Pagination

**GraphQL:**
```graphql
query {
  customers(limit: 50, offset: 100) {
    customer_id
    email
  }
}
```

**Resolution Strategy:**
1. Extract `limit` (default 100, max 1000) and `offset` (default 0)
2. Generate SQL: `SELECT * FROM <table> LIMIT <limit> OFFSET <offset>`
3. Execute via DataFusion
4. Convert `Vec<RecordBatch>` to GraphQL array
5. Return results

---

### 3.5 Configuration File Format

**File:** `nouninator.toml`

```toml
# Databricks connection
[databricks]
host = "https://dbc-xxx-yyy.cloud.databricks.com"
# Token read from DATABRICKS_TOKEN environment variable

# Server configuration
[server]
port = 4000
# Optional: restrict which interfaces to bind
bind = "0.0.0.0"

# Entity definitions (auto-generated by `init`)
[[entity]]
# Unity Catalog table path
table = "main.sales.customers"
# GraphQL type name (defaults to PascalCase of table name)
graphql_name = "Customer"
# Primary key column (required for get_<entity> query)
primary_key = "customer_id"
# Optional: table description for GraphQL schema
description = "Deduplicated customer master data"

[[entity]]
table = "main.sales.orders"
graphql_name = "Order"
primary_key = "order_id"
description = "Customer orders from all channels"

[[entity]]
table = "main.sales.order_items"
graphql_name = "OrderItem"
primary_key = "item_id"
```

**Validation Rules:**
- `databricks.host` must be valid URL
- `entity.table` must be in format `catalog.schema.table`
- `entity.primary_key` must exist in table schema
- `entity.graphql_name` must be valid GraphQL type name (PascalCase, alphanumeric)

---

## 4. Detailed Module Structure

### 4.1 Project Layout

```
nouninator/
├── Cargo.toml
├── README.md
├── LICENSE (MIT or Apache-2.0)
├── src/
│   ├── main.rs                 # CLI entry point
│   ├── lib.rs                  # Library exports
│   │
│   ├── cli/
│   │   ├── mod.rs              # CLI command routing
│   │   ├── init.rs             # `init` command implementation
│   │   └── serve.rs            # `serve` command implementation
│   │
│   ├── config/
│   │   ├── mod.rs              # Config loading/validation
│   │   └── types.rs            # Config struct definitions
│   │
│   ├── unity/
│   │   ├── mod.rs              # Unity Catalog client
│   │   ├── client.rs           # HTTP client wrapper
│   │   ├── types.rs            # API response types
│   │   └── discovery.rs        # Table discovery logic
│   │
│   ├── schema/
│   │   ├── mod.rs              # Schema generation
│   │   ├── builder.rs          # GraphQL schema builder
│   │   ├── type_mapping.rs     # Arrow → GraphQL type conversion
│   │   └── scalars.rs          # Custom scalar types (Date, DateTime)
│   │
│   ├── resolver/
│   │   ├── mod.rs              # Resolver logic
│   │   ├── query.rs            # Query field resolvers
│   │   └── conversion.rs       # RecordBatch → GraphQL Value
│   │
│   ├── server/
│   │   ├── mod.rs              # HTTP server setup
│   │   ├── handlers.rs         # GraphQL endpoint handlers
│   │   └── middleware.rs       # Logging, CORS, etc.
│   │
│   └── error.rs                # Error types
│
├── tests/
│   ├── integration_test.rs     # End-to-end tests
│   └── fixtures/
│       └── sample.toml         # Test configuration
│
└── examples/
    └── quickstart.rs           # Example usage
```

---

## 5. Implementation Specifications

### 5.1 Unity Catalog Client

**File:** `src/unity/client.rs`

**Purpose:** Interact with Databricks Unity Catalog REST API

**Key Methods:**

```rust
pub struct UnityClient {
    base_url: String,
    token: String,
    client: reqwest::Client,
}

impl UnityClient {
    pub fn new(host: String, token: String) -> Self;
    
    /// List tables in a schema
    /// GET /api/2.1/unity-catalog/tables
    /// Query params: catalog_name, schema_name
    pub async fn list_tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<TableInfo>>;
    
    /// Get table metadata
    /// GET /api/2.1/unity-catalog/tables/{full_name}
    /// where full_name = catalog.schema.table
    pub async fn get_table(
        &self,
        full_name: &str,
    ) -> Result<TableMetadata>;
}
```

**Types:**

```rust
#[derive(Debug, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String, // MANAGED, EXTERNAL
    pub data_source_format: String, // DELTA
    pub storage_location: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub columns: Vec<ColumnInfo>,
    pub storage_location: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String, // "bigint", "string", "timestamp", etc.
    pub type_text: String, // Full type definition
    pub position: i32,
    pub nullable: bool,
    pub comment: Option<String>,
}
```

**Error Handling:**
- Network errors → `UnityError::Network`
- 401/403 → `UnityError::Unauthorized`
- 404 → `UnityError::NotFound`
- Other 4xx/5xx → `UnityError::Api`

---

### 5.2 Schema Builder

**File:** `src/schema/builder.rs`

**Purpose:** Generate dynamic GraphQL schema from Delta table metadata

**Key Method:**

```rust
pub struct SchemaBuilder {
    datafusion_ctx: SessionContext,
}

impl SchemaBuilder {
    pub fn new() -> Self;
    
    /// Build complete GraphQL schema from entities
    pub async fn build_schema(
        &mut self,
        entities: Vec<EntityConfig>,
        databricks_config: &DatabricksConfig,
    ) -> Result<async_graphql::dynamic::Schema>;
    
    /// Convert single entity to GraphQL Object type
    fn build_entity_type(
        &self,
        entity: &EntityConfig,
        arrow_schema: &ArrowSchema,
    ) -> Result<async_graphql::dynamic::Object>;
    
    /// Create Query type with get_X and list_X resolvers
    fn build_query_type(
        &self,
        entities: &[EntityConfig],
    ) -> Result<async_graphql::dynamic::Object>;
}
```

**Key Logic:**

```rust
// Pseudo-code for schema building
fn build_schema(entities) -> Schema {
    let mut query = Object::new("Query");
    
    for entity in entities {
        // 1. Open Delta table
        let table = open_delta_table(entity.table_path).await?;
        
        // 2. Register with DataFusion
        datafusion_ctx.register_table(&entity.graphql_name, table)?;
        
        // 3. Create GraphQL type from Arrow schema
        let object_type = arrow_schema_to_graphql_object(
            &entity.graphql_name,
            table.schema()?
        )?;
        
        // 4. Add get_X(id) resolver
        let get_field = Field::new(
            format!("get_{}", snake_case(&entity.graphql_name)),
            TypeRef::named(&entity.graphql_name),
            move |ctx| {
                // Resolver implementation (see 5.3)
            }
        )
        .argument(InputValue::new(
            &entity.primary_key,
            TypeRef::named_nn(TypeRef::ID)
        ));
        
        query = query.field(get_field);
        
        // 5. Add list_X(limit, offset) resolver
        let list_field = Field::new(
            format!("list_{}", snake_case(&entity.graphql_name)),
            TypeRef::named_list_nn(&entity.graphql_name),
            move |ctx| {
                // Resolver implementation (see 5.3)
            }
        )
        .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)));
        
        query = query.field(list_field);
    }
    
    Schema::build(query, None, None).finish()
}
```

---

### 5.3 Type Mapping

**File:** `src/schema/type_mapping.rs`

**Purpose:** Convert Arrow data types to GraphQL types

**Key Function:**

```rust
/// Map Arrow DataType to GraphQL TypeRef
pub fn arrow_to_graphql_type(
    field_name: &str,
    data_type: &ArrowDataType,
    nullable: bool,
) -> Option<TypeRef> {
    let base_type = match data_type {
        ArrowDataType::Int8 | ArrowDataType::Int16 
        | ArrowDataType::Int32 | ArrowDataType::Int64 => {
            // If field name ends with _id, use ID type
            if field_name.ends_with("_id") {
                TypeRef::ID
            } else {
                TypeRef::INT
            }
        }
        ArrowDataType::UInt8 | ArrowDataType::UInt16
        | ArrowDataType::UInt32 | ArrowDataType::UInt64 => {
            if field_name.ends_with("_id") {
                TypeRef::ID
            } else {
                TypeRef::INT
            }
        }
        ArrowDataType::Float32 | ArrowDataType::Float64 => TypeRef::FLOAT,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TypeRef::STRING,
        ArrowDataType::Boolean => TypeRef::BOOLEAN,
        ArrowDataType::Date32 | ArrowDataType::Date64 => {
            TypeRef::named("Date") // Custom scalar
        }
        ArrowDataType::Timestamp(_, _) => {
            TypeRef::named("DateTime") // Custom scalar
        }
        ArrowDataType::List(inner) | ArrowDataType::LargeList(inner) => {
            let inner_type = arrow_to_graphql_type(
                "", 
                inner.data_type(), 
                inner.is_nullable()
            )?;
            TypeRef::named_list(inner_type)
        }
        ArrowDataType::Struct(fields) => {
            // For MVP: skip structs (future: create nested objects)
            return None;
        }
        _ => {
            // Unsupported type, skip field
            return None;
        }
    };
    
    Some(if nullable {
        base_type
    } else {
        base_type.non_null()
    })
}
```

**Custom Scalars:**

```rust
// src/schema/scalars.rs

/// ISO 8601 date (YYYY-MM-DD)
pub fn date_scalar() -> Scalar {
    Scalar::new("Date")
        .description("ISO 8601 date format (YYYY-MM-DD)")
        .parse_value(|value| {
            // Parse string to NaiveDate
        })
        .to_value(|date: NaiveDate| {
            // Serialize to string
        })
}

/// ISO 8601 datetime with timezone
pub fn datetime_scalar() -> Scalar {
    Scalar::new("DateTime")
        .description("ISO 8601 datetime format")
        .parse_value(|value| {
            // Parse string to DateTime<Utc>
        })
        .to_value(|datetime: DateTime<Utc>| {
            // Serialize to string
        })
}
```

---

### 5.4 Query Resolvers

**File:** `src/resolver/query.rs`

**Purpose:** Implement GraphQL query resolution using DataFusion

**Get by Primary Key Resolver:**

```rust
pub fn create_get_resolver(
    table_name: String,
    primary_key: String,
) -> impl Fn(ResolverContext) -> FieldFuture<Value> {
    move |ctx| {
        let table_name = table_name.clone();
        let primary_key = primary_key.clone();
        
        FieldFuture::new(async move {
            // 1. Extract primary key value from args
            let pk_value: String = ctx.args.get(&primary_key)
                .ok_or("Primary key argument missing")?;
            
            // 2. Get DataFusion context from GraphQL context
            let datafusion_ctx = ctx.data::<SessionContext>()?;
            
            // 3. Build SQL query
            let sql = format!(
                "SELECT * FROM {} WHERE {} = '{}'",
                table_name, primary_key, pk_value
            );
            
            // 4. Execute query
            let df = datafusion_ctx.sql(&sql).await
                .map_err(|e| format!("Query execution failed: {}", e))?;
            
            let batches = df.collect().await
                .map_err(|e| format!("Data collection failed: {}", e))?;
            
            // 5. Convert first row to GraphQL Value
            if batches.is_empty() || batches[0].num_rows() == 0 {
                return Ok(None);
            }
            
            let record_batch = &batches[0];
            let row_value = record_batch_to_graphql_value(record_batch, 0)?;
            
            Ok(Some(row_value))
        })
    }
}
```

**List Resolver:**

```rust
pub fn create_list_resolver(
    table_name: String,
) -> impl Fn(ResolverContext) -> FieldFuture<Value> {
    move |ctx| {
        let table_name = table_name.clone();
        
        FieldFuture::new(async move {
            // 1. Extract pagination args
            let limit: i32 = ctx.args.get("limit").unwrap_or(100);
            let offset: i32 = ctx.args.get("offset").unwrap_or(0);
            
            // Enforce max limit
            let limit = limit.min(1000);
            
            // 2. Get DataFusion context
            let datafusion_ctx = ctx.data::<SessionContext>()?;
            
            // 3. Build SQL query
            let sql = format!(
                "SELECT * FROM {} LIMIT {} OFFSET {}",
                table_name, limit, offset
            );
            
            // 4. Execute query
            let df = datafusion_ctx.sql(&sql).await
                .map_err(|e| format!("Query execution failed: {}", e))?;
            
            let batches = df.collect().await
                .map_err(|e| format!("Data collection failed: {}", e))?;
            
            // 5. Convert all rows to GraphQL array
            let mut results = Vec::new();
            for batch in batches {
                for row_idx in 0..batch.num_rows() {
                    let row_value = record_batch_to_graphql_value(&batch, row_idx)?;
                    results.push(row_value);
                }
            }
            
            Ok(Some(Value::List(results)))
        })
    }
}
```

---

### 5.5 RecordBatch Conversion

**File:** `src/resolver/conversion.rs`

**Purpose:** Convert Arrow RecordBatch rows to GraphQL Values

**Key Function:**

```rust
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use async_graphql::Value;

/// Convert a single row from RecordBatch to GraphQL Value (Object)
pub fn record_batch_to_graphql_value(
    batch: &RecordBatch,
    row_idx: usize,
) -> Result<Value> {
    let schema = batch.schema();
    let mut object_map = IndexMap::new();
    
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let column = batch.column(col_idx);
        
        // Check if value is null
        if column.is_null(row_idx) {
            object_map.insert(Name::new(field.name()), Value::Null);
            continue;
        }
        
        // Convert based on data type
        let value = match column.data_type() {
            ArrowDataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                Value::Number((array.value(row_idx) as i32).into())
            }
            ArrowDataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                Value::Number((array.value(row_idx) as i32).into())
            }
            ArrowDataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                Value::Number(array.value(row_idx).into())
            }
            ArrowDataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                // For ID fields, convert to string
                if field.name().ends_with("_id") {
                    Value::String(array.value(row_idx).to_string())
                } else {
                    Value::Number(array.value(row_idx).into())
                }
            }
            ArrowDataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                Value::Number(Number::from_f64(array.value(row_idx) as f64).unwrap())
            }
            ArrowDataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Value::Number(Number::from_f64(array.value(row_idx)).unwrap())
            }
            ArrowDataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            ArrowDataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Value::Boolean(array.value(row_idx))
            }
            ArrowDataType::Timestamp(unit, tz) => {
                let array = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                let timestamp = array.value(row_idx);
                // Convert to ISO 8601 string
                let datetime = NaiveDateTime::from_timestamp_opt(
                    timestamp / 1_000_000_000,
                    (timestamp % 1_000_000_000) as u32
                ).unwrap();
                Value::String(datetime.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string())
            }
            // Add more types as needed
            _ => Value::Null, // Unsupported types
        };
        
        object_map.insert(Name::new(field.name()), value);
    }
    
    Ok(Value::Object(object_map))
}
```

---

### 5.6 Server Setup

**File:** `src/server/mod.rs`

**Purpose:** Configure and run Axum HTTP server

**Key Function:**

```rust
use axum::{Router, Extension};
use axum::routing::{get, post};
use tower_http::cors::CorsLayer;
use std::net::SocketAddr;

pub async fn run_server(
    schema: async_graphql::dynamic::Schema,
    port: u16,
) -> Result<()> {
    let app = Router::new()
        .route("/graphql", post(graphql_handler).get(graphql_playground))
        .route("/health", get(health_check))
        .layer(Extension(schema))
        .layer(CorsLayer::permissive()); // Allow all origins for dev
    
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    
    tracing::info!("🚀 GraphQL server running on http://localhost:{}", port);
    tracing::info!("📊 Playground: http://localhost:{}/graphql", port);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}
```

**Handlers:**

```rust
// src/server/handlers.rs

use axum::{Extension, Json, response::Html};
use async_graphql::http::{GraphQLPlaygroundConfig, playground_source};
use async_graphql::dynamic::Schema;

/// POST /graphql - Execute GraphQL query
pub async fn graphql_handler(
    schema: Extension<Schema>,
    req: Json<async_graphql::Request>,
) -> Json<async_graphql::Response> {
    let response = schema.execute(req.0).await;
    Json(response)
}

/// GET /graphql - Serve GraphQL Playground
pub async fn graphql_playground() -> Html<String> {
    Html(playground_source(
        GraphQLPlaygroundConfig::new("/graphql")
    ))
}

/// GET /health - Health check
pub async fn health_check() -> &'static str {
    "OK"
}
```

---

## 6. CLI Implementation

### 6.1 Main Entry Point

**File:** `src/main.rs`

```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "nouninator")]
#[command(about = "Turn Delta tables into GraphQL APIs", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize configuration from Unity Catalog
    Init {
        /// Databricks workspace URL
        #[arg(long)]
        host: String,
        
        /// Unity Catalog name
        #[arg(long)]
        catalog: String,
        
        /// Schema name
        #[arg(long)]
        schema: String,
        
        /// Output config file path
        #[arg(long, default_value = "nouninator.toml")]
        output: String,
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
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Init { host, catalog, schema, output } => {
            cli::init::run(host, catalog, schema, output).await?;
        }
        Commands::Serve { config, port } => {
            cli::serve::run(config, port).await?;
        }
    }
    
    Ok(())
}
```

### 6.2 Init Command

**File:** `src/cli/init.rs`

```rust
use crate::unity::UnityClient;
use crate::config::{Config, EntityConfig, DatabricksConfig, ServerConfig};
use std::fs;

pub async fn run(
    host: String,
    catalog: String,
    schema: String,
    output: String,
) -> anyhow::Result<()> {
    tracing::info!("🔍 Discovering entities in {}.{}...", catalog, schema);
    
    // 1. Get token from environment
    let token = std::env::var("DATABRICKS_TOKEN")
        .context("DATABRICKS_TOKEN environment variable not set")?;
    
    // 2. Create Unity Catalog client
    let client = UnityClient::new(host.clone(), token);
    
    // 3. List tables
    let tables = client.list_tables(&catalog, &schema).await?;
    
    if tables.is_empty() {
        tracing::warn!("No tables found in {}.{}", catalog, schema);
        return Ok(());
    }
    
    tracing::info!("✅ Found {} tables", tables.len());
    
    // 4. Convert to entity configs
    let mut entities = Vec::new();
    for table in tables {
        // Skip non-Delta tables
        if table.data_source_format != "DELTA" {
            continue;
        }
        
        let full_name = format!("{}.{}.{}", 
            table.catalog_name, 
            table.schema_name, 
            table.name
        );
        
        // Fetch detailed metadata
        let metadata = client.get_table(&full_name).await?;
        
        // Infer primary key
        let primary_key = metadata.properties
            .get("primary_key")
            .cloned()
            .or_else(|| {
                // Look for column named "id" or ending with "_id"
                metadata.columns.iter()
                    .find(|c| c.name == "id" || c.name.ends_with("_id"))
                    .map(|c| c.name.clone())
            })
            .unwrap_or_else(|| {
                tracing::warn!("No primary key found for {}, using first column", full_name);
                metadata.columns[0].name.clone()
            });
        
        entities.push(EntityConfig {
            table: full_name.clone(),
            graphql_name: to_pascal_case(&table.name),
            primary_key,
            description: table.comment,
        });
        
        tracing::info!("   • {} ({} rows)", 
            full_name,
            metadata.properties.get("row_count").unwrap_or(&"unknown".to_string())
        );
    }
    
    // 5. Build config
    let config = Config {
        databricks: DatabricksConfig {
            host,
        },
        server: ServerConfig {
            port: 4000,
            bind: "0.0.0.0".to_string(),
        },
        entity: entities,
    };
    
    // 6. Write to file
    let toml_string = toml::to_string_pretty(&config)?;
    fs::write(&output, toml_string)?;
    
    tracing::info!("📝 Generated {}", output);
    tracing::info!("🚀 Ready to serve! Run: nouninator serve");
    
    Ok(())
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}
```

### 6.3 Serve Command

**File:** `src/cli/serve.rs`

```rust
use crate::config::Config;
use crate::schema::SchemaBuilder;
use crate::server;
use std::fs;

pub async fn run(config_path: String, port: u16) -> anyhow::Result<()> {
    tracing::info!("📖 Loading configuration from {}", config_path);
    
    // 1. Load config
    let config_str = fs::read_to_string(&config_path)
        .context("Failed to read config file")?;
    let config: Config = toml::from_str(&config_str)
        .context("Failed to parse config file")?;
    
    // 2. Get Databricks token
    let token = std::env::var("DATABRICKS_TOKEN")
        .context("DATABRICKS_TOKEN environment variable not set")?;
    
    tracing::info!("🔧 Building GraphQL schema for {} entities...", 
        config.entity.len()
    );
    
    // 3. Build schema
    let mut builder = SchemaBuilder::new();
    let schema = builder.build_schema(
        config.entity,
        &config.databricks,
        &token,
    ).await?;
    
    // 4. Start server
    let server_port = config.server.port;
    server::run_server(schema, server_port).await?;
    
    Ok(())
}
```

---

## 7. Error Handling

**File:** `src/error.rs`

```rust
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
}

pub type Result<T> = std::result::Result<T, NouninatorError>;
```

---

## 8. Testing Strategy

### 8.1 Unit Tests

Each module should have unit tests:

```rust
// src/schema/type_mapping.rs

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_int64_to_graphql() {
        let type_ref = arrow_to_graphql_type(
            "count",
            &ArrowDataType::Int64,
            false
        );
        assert!(matches!(type_ref, Some(TypeRef::INT)));
    }
    
    #[test]
    fn test_id_field_inference() {
        let type_ref = arrow_to_graphql_type(
            "customer_id",
            &ArrowDataType::Int64,
            false
        );
        assert!(matches!(type_ref, Some(TypeRef::ID)));
    }
}
```

### 8.2 Integration Tests

**File:** `tests/integration_test.rs`

Test end-to-end flow with local Delta table:

```rust
use nouninator::config::*;
use nouninator::schema::SchemaBuilder;

#[tokio::test]
async fn test_schema_generation() {
    // Create test Delta table
    let table_path = create_test_delta_table().await;
    
    // Build schema
    let entity = EntityConfig {
        table: table_path,
        graphql_name: "TestEntity".to_string(),
        primary_key: "id".to_string(),
        description: None,
    };
    
    let mut builder = SchemaBuilder::new();
    let schema = builder.build_schema(
        vec![entity],
        &test_databricks_config(),
        "test_token",
    ).await.unwrap();
    
    // Verify schema has expected queries
    // Test query execution
}
```

---

## 9. Documentation Requirements

### 9.1 README.md

Must include:
- **Quick Start**: 3-step getting started
- **Installation**: `cargo install nouninator`
- **Configuration**: Example `nouninator.toml`
- **Usage**: `init` and `serve` commands
- **Authentication**: Setting `DATABRICKS_TOKEN`
- **Examples**: Sample GraphQL queries
- **Limitations**: What's not supported in OSS
- **Contributing**: Link to issues
- **License**: MIT

### 9.2 In-code Documentation

All public APIs must have rustdoc comments:

```rust
/// Unity Catalog client for interacting with Databricks metadata APIs.
///
/// # Example
///
/// ```no_run
/// use nouninator::unity::UnityClient;
///
/// let client = UnityClient::new(
///     "https://workspace.databricks.com".to_string(),
///     "dapi_token_here".to_string()
/// );
///
/// let tables = client.list_tables("main", "sales").await?;
/// ```
pub struct UnityClient {
    // ...
}
```

---

## 10. Success Criteria

### 10.1 Functional Requirements

- [ ] `nouninator init` discovers Delta tables from Unity Catalog
- [ ] Generated config includes all table metadata
- [ ] `nouninator serve` starts GraphQL server
- [ ] GraphQL playground accessible at `/graphql`
- [ ] Primary key lookups work correctly
- [ ] List queries with pagination work
- [ ] All supported Arrow types convert to GraphQL
- [ ] Null handling works correctly
- [ ] Error messages are helpful

### 10.2 Non-Functional Requirements

- [ ] Server starts in under 5 seconds for 10 tables
- [ ] Primary key queries return in <100ms (with warm cache)
- [ ] Memory usage stays under 100MB for schema with 20 tables
- [ ] No panics on malformed queries
- [ ] All public APIs have documentation
- [ ] README has working quick-start example
- [ ] CI passes (cargo test, cargo clippy)

---

## 11. Known Limitations (MVP)

**Out of Scope for OSS v0.1:**
- ❌ Relationships/joins (no nested queries across tables)
- ❌ Filtering beyond primary key (no WHERE clauses)
- ❌ Sorting (no ORDER BY)
- ❌ Aggregations (no COUNT, SUM, etc.)
- ❌ Mutations (read-only)
- ❌ Subscriptions (no real-time updates)
- ❌ Authentication/authorization
- ❌ Rate limiting
- ❌ Query complexity analysis
- ❌ Struct/nested column support
- ❌ Schema caching (rebuilds on every start)

These are **features for Pro/Enterprise** or future OSS releases.

---

## 12. Development Phases

### Phase 1: Core Infrastructure (Week 1)
- [ ] Project setup with Cargo.toml
- [ ] Unity Catalog client implementation
- [ ] Config loading/validation
- [ ] Error types

### Phase 2: Schema Generation (Week 1-2)
- [ ] Arrow → GraphQL type mapping
- [ ] Dynamic schema builder
- [ ] Custom scalars (Date, DateTime)

### Phase 3: Query Resolution (Week 2)
- [ ] DataFusion integration
- [ ] Get by PK resolver
- [ ] List with pagination resolver
- [ ] RecordBatch → GraphQL Value conversion

### Phase 4: CLI & Server (Week 2)
- [ ] `init` command implementation
- [ ] `serve` command implementation
- [ ] Axum server setup
- [ ] GraphQL Playground integration

### Phase 5: Testing & Documentation (Week 3)
- [ ] Unit tests for all modules
- [ ] Integration tests
- [ ] README with examples
- [ ] API documentation

### Phase 6: Polish & Release (Week 3)
- [ ] CI/CD setup (GitHub Actions)
- [ ] Example project
- [ ] Release v0.1.0 to crates.io
- [ ] Announcement blog post

---

## 13. Future Considerations

**Post-MVP Enhancements:**
1. **Filtering DSL**: GraphQL input types for WHERE clauses
2. **Relations**: Auto-detect FKs and generate nested resolvers
3. **Schema caching**: Persist schema to avoid rebuild
4. **Live reload**: Watch config file for changes
5. **Metrics**: Prometheus endpoint for query stats
6. **Query batching**: DataLoader pattern for N+1 prevention

---

## Appendix A: Example Queries

**Get by Primary Key:**
```graphql
query {
  customer(customer_id: "C12345") {
    email
    first_name
    created_at
  }
}
```

**List with Pagination:**
```graphql
query {
  customers(limit: 50, offset: 100) {
    customer_id
    email
    lifetime_value
  }
}
```

---

## Appendix B: Example Config

```toml
[databricks]
host = "https://dbc-xxx-yyy.cloud.databricks.com"

[server]
port = 4000
bind = "0.0.0.0"

[[entity]]
table = "main.sales.customers"
graphql_name = "Customer"
primary_key = "customer_id"
description = "Deduplicated customer master data"

[[entity]]
table = "main.sales.orders"
graphql_name = "Order"
primary_key = "order_id"
```
