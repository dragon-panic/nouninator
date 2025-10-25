# Nouninator

**Turn Delta tables into GraphQL APIs**

Nouninator is a Rust CLI tool and server that automatically generates type-safe GraphQL APIs from Databricks Unity Catalog Delta tables.

## Quick Start

### Try It Now (No Databricks Needed!)

Generate an example configuration to see what Nouninator does:

```bash
# Build nouninator
cargo build --release

# Generate example config and Delta tables
./target/release/nouninator init --example --output nouninator.example.toml

# Start the server
./target/release/nouninator serve --config nouninator.example.toml
```

This creates:
- Local Delta tables from sample CSV data (6 tables with linguistic data)
- Sample configuration showcasing multiple interconnected entities
- Primary key inference
- GraphQL naming conventions
- Table descriptions

**📊 Sample data included!** Check `examples/data/` for CSV files with realistic linguistic data (nouns, verbs, adjectives, sentences, etc.). 

Goto http://localhost:4000/graphql to try out the playground.

Example Query:

```graphql
query {
  verb(verb_id:"3") {
    verb_id
    word
    tense
    type
    definition
    transitivity
  }
}
```

Responds with

```json
{
  "data": {
    "verb": {
      "verb_id": "3",
      "word": "be",
      "tense": "base",
      "type": "linking",
      "definition": "To exist or occur",
      "transitivity": "linking"
    }
  }
}
```

### Step 1: Build Nouninator

Choose the build option that matches your use case:

```bash
# Option A: Development/Local (includes GraphQL server, local tables) - 1-2 min, 15 MB, NO native deps!
cargo build --release

# Option B: Production - AWS Databricks - 3-5 min, 30 MB, requires CMake/NASM
cargo build --release --features s3

# Option C: Production - Azure Databricks - 3-5 min, 30 MB, requires CMake/NASM
cargo build --release --features azure

# Option D: Production - GCP Databricks - 3-5 min, 30 MB, requires CMake/NASM
cargo build --release --features gcs

# Option E: Multi-cloud support - 5-10 min, 55 MB, requires CMake/NASM
cargo build --release --features all-clouds
```

💡 **Tip:** Start with Option A for development, then use Option B/C/D for production.

#### Native Build Tools (Optional - Only for Cloud Features)

**NOT needed for:**
- ✅ Base build (local development with Delta tables)

**Only needed when building with cloud storage features** (`s3`, `azure`, `gcs`):

**Windows:**
- [CMake](https://cmake.org/download/)
- [NASM](https://www.nasm.us/)
- Visual Studio Build Tools with C++

**Ubuntu/Debian:**
```bash
sudo apt-get install cmake nasm build-essential
```

**macOS:**
```bash
brew install cmake nasm
```

### Step 2: Generate Configuration

**Option A: Try the example first (no Databricks needed)**
```bash
./target/release/nouninator init --example --output nouninator.example.toml
```

**Option B: Connect to your Unity Catalog**
```bash
export DATABRICKS_TOKEN="dapi..."
./target/release/nouninator init \
  --host https://your-workspace.cloud.databricks.com \
  --catalog main \
  --schema sales \
  --output nouninator.toml
```

### Step 3: Start Server

```bash
# For example data (no Databricks needed)
./target/release/nouninator serve --config nouninator.example.toml

# For your Unity Catalog tables
./target/release/nouninator serve --config nouninator.toml
```

Access the GraphQL Playground at `http://localhost:4000/graphql`


## CLI Commands

```
Turn Delta tables into GraphQL APIs

Usage: nouninator <COMMAND>

Commands:
  init     Initialize configuration from Unity Catalog or generate example
  serve    Start GraphQL server
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Command Details

**`nouninator init`**
- **With Unity Catalog:** Requires `DATABRICKS_TOKEN` environment variable, discovers Delta tables and generates production-ready `nouninator.toml`
- **Example mode (`--example`):** No prerequisites needed, converts CSV data to local Delta tables and generates `nouninator.example.toml` with grammar-themed tables
- Perfect for understanding the tool before connecting to Unity Catalog

**`nouninator serve`**
- Starts GraphQL server on port 4000 (configurable)
- Serves GraphQL Playground UI
- Works with local Delta tables or cloud storage (with cloud features)

## Configuration Format

### Example Configuration (Grammar Theme)

Run `nouninator example` to generate this:

```toml
[databricks]
host = "https://example.databricks.com"

[server]
port = 4000
bind = "0.0.0.0"

[[entity]]
table = "language.parts_of_speech.nouns"
graphql_name = "Noun"
primary_key = "noun_id"
description = "A person, place, thing, or idea. Core entities in language."

[[entity]]
table = "language.parts_of_speech.verbs"
graphql_name = "Verb"
primary_key = "verb_id"
description = "Action words that express what someone or something does."

[[entity]]
table = "language.parts_of_speech.adjectives"
graphql_name = "Adjective"
primary_key = "adjective_id"
description = "Descriptive words that modify nouns and pronouns."

# ... 9 more entities (12 total)
```

### Real Unity Catalog Configuration

Run `nouninator init` to generate from your Databricks:

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

## Development

### Running Tests

```bash
# Run all tests (includes all features)
cargo test

# Run specific module tests
cargo test --lib config
cargo test --lib unity
cargo test --lib schema
```

### Code Quality

```bash
# Check for linting errors
cargo clippy

# Format code
cargo fmt
```

## Build Options

Nouninator provides **granular build options** so you only include what you need:

| Build Command | What You Get | Native Deps | Build Time | Binary Size | Use Case |
|---------------|--------------|-------------|------------|-------------|----------|
| `cargo build` | GraphQL + local tables | ❌ None | ~1-2 min | ~15 MB | **Development** ⭐ |
| `cargo build --features s3` | + AWS S3 storage | ✅ CMake/NASM | ~3-5 min | ~30 MB | AWS Databricks |
| `cargo build --features azure` | + Azure Blob storage | ✅ CMake/NASM | ~3-5 min | ~30 MB | Azure Databricks |
| `cargo build --features gcs` | + Google Cloud Storage | ✅ CMake/NASM | ~3-5 min | ~30 MB | GCP Databricks |
| `cargo build --features all-clouds` | + All cloud providers | ✅ CMake/NASM | ~5-10 min | ~55 MB | Multi-cloud |

### Choosing Your Build

```bash
# 🎯 For local development (RECOMMENDED)
cargo build
# ✅ No CMake/NASM needed
# ✅ Fast builds
# ✅ Includes GraphQL server and local Delta table support

# 🚀 For AWS production
cargo build --release --features s3

# 🚀 For Azure production
cargo build --release --features azure

# 🚀 For GCP production
cargo build --release --features gcs

# 🌐 For multi-cloud (custom combo)
cargo build --release --features s3,azure    # AWS + Azure
cargo build --release --features s3,gcs      # AWS + GCP
cargo build --release --features azure,gcs   # Azure + GCP

# 🌐 For multi-cloud (all providers)
cargo build --release --features all-clouds
```

## Authentication

Nouninator reads the Databricks token from the `DATABRICKS_TOKEN` environment variable:

```bash
export DATABRICKS_TOKEN="dapi..."
```

## License

MIT

## Contributing

Contributions welcome! Please see the PRD for detailed specifications.

