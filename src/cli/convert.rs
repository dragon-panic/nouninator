use nouninator::error::Result;
use deltalake::arrow::csv::ReaderBuilder;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Convert example CSV files to Delta tables
/// This is used internally by init --example
pub async fn convert_example_data(output_dir: String) -> Result<()> {
    tracing::info!("🔄 Converting example CSV files to Delta tables...");
    
    // Ensure output directory exists
    std::fs::create_dir_all(&output_dir)?;
    
    let csv_dir = "examples/data";
    let csv_files = vec![
        ("nouns.csv", get_nouns_schema()),
        ("verbs.csv", get_verbs_schema()),
        ("adjectives.csv", get_adjectives_schema()),
        ("sentences.csv", get_sentences_schema()),
        ("synonyms.csv", get_synonyms_schema()),
        ("word_frequency.csv", get_word_frequency_schema()),
    ];
    
    let csv_count = csv_files.len();
    let mut success_count = 0;
    
    for (csv_file, schema) in csv_files {
        let csv_path = format!("{}/{}", csv_dir, csv_file);
        let table_name = csv_file.replace(".csv", "");
        let delta_path = format!("{}/{}", output_dir, table_name);
        
        match convert_single_file(&csv_path, &delta_path, schema).await {
            Ok(row_count) => {
                tracing::info!("✅ Converted {} ({} rows) -> {}", csv_file, row_count, delta_path);
                success_count += 1;
            }
            Err(e) => {
                tracing::error!("❌ Failed to convert {}: {}", csv_file, e);
            }
        }
    }
    
    tracing::info!("");
    tracing::info!("🎉 Conversion complete! {} of {} tables converted", success_count, csv_count);
    tracing::info!("📁 Delta tables created in: {}", output_dir);
    
    Ok(())
}

async fn convert_single_file(
    csv_path: &str,
    delta_path: &str,
    schema: Arc<Schema>,
) -> Result<usize> {
    
    // Check if CSV file exists
    if !Path::new(csv_path).exists() {
        return Err(nouninator::error::NouninatorError::Config(
            format!("CSV file not found: {}", csv_path)
        ));
    }
    
    // Remove existing Delta table if it exists
    if Path::new(delta_path).exists() {
        std::fs::remove_dir_all(delta_path)?;
    }
    
    // Read CSV into Arrow RecordBatch
    let file = File::open(csv_path)?;
    let mut csv_reader = ReaderBuilder::new(Arc::clone(&schema))
        .with_header(true)
        .build(file)
        .map_err(|e| nouninator::error::NouninatorError::Config(format!("CSV read error: {}", e)))?;
    
    let mut batches = Vec::new();
    let mut total_rows = 0;
    
    while let Some(batch) = csv_reader.next() {
        let batch = batch.map_err(|e| {
            nouninator::error::NouninatorError::Config(
                format!("Failed to read CSV batch: {}", e)
            )
        })?;
        total_rows += batch.num_rows();
        batches.push(batch);
    }
    
    // Create Delta table
    let columns: Vec<deltalake::kernel::StructField> = schema
        .fields()
        .iter()
        .cloned()
        .map(|f| {
            let delta_type: deltalake::kernel::DataType = f.data_type().try_into()
                .expect(&format!("Failed to convert data type: {:?}", f.data_type()));
            deltalake::kernel::StructField::new(
                f.name().clone(),
                delta_type,
                f.is_nullable(),
            )
        })
        .collect();
    
    let mut table = CreateBuilder::new()
        .with_location(delta_path)
        .with_columns(columns)
        .await?;
    
    // Write batches to Delta table
    let mut writer = RecordBatchWriter::for_table(&table)?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.flush_and_commit(&mut table).await?;
    
    Ok(total_rows)
}

// Schema definitions for each table
fn get_nouns_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("noun_id", DataType::Int64, false),
        Field::new("word", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, false),
        Field::new("example_usage", DataType::Utf8, false),
        Field::new("frequency_rank", DataType::Int64, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn get_verbs_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("verb_id", DataType::Int64, false),
        Field::new("word", DataType::Utf8, false),
        Field::new("tense", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, false),
        Field::new("example_usage", DataType::Utf8, false),
        Field::new("transitivity", DataType::Utf8, false),
        Field::new("frequency_rank", DataType::Int64, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn get_adjectives_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("adjective_id", DataType::Int64, false),
        Field::new("word", DataType::Utf8, false),
        Field::new("degree", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, false),
        Field::new("example_usage", DataType::Utf8, false),
        Field::new("frequency_rank", DataType::Int64, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn get_sentences_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("sentence_id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("complexity", DataType::Utf8, false),
        Field::new("subject", DataType::Utf8, false),
        Field::new("predicate", DataType::Utf8, false),
        Field::new("word_count", DataType::Int64, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn get_synonyms_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("word1", DataType::Utf8, false),
        Field::new("word2", DataType::Utf8, false),
        Field::new("similarity_score", DataType::Float64, false),
        Field::new("context", DataType::Utf8, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn get_word_frequency_schema() -> Arc<Schema> {
    
    Arc::new(Schema::new(vec![
        Field::new("word_id", DataType::Int64, false),
        Field::new("word", DataType::Utf8, false),
        Field::new("part_of_speech", DataType::Utf8, false),
        Field::new("frequency_per_million", DataType::Int64, false),
        Field::new("corpus", DataType::Utf8, false),
        Field::new("rank", DataType::Int64, false),
        Field::new("last_updated", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}


