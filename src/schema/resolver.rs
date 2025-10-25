/// GraphQL resolvers for query operations
///
/// This module provides resolver functions for GraphQL queries, including:
/// - Get by primary key resolvers
/// - List with pagination resolvers
/// - Data conversion from Arrow RecordBatch to GraphQL Value

use crate::config::EntityConfig;
use crate::error::{NouninatorError, Result};
use crate::schema::type_mapping::to_snake_case;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType as ArrowDataType};
use datafusion::arrow::record_batch::RecordBatch;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, ResolverContext, TypeRef};
use async_graphql::{Name, Value};
use datafusion::prelude::SessionContext;
use indexmap::IndexMap;
use std::sync::Arc;

/// Create get_X(id) resolver for an entity
///
/// This creates a resolver that fetches a single entity by its primary key.
///
/// # Arguments
///
/// * `entity` - Entity configuration
///
/// # Returns
///
/// A GraphQL Field with the resolver function
pub fn create_get_resolver(entity: &EntityConfig) -> Field {
    let table_name = entity.table.clone();
    let primary_key = entity.primary_key.clone();
    let primary_key_arg = entity.primary_key.clone(); // Clone for the argument
    let graphql_name = entity.graphql_name.clone();
    let field_name = format!("{}", to_snake_case(&graphql_name));

    Field::new(
        field_name,
        TypeRef::named(&graphql_name),
        move |ctx: ResolverContext| {
            let table_name = table_name.clone();
            let primary_key = primary_key.clone();

            FieldFuture::new(async move {
                // Extract primary key value from arguments
                let pk_arg = ctx
                    .args
                    .try_get(&primary_key)
                    .map_err(|_| format!("Primary key '{}' argument missing", primary_key))?;
                
                let pk_value: String = match pk_arg.string() {
                    Ok(s) => s.to_string(),
                    Err(_) => return Err("Primary key must be a string".into()),
                };

                // Get DataFusion context from schema data
                let datafusion_ctx = ctx
                    .data::<Arc<SessionContext>>()
                    .map_err(|_e| "Failed to get DataFusion context")?;

                // Build SQL query
                let sql = format!(
                    "SELECT * FROM \"{}\" WHERE \"{}\" = '{}'",
                    table_name, primary_key, pk_value
                );

                tracing::debug!("Executing query: {}", sql);

                // Execute query
                let df = datafusion_ctx
                    .sql(&sql)
                    .await
                    .map_err(|e| format!("Query execution failed: {}", e))?;

                let batches = df
                    .collect()
                    .await
                    .map_err(|e| format!("Data collection failed: {}", e))?;

                // Convert first row to GraphQL Value
                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(None);
                }

                let record_batch = &batches[0];
                let row_value = record_batch_to_graphql_value(record_batch, 0)
                    .map_err(|e| format!("Failed to convert row: {}", e))?;

                // Return as owned_any so async-graphql can handle field extraction
                Ok(Some(FieldValue::owned_any(row_value)))
            })
        },
    )
    .argument(async_graphql::dynamic::InputValue::new(
        primary_key_arg,
        TypeRef::named_nn(TypeRef::ID),
    ))
}

/// Create list_X(limit, offset) resolver for an entity
///
/// This creates a resolver that fetches a paginated list of entities.
///
/// # Arguments
///
/// * `entity` - Entity configuration
///
/// # Returns
///
/// A GraphQL Field with the resolver function
pub fn create_list_resolver(entity: &EntityConfig) -> Field {
    let table_name = entity.table.clone();
    let graphql_name = entity.graphql_name.clone();
    let field_name = format!("list_{}", to_snake_case(&graphql_name));

    Field::new(
        field_name,
        TypeRef::named_nn_list_nn(&graphql_name),
        move |ctx: ResolverContext| {
            let table_name = table_name.clone();

            FieldFuture::new(async move {
                // Extract pagination arguments
                let limit: i64 = ctx
                    .args
                    .try_get("limit")
                    .ok()
                    .and_then(|v| v.i64().ok())
                    .unwrap_or(100);
                let offset: i64 = ctx
                    .args
                    .try_get("offset")
                    .ok()
                    .and_then(|v| v.i64().ok())
                    .unwrap_or(0);

                // Enforce max limit
                let limit = limit.min(1000);

                // Get DataFusion context from schema data
                let datafusion_ctx = ctx
                    .data::<Arc<SessionContext>>()
                    .map_err(|_e| "Failed to get DataFusion context")?;

                // Build SQL query
                let sql = format!(
                    "SELECT * FROM \"{}\" LIMIT {} OFFSET {}",
                    table_name, limit, offset
                );

                tracing::debug!("Executing query: {}", sql);

                // Execute query
                let df = datafusion_ctx
                    .sql(&sql)
                    .await
                    .map_err(|e| format!("Query execution failed: {}", e))?;

                let batches = df
                    .collect()
                    .await
                    .map_err(|e| format!("Data collection failed: {}", e))?;

                // Convert all rows to GraphQL array
                let mut results = Vec::new();
                for batch in batches {
                    for row_idx in 0..batch.num_rows() {
                        let row_value = record_batch_to_graphql_value(&batch, row_idx)
                            .map_err(|e| format!("Failed to convert row: {}", e))?;
                        results.push(FieldValue::owned_any(row_value));
                    }
                }

                Ok(Some(FieldValue::list(results)))
            })
        },
    )
    .argument(async_graphql::dynamic::InputValue::new(
        "limit",
        TypeRef::named(TypeRef::INT),
    ))
    .argument(async_graphql::dynamic::InputValue::new(
        "offset",
        TypeRef::named(TypeRef::INT),
    ))
}

/// Convert a single row from RecordBatch to GraphQL Value (Object)
///
/// This function handles type conversion from Arrow types to GraphQL types,
/// including special handling for timestamps, dates, and ID fields.
///
/// # Arguments
///
/// * `batch` - The RecordBatch containing the data
/// * `row_idx` - The index of the row to convert
///
/// # Returns
///
/// A GraphQL Value::Object representing the row
pub fn record_batch_to_graphql_value(batch: &RecordBatch, row_idx: usize) -> Result<Value> {
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
                if field.name().ends_with("_id") || field.name() == "id" {
                    Value::String(array.value(row_idx).to_string())
                } else {
                    Value::Number(array.value(row_idx).into())
                }
            }
            ArrowDataType::UInt8 => {
                let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                Value::Number(serde_json::Number::from(array.value(row_idx)))
            }
            ArrowDataType::UInt16 => {
                let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                Value::Number(serde_json::Number::from(array.value(row_idx)))
            }
            ArrowDataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                Value::Number(serde_json::Number::from(array.value(row_idx)))
            }
            ArrowDataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                // For ID fields, convert to string
                if field.name().ends_with("_id") || field.name() == "id" {
                    Value::String(array.value(row_idx).to_string())
                } else {
                    // Note: u64 may not fit in i64/JSON number, so convert to string for large values
                    let val = array.value(row_idx);
                    if val <= i64::MAX as u64 {
                        Value::Number(serde_json::Number::from(val))
                    } else {
                        Value::String(val.to_string())
                    }
                }
            }
            ArrowDataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                let f = array.value(row_idx);
                Value::Number(
                    serde_json::Number::from_f64(f as f64)
                        .ok_or_else(|| NouninatorError::SchemaGeneration("Invalid float value".to_string()))?,
                )
            }
            ArrowDataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                let f = array.value(row_idx);
                Value::Number(
                    serde_json::Number::from_f64(f)
                        .ok_or_else(|| NouninatorError::SchemaGeneration("Invalid float value".to_string()))?,
                )
            }
            ArrowDataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            ArrowDataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Value::Boolean(array.value(row_idx))
            }
            ArrowDataType::Timestamp(unit, _tz) => {
                use datafusion::arrow::datatypes::TimeUnit;
                let timestamp_ns = match unit {
                    TimeUnit::Nanosecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        array.value(row_idx)
                    }
                    TimeUnit::Microsecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        array.value(row_idx) * 1_000
                    }
                    TimeUnit::Millisecond => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        array.value(row_idx) * 1_000_000
                    }
                    TimeUnit::Second => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        array.value(row_idx) * 1_000_000_000
                    }
                };

                // Convert to ISO 8601 string
                let secs = timestamp_ns / 1_000_000_000;
                let nsecs = (timestamp_ns % 1_000_000_000) as u32;

                use chrono::{DateTime, Utc};
                let datetime = DateTime::<Utc>::from_timestamp(secs, nsecs)
                    .ok_or_else(|| {
                        NouninatorError::SchemaGeneration(format!(
                            "Invalid timestamp: {}",
                            timestamp_ns
                        ))
                    })?;
                Value::String(datetime.to_rfc3339())
            }
            ArrowDataType::Date32 => {
                let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = array.value(row_idx);

                use chrono::NaiveDate;
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| {
                        NouninatorError::SchemaGeneration("Invalid base date".to_string())
                    })?
                    .checked_add_signed(chrono::Duration::days(days as i64))
                    .ok_or_else(|| {
                        NouninatorError::SchemaGeneration(format!("Invalid date: {} days", days))
                    })?;

                Value::String(date.format("%Y-%m-%d").to_string())
            }
            ArrowDataType::Date64 => {
                let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
                let millis = array.value(row_idx);

                use chrono::NaiveDate;
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| {
                        NouninatorError::SchemaGeneration("Invalid base date".to_string())
                    })?
                    .checked_add_signed(chrono::Duration::milliseconds(millis))
                    .ok_or_else(|| {
                        NouninatorError::SchemaGeneration(format!("Invalid date: {} ms", millis))
                    })?;

                Value::String(date.format("%Y-%m-%d").to_string())
            }
            _ => {
                tracing::warn!(
                    "Unsupported type {:?} for field '{}', returning null",
                    column.data_type(),
                    field.name()
                );
                Value::Null
            }
        };

        object_map.insert(Name::new(field.name()), value);
    }

    Ok(Value::Object(object_map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    #[test]
    fn test_record_batch_to_graphql_value_basic_types() {
        // Create a simple schema with basic types
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("age", DataType::Int32, true),
            ArrowField::new("active", DataType::Boolean, false),
        ]));

        // Create arrays
        let id_array = Int64Array::from(vec![1]);
        let name_array = StringArray::from(vec!["Alice"]);
        let age_array = Int32Array::from(vec![Some(30)]);
        let active_array = BooleanArray::from(vec![true]);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array),
                Arc::new(active_array),
            ],
        )
        .unwrap();

        // Convert to GraphQL value
        let result = record_batch_to_graphql_value(&batch, 0).unwrap();

        // Verify the result
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("id").unwrap(), &Value::String("1".to_string())); // ID fields are strings
            assert_eq!(obj.get("name").unwrap(), &Value::String("Alice".to_string()));
            assert_eq!(obj.get("age").unwrap(), &Value::Number(30.into()));
            assert_eq!(obj.get("active").unwrap(), &Value::Boolean(true));
        } else {
            panic!("Expected Value::Object");
        }
    }

    #[test]
    fn test_record_batch_to_graphql_value_nullable() {
        // Create schema with nullable field
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("nickname", DataType::Utf8, true),
        ]));

        // Create arrays with null value
        let id_array = Int64Array::from(vec![1]);
        let nickname_array = StringArray::from(vec![None as Option<&str>]);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(nickname_array)],
        )
        .unwrap();

        // Convert to GraphQL value
        let result = record_batch_to_graphql_value(&batch, 0).unwrap();

        // Verify the result
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("id").unwrap(), &Value::String("1".to_string()));
            assert_eq!(obj.get("nickname").unwrap(), &Value::Null);
        } else {
            panic!("Expected Value::Object");
        }
    }

    #[test]
    fn test_record_batch_to_graphql_value_numeric_types() {
        // Create schema with various numeric types
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("int8", DataType::Int8, false),
            ArrowField::new("int16", DataType::Int16, false),
            ArrowField::new("int32", DataType::Int32, false),
            ArrowField::new("int64", DataType::Int64, false),
            ArrowField::new("uint8", DataType::UInt8, false),
            ArrowField::new("float32", DataType::Float32, false),
            ArrowField::new("float64", DataType::Float64, false),
        ]));

        // Create arrays
        let int8_array = Int8Array::from(vec![10i8]);
        let int16_array = Int16Array::from(vec![100i16]);
        let int32_array = Int32Array::from(vec![1000i32]);
        let int64_array = Int64Array::from(vec![10000i64]);
        let uint8_array = UInt8Array::from(vec![255u8]);
        let float32_array = Float32Array::from(vec![3.14f32]);
        let float64_array = Float64Array::from(vec![2.718f64]);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(int8_array),
                Arc::new(int16_array),
                Arc::new(int32_array),
                Arc::new(int64_array),
                Arc::new(uint8_array),
                Arc::new(float32_array),
                Arc::new(float64_array),
            ],
        )
        .unwrap();

        // Convert to GraphQL value
        let result = record_batch_to_graphql_value(&batch, 0).unwrap();

        // Verify the result
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("int8").unwrap(), &Value::Number(10.into()));
            assert_eq!(obj.get("int16").unwrap(), &Value::Number(100.into()));
            assert_eq!(obj.get("int32").unwrap(), &Value::Number(1000.into()));
            assert_eq!(obj.get("int64").unwrap(), &Value::Number(10000.into()));
            assert_eq!(obj.get("uint8").unwrap(), &Value::Number(255.into()));
            // Float comparisons
            match obj.get("float32").unwrap() {
                Value::Number(n) => {
                    let f = n.as_f64().unwrap();
                    assert!((f - 3.14).abs() < 0.01);
                }
                _ => panic!("Expected number"),
            }
            match obj.get("float64").unwrap() {
                Value::Number(n) => {
                    let f = n.as_f64().unwrap();
                    assert!((f - 2.718).abs() < 0.001);
                }
                _ => panic!("Expected number"),
            }
        } else {
            panic!("Expected Value::Object");
        }
    }

    #[test]
    fn test_record_batch_to_graphql_value_id_fields() {
        // Create schema with ID-like fields
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("user_id", DataType::Int64, false),
            ArrowField::new("count", DataType::Int64, false), // Not an ID field
        ]));

        // Create arrays
        let id_array = Int64Array::from(vec![123]);
        let user_id_array = Int64Array::from(vec![456]);
        let count_array = Int64Array::from(vec![789]);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(user_id_array),
                Arc::new(count_array),
            ],
        )
        .unwrap();

        // Convert to GraphQL value
        let result = record_batch_to_graphql_value(&batch, 0).unwrap();

        // Verify the result - ID fields should be strings, non-ID int64 should be numbers
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("id").unwrap(), &Value::String("123".to_string()));
            assert_eq!(obj.get("user_id").unwrap(), &Value::String("456".to_string()));
            assert_eq!(obj.get("count").unwrap(), &Value::Number(789.into()));
        } else {
            panic!("Expected Value::Object");
        }
    }
}

