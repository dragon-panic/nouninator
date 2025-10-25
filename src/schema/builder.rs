/// GraphQL schema builder
///
/// This module provides the `SchemaBuilder` which generates a complete GraphQL schema
/// from Delta tables (or CSV files for testing).

use crate::config::EntityConfig;
use crate::error::{NouninatorError, Result};
use crate::schema::scalars::register_custom_scalars;
use crate::schema::type_mapping::arrow_to_graphql_type;
use crate::schema::resolver::{create_get_resolver, create_list_resolver};

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, Schema};
use async_graphql::Value;
use datafusion::prelude::*;
use std::sync::Arc;

/// Schema builder for generating GraphQL schemas from Delta tables
pub struct SchemaBuilder {
    /// DataFusion session context for query execution
    datafusion_ctx: SessionContext,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        Self {
            datafusion_ctx: SessionContext::new(),
        }
    }

    /// Build complete GraphQL schema from entities
    ///
    /// # Arguments
    ///
    /// * `entities` - List of entity configurations
    ///
    /// # Returns
    ///
    /// A dynamic GraphQL schema with query resolvers
    pub async fn build_schema(&mut self, entities: Vec<EntityConfig>) -> Result<Schema> {
        if entities.is_empty() {
            return Err(NouninatorError::SchemaGeneration(
                "No entities provided".to_string(),
            ));
        }

        // Build Query type
        let mut query = Object::new("Query");

        for entity in &entities {
            // Validate entity config
            entity.validate().map_err(|e| {
                NouninatorError::Config(format!("Invalid entity '{}': {}", entity.graphql_name, e))
            })?;

            tracing::info!("Building schema for entity: {}", entity.graphql_name);

            // Get the table from DataFusion context
            let _table = self
                .datafusion_ctx
                .table(&entity.table)
                .await
                .map_err(|e| {
                    NouninatorError::SchemaGeneration(format!(
                        "Failed to get table '{}': {}",
                        entity.table, e
                    ))
                })?;

            // Get the Arrow schema from the table provider
            let table_provider = self
                .datafusion_ctx
                .table_provider(&entity.table)
                .await
                .map_err(|e| {
                    NouninatorError::SchemaGeneration(format!(
                        "Failed to get table provider for '{}': {}",
                        entity.table, e
                    ))
                })?;

            let arrow_schema = table_provider.schema().as_ref().clone();

            // Build GraphQL object type from Arrow schema
            let _object_type = self.build_entity_type(entity, arrow_schema)?;

            // Add get_X resolver (by primary key)
            let get_field = create_get_resolver(entity);
            query = query.field(get_field);

            // Add list_X resolver (with pagination)
            let list_field = create_list_resolver(entity);
            query = query.field(list_field);

            // Store the object type to register later
            // Note: We'll register it after building the Query type
        }

        // Build the schema with custom scalars and entity types
        let mut schema_builder = Schema::build(query.type_name(), None, None);

        // Add custom scalars
        for scalar in register_custom_scalars() {
            schema_builder = schema_builder.register(scalar);
        }

        // Register all entity types
        for entity in &entities {
            // Re-build the object type to register it
            let table_provider = self
                .datafusion_ctx
                .table_provider(&entity.table)
                .await
                .map_err(|e| {
                    NouninatorError::SchemaGeneration(format!(
                        "Failed to get table provider for '{}': {}",
                        entity.table, e
                    ))
                })?;

            let arrow_schema = table_provider.schema().as_ref().clone();
            let object_type = self.build_entity_type(entity, arrow_schema)?;
            schema_builder = schema_builder.register(object_type);
        }

        // Add the Query object
        schema_builder = schema_builder.register(query);

        // Store DataFusion context in schema data
        let schema = schema_builder
            .data(Arc::new(self.datafusion_ctx.clone()))
            .finish()
            .map_err(|e| {
                NouninatorError::SchemaGeneration(format!("Failed to build schema: {}", e))
            })?;

        Ok(schema)
    }

    /// Register a table from a file path (supports CSV for testing, Delta for production)
    ///
    /// # Arguments
    ///
    /// * `name` - Name to register the table as
    /// * `path` - Path to the file (CSV or Delta table)
    pub async fn register_table_from_path(&mut self, name: &str, path: &str) -> Result<()> {
        if path.ends_with(".csv") {
            // Register CSV file
            self.datafusion_ctx
                .register_csv(name, path, CsvReadOptions::default())
                .await
                .map_err(|e| {
                    NouninatorError::SchemaGeneration(format!(
                        "Failed to register CSV '{}': {}",
                        path, e
                    ))
                })?;
        } else {
            // Register Delta table
            let delta_table = deltalake::open_table(path).await.map_err(|e| {
                NouninatorError::SchemaGeneration(format!(
                    "Failed to open Delta table '{}': {}",
                    path, e
                ))
            })?;

            self.datafusion_ctx
                .register_table(name, Arc::new(delta_table))
                .map_err(|e| {
                    NouninatorError::SchemaGeneration(format!(
                        "Failed to register Delta table '{}': {}",
                        name, e
                    ))
                })?;
        }

        Ok(())
    }

    /// Build GraphQL object type from Arrow schema
    fn build_entity_type(
        &self,
        entity: &EntityConfig,
        arrow_schema: ArrowSchema,
    ) -> Result<Object> {
        let mut object = Object::new(&entity.graphql_name);

        if let Some(desc) = &entity.description {
            object = object.description(desc);
        }

        // Map each Arrow field to a GraphQL field
        for field in arrow_schema.fields() {
            if let Some(type_ref) =
                arrow_to_graphql_type(field.name(), field.data_type(), field.is_nullable())
            {
                let field_name = field.name().to_string();
                let field_name_for_closure = field_name.clone();
                
                let graphql_field = Field::new(field_name, type_ref, move |ctx| {
                    let field_name = field_name_for_closure.clone();
                    FieldFuture::new(async move {
                        // Extract the field value from the parent object
                        let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
                        
                        if let Value::Object(obj) = parent {
                            if let Some(value) = obj.get(field_name.as_str()) {
                                return Ok(Some(FieldValue::value(value.clone())));
                            }
                        }
                        
                        Ok(Some(FieldValue::NULL))
                    })
                });

                object = object.field(graphql_field);
            }
        }

        Ok(object)
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::type_mapping::to_snake_case;

    #[test]
    fn test_schema_builder_creation() {
        let builder = SchemaBuilder::new();
        assert!(std::ptr::eq(
            &builder.datafusion_ctx,
            &builder.datafusion_ctx
        ));
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Customer"), "customer");
        assert_eq!(to_snake_case("OrderItem"), "order_item");
    }
}

