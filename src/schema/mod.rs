/// GraphQL schema generation from Delta tables
///
/// This module provides functionality to generate GraphQL schemas from Delta table
/// Arrow schemas, including type mapping, resolvers, and dynamic schema building.

mod builder;
mod resolver;
mod scalars;
mod type_mapping;

pub use builder::SchemaBuilder;
pub use resolver::{create_get_resolver, create_list_resolver, record_batch_to_graphql_value};
pub use scalars::{register_custom_scalars, Date, DateTime};
pub use type_mapping::arrow_to_graphql_type;

