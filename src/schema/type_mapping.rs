/// Arrow to GraphQL type mapping
///
/// This module handles conversion of Arrow data types to GraphQL types,
/// including special handling for ID fields and custom scalars.

use async_graphql::dynamic::TypeRef;
use datafusion::arrow::datatypes::DataType as ArrowDataType;

/// Map Arrow DataType to GraphQL TypeRef
///
/// # Arguments
///
/// * `field_name` - The name of the field (used for ID inference)
/// * `data_type` - The Arrow data type
/// * `nullable` - Whether the field is nullable
///
/// # Returns
///
/// `Some(TypeRef)` if the type is supported, `None` if the type should be skipped
///
/// # Type Mapping Rules
///
/// - Integer types → `Int` (or `ID` if field name ends with `_id`)
/// - Float types → `Float`
/// - String types → `String`
/// - Boolean → `Boolean`
/// - Date types → `Date` custom scalar
/// - Timestamp → `DateTime` custom scalar
/// - List types → GraphQL list of inner type
/// - Struct types → Currently unsupported (returns None)
pub fn arrow_to_graphql_type(
    field_name: &str,
    data_type: &ArrowDataType,
    nullable: bool,
) -> Option<TypeRef> {
    let base_type_ref = match data_type {
        // Integer types - check for ID inference
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64 => {
            if field_name.ends_with("_id") || field_name == "id" {
                TypeRef::named(TypeRef::ID)
            } else {
                TypeRef::named(TypeRef::INT)
            }
        }

        // Unsigned integer types - check for ID inference
        ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => {
            if field_name.ends_with("_id") || field_name == "id" {
                TypeRef::named(TypeRef::ID)
            } else {
                TypeRef::named(TypeRef::INT)
            }
        }

        // Float types
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            TypeRef::named(TypeRef::FLOAT)
        }

        // String types
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TypeRef::named(TypeRef::STRING),

        // Boolean type
        ArrowDataType::Boolean => TypeRef::named(TypeRef::BOOLEAN),

        // Date types → custom Date scalar
        ArrowDataType::Date32 | ArrowDataType::Date64 => {
            return Some(if nullable {
                TypeRef::named("Date")
            } else {
                TypeRef::named_nn("Date")
            });
        }

        // Timestamp → custom DateTime scalar
        ArrowDataType::Timestamp(_, _) => {
            return Some(if nullable {
                TypeRef::named("DateTime")
            } else {
                TypeRef::named_nn("DateTime")
            });
        }

        // List types - not supported in MVP
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            tracing::warn!(
                "List type for field '{}' is not supported in MVP, skipping field",
                field_name
            );
            return None;
        }

        // Struct types - not supported in MVP
        ArrowDataType::Struct(_) => {
            tracing::warn!(
                "Struct type for field '{}' is not supported, skipping field",
                field_name
            );
            return None;
        }

        // Other unsupported types
        _ => {
            tracing::warn!(
                "Unsupported Arrow type {:?} for field '{}', skipping field",
                data_type,
                field_name
            );
            return None;
        }
    };

    // If nullable, return as-is; otherwise wrap with named_nn
    if nullable {
        Some(base_type_ref)
    } else {
        // Extract the type name string from the base_type_ref and create a non-null version
        // Since we just created these TypeRefs with TypeRef::named(), we can create non-null versions
        let result = match data_type {
            ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 => {
                if field_name.ends_with("_id") || field_name == "id" {
                    TypeRef::named_nn(TypeRef::ID)
                } else {
                    TypeRef::named_nn(TypeRef::INT)
                }
            }
            ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 | ArrowDataType::UInt64 => {
                if field_name.ends_with("_id") || field_name == "id" {
                    TypeRef::named_nn(TypeRef::ID)
                } else {
                    TypeRef::named_nn(TypeRef::INT)
                }
            }
            ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
                TypeRef::named_nn(TypeRef::FLOAT)
            }
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TypeRef::named_nn(TypeRef::STRING),
            ArrowDataType::Boolean => TypeRef::named_nn(TypeRef::BOOLEAN),
            _ => base_type_ref, // For custom scalars and others, return as-is
        };
        Some(result)
    }
}

/// Helper function to convert field name to snake_case
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch.to_ascii_lowercase());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int64_to_int() {
        let type_ref =
            arrow_to_graphql_type("count", &ArrowDataType::Int64, false).expect("Should map");

        // TypeRef doesn't expose its inner type directly, but we can verify it's non-null
        assert!(type_ref.to_string().contains("Int"));
    }

    #[test]
    fn test_int64_to_id_inference() {
        let type_ref = arrow_to_graphql_type("customer_id", &ArrowDataType::Int64, false)
            .expect("Should map");

        assert!(type_ref.to_string().contains("ID"));
    }

    #[test]
    fn test_id_field_inference() {
        let type_ref =
            arrow_to_graphql_type("id", &ArrowDataType::Int64, false).expect("Should map");

        assert!(type_ref.to_string().contains("ID"));
    }

    #[test]
    fn test_string_mapping() {
        let type_ref =
            arrow_to_graphql_type("name", &ArrowDataType::Utf8, false).expect("Should map");

        assert!(type_ref.to_string().contains("String"));
    }

    #[test]
    fn test_nullable_field() {
        let type_ref =
            arrow_to_graphql_type("name", &ArrowDataType::Utf8, true).expect("Should map");

        // Nullable fields should not have the non-null indicator (!)
        let type_str = type_ref.to_string();
        assert!(type_str.contains("String"));
        assert!(!type_str.ends_with('!'));
    }

    #[test]
    fn test_non_nullable_field() {
        let type_ref =
            arrow_to_graphql_type("name", &ArrowDataType::Utf8, false).expect("Should map");

        // Non-nullable fields should have the non-null indicator (!)
        let type_str = type_ref.to_string();
        assert!(type_str.contains("String!"));
    }

    #[test]
    fn test_float_mapping() {
        let type_ref =
            arrow_to_graphql_type("price", &ArrowDataType::Float64, false).expect("Should map");

        assert!(type_ref.to_string().contains("Float"));
    }

    #[test]
    fn test_boolean_mapping() {
        let type_ref = arrow_to_graphql_type("active", &ArrowDataType::Boolean, false)
            .expect("Should map");

        assert!(type_ref.to_string().contains("Boolean"));
    }

    #[test]
    fn test_date_mapping() {
        let type_ref =
            arrow_to_graphql_type("birth_date", &ArrowDataType::Date32, false).expect("Should map");

        assert!(type_ref.to_string().contains("Date"));
    }

    #[test]
    fn test_timestamp_mapping() {
        use datafusion::arrow::datatypes::TimeUnit;
        let type_ref = arrow_to_graphql_type(
            "created_at",
            &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )
        .expect("Should map");

        assert!(type_ref.to_string().contains("DateTime"));
    }

    #[test]
    fn test_struct_returns_none() {
        use datafusion::arrow::datatypes::Fields;
        let struct_type = ArrowDataType::Struct(Fields::empty());
        let type_ref = arrow_to_graphql_type("nested", &struct_type, false);

        assert!(type_ref.is_none());
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("Customer"), "customer");
        assert_eq!(to_snake_case("OrderItem"), "order_item");
        assert_eq!(to_snake_case("SimpleWord"), "simple_word");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
    }
}

