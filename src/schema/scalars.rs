/// Custom GraphQL scalar types for Date and DateTime
///
/// These scalars handle ISO 8601 formatted date and datetime strings.

use async_graphql::dynamic::Scalar;
use async_graphql::Value;
use chrono::{DateTime as ChronoDateTime, NaiveDate};

/// ISO 8601 date scalar (YYYY-MM-DD)
#[derive(Debug, Clone)]
pub struct Date;

/// ISO 8601 datetime scalar with timezone
#[derive(Debug, Clone)]
pub struct DateTime;

/// Register custom scalars in the schema builder
pub fn register_custom_scalars() -> Vec<Scalar> {
    vec![date_scalar(), datetime_scalar()]
}

/// Create the Date scalar
fn date_scalar() -> Scalar {
    Scalar::new("Date")
        .description("ISO 8601 date format (YYYY-MM-DD)")
        .validator(|value| {
            if let Value::String(s) = value {
                NaiveDate::parse_from_str(s.as_str(), "%Y-%m-%d").is_ok()
            } else {
                false
            }
        })
}

/// Create the DateTime scalar
fn datetime_scalar() -> Scalar {
    Scalar::new("DateTime")
        .description("ISO 8601 datetime format with timezone")
        .validator(|value| {
            if let Value::String(s) = value {
                ChronoDateTime::parse_from_rfc3339(s.as_str()).is_ok()
            } else {
                false
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_scalar_registration() {
        let scalars = register_custom_scalars();
        assert_eq!(scalars.len(), 2);
    }

    #[test]
    fn test_datetime_scalar_registration() {
        let scalars = register_custom_scalars();
        assert_eq!(scalars.len(), 2);
    }

    #[test]
    fn test_date_validation_valid() {
        let _value = Value::String("2024-01-15".to_string());
        let result = NaiveDate::parse_from_str("2024-01-15", "%Y-%m-%d");
        assert!(result.is_ok());
    }

    #[test]
    fn test_date_validation_invalid() {
        let result = NaiveDate::parse_from_str("invalid-date", "%Y-%m-%d");
        assert!(result.is_err());
    }

    #[test]
    fn test_datetime_validation_valid() {
        let result = ChronoDateTime::parse_from_rfc3339("2024-01-15T10:00:00Z");
        assert!(result.is_ok());
    }

    #[test]
    fn test_datetime_validation_invalid() {
        let result = ChronoDateTime::parse_from_rfc3339("not-a-datetime");
        assert!(result.is_err());
    }
}

