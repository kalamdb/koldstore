//! Validated short identifiers for registered object storage locations.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// An eight-character lowercase hexadecimal storage identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct StorageId(String);

impl StorageId {
    /// Parses an eight-character lowercase hexadecimal token.
    ///
    /// # Errors
    ///
    /// Returns an error when the token is not exactly eight lowercase hex
    /// characters.
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let valid = value.len() == 8
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !valid {
            return Err("storage id must be exactly 8 lowercase hex characters".to_string());
        }
        Ok(Self(value))
    }

    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for StorageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for StorageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageId;

    #[test]
    fn accepts_only_lowercase_eight_digit_hex_tokens() {
        assert!(StorageId::new("a1b2c3d4").is_ok());
        assert!(StorageId::new("A1b2c3d4").is_err());
        assert!(StorageId::new("a1b2c3").is_err());
        assert!(StorageId::new("a1b2c3d4e").is_err());
    }
}
