//! Flush policy parsing and mirror-backed selection helpers.

use std::time::Duration;

use thiserror::Error;

/// Parsed flush policy for clean-schema mirror selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushPolicy {
    /// Maximum pending hot mirror rows to keep before flushing oldest rows.
    pub row_limit: Option<u64>,
    /// Row-age threshold for duration policies.
    pub duration: Option<Duration>,
}

impl FlushPolicy {
    /// Parses a comma-separated flush policy string.
    ///
    /// Supported keys:
    /// - `rows:N`: keep at most N pending hot mirror rows
    /// - `duration:S`: flush rows older than S, where S may use `s`, `m`, `h`, or `d`
    /// - `interval:S`: compatibility alias for `duration:S` in seconds or with units
    ///
    /// # Errors
    ///
    /// Returns an error for blank input, unknown keys, invalid numbers, duplicate
    /// keys, or zero-valued policy components.
    pub fn parse(value: &str) -> Result<Self, FlushPolicyError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(FlushPolicyError::Blank);
        }

        let mut policy = Self::default();
        for part in trimmed.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let Some((key, raw_value)) = part.split_once(':') else {
                return Err(FlushPolicyError::InvalidPart(part.to_string()));
            };
            let key = key.trim();
            let raw_value = raw_value.trim();
            match key {
                "rows" => {
                    if policy.row_limit.is_some() {
                        return Err(FlushPolicyError::DuplicateKey(key.to_string()));
                    }
                    policy.row_limit = Some(parse_positive_u64(key, raw_value)?);
                }
                "duration" | "interval" => {
                    if policy.duration.is_some() {
                        return Err(FlushPolicyError::DuplicateKey(key.to_string()));
                    }
                    policy.duration = Some(parse_duration(raw_value)?);
                }
                unknown => return Err(FlushPolicyError::UnknownKey(unknown.to_string())),
            }
        }

        if policy.row_limit.is_none() && policy.duration.is_none() {
            return Err(FlushPolicyError::Blank);
        }

        Ok(policy)
    }
}

/// Flush policy parsing error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FlushPolicyError {
    /// Policy input is blank.
    #[error("flush_policy cannot be blank")]
    Blank,
    /// A policy part is not `key:value`.
    #[error("invalid flush_policy part `{0}`")]
    InvalidPart(String),
    /// The policy key is unknown.
    #[error("unknown flush_policy key `{0}`")]
    UnknownKey(String),
    /// The same policy key was supplied more than once.
    #[error("duplicate flush_policy key `{0}`")]
    DuplicateKey(String),
    /// Numeric value is invalid.
    #[error("invalid numeric flush_policy value for {key}: {value}")]
    InvalidNumber {
        /// Policy key.
        key: String,
        /// Rejected value.
        value: String,
    },
    /// Duration value is invalid.
    #[error("invalid duration flush_policy value `{value}`")]
    InvalidDuration {
        /// Rejected value.
        value: String,
    },
}

fn parse_positive_u64(key: &str, value: &str) -> Result<u64, FlushPolicyError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| FlushPolicyError::InvalidNumber {
            key: key.to_string(),
            value: value.to_string(),
        })?;
    if parsed == 0 {
        return Err(FlushPolicyError::InvalidNumber {
            key: key.to_string(),
            value: value.to_string(),
        });
    }
    Ok(parsed)
}

fn parse_duration(value: &str) -> Result<Duration, FlushPolicyError> {
    if value.is_empty() {
        return Err(FlushPolicyError::InvalidDuration {
            value: value.to_string(),
        });
    }

    let (number, multiplier) = match value.as_bytes().last().copied() {
        Some(b'd') => (&value[..value.len() - 1], 86_400),
        Some(b'h') => (&value[..value.len() - 1], 3_600),
        Some(b'm') => (&value[..value.len() - 1], 60),
        Some(b's') => (&value[..value.len() - 1], 1),
        Some(byte) if byte.is_ascii_digit() => (value, 1),
        _ => {
            return Err(FlushPolicyError::InvalidDuration {
                value: value.to_string(),
            })
        }
    };

    let base = number
        .parse::<u64>()
        .map_err(|_| FlushPolicyError::InvalidDuration {
            value: value.to_string(),
        })?;
    let seconds = base
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| FlushPolicyError::InvalidDuration {
            value: value.to_string(),
        })?;
    Ok(Duration::from_secs(seconds))
}
