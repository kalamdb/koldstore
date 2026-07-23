//! Bounded decoder for PostgreSQL's `pgoutput` protocol.
//!
//! Owns the PostgreSQL-free binary decoder used by async mirror apply. Malformed
//! logical-decoding output is rejected here before SPI or mirror mutation code.
//! Execution and slot lifecycle stay in `pg_koldstore::async_mirror`.

use thiserror::Error;

/// One column described by a `pgoutput` relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputColumn {
    /// Whether the publication marks this column as part of replica identity.
    pub key: bool,
    /// Source column name.
    pub name: String,
    /// Source PostgreSQL type OID.
    pub type_oid: u32,
    /// Source PostgreSQL type modifier.
    pub typmod: i32,
}

/// Relation metadata needed to map logical tuples to a managed source table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputRelation {
    /// Publication-local relation identifier (the source relation OID).
    pub id: u32,
    /// Source namespace.
    pub namespace: String,
    /// Source relation name.
    pub name: String,
    /// PostgreSQL replica identity marker.
    pub replica_identity: u8,
    /// Published columns in tuple order.
    pub columns: Vec<PgOutputColumn>,
}

/// One tuple-column value in a logical replication message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputValue {
    /// SQL NULL.
    Null,
    /// An unchanged toasted value omitted by PostgreSQL.
    UnchangedToast,
    /// Text-format value.
    Text(Vec<u8>),
    /// Binary-format value.
    Binary(Vec<u8>),
}

/// Tuple values in relation-column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputTuple {
    /// Decoded column values.
    pub values: Vec<PgOutputValue>,
}

/// Logical messages needed by the asynchronous mirror applier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputMessage {
    /// Start of one source transaction.
    Begin {
        /// LSN at which the transaction is expected to commit.
        final_lsn: u64,
        /// Source transaction id.
        xid: u32,
    },
    /// Successful source transaction commit.
    Commit {
        /// Commit record LSN.
        commit_lsn: u64,
        /// End of the commit record.
        end_lsn: u64,
    },
    /// Relation metadata refresh.
    Relation(PgOutputRelation),
    /// Inserted tuple.
    Insert {
        /// Source relation identifier.
        relation_id: u32,
        /// New tuple.
        new: PgOutputTuple,
    },
    /// Updated tuple and optional replica-identity old tuple.
    Update {
        /// Source relation identifier.
        relation_id: u32,
        /// Old key or full tuple when emitted.
        old: Option<PgOutputTuple>,
        /// New tuple.
        new: PgOutputTuple,
    },
    /// Deleted replica-identity tuple.
    Delete {
        /// Source relation identifier.
        relation_id: u32,
        /// Old key or full tuple.
        old: PgOutputTuple,
    },
    /// Replication origin for the current transaction (when stamped).
    Origin {
        /// Origin name from `pg_replication_origin`.
        name: String,
    },
    /// A known protocol message not needed by protocol version 1 apply.
    Ignored {
        /// Message type byte.
        tag: u8,
    },
}

/// `pgoutput` decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PgOutputDecodeError {
    /// Input ended before the declared message shape was complete.
    #[error("truncated pgoutput message while reading {0}")]
    Truncated(&'static str),
    /// A cstring was not valid UTF-8.
    #[error("invalid UTF-8 in pgoutput {0}")]
    InvalidUtf8(&'static str),
    /// The message tag is not part of the supported protocol.
    #[error("unsupported pgoutput message tag 0x{0:02x}")]
    UnsupportedTag(u8),
    /// A tuple marker or value marker was invalid.
    #[error("invalid pgoutput {kind} marker 0x{marker:02x}")]
    InvalidMarker {
        /// Marker category.
        kind: &'static str,
        /// Invalid byte.
        marker: u8,
    },
    /// Bytes remained after decoding the message.
    #[error("pgoutput message has {0} trailing bytes")]
    TrailingBytes(usize),
}

/// Decodes one complete binary `pgoutput` message.
///
/// # Errors
///
/// Returns [`PgOutputDecodeError`] for truncated, malformed, unsupported, or
/// trailing input.
pub fn decode_message(input: &[u8]) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let mut reader = Reader::new(input);
    let tag = reader.byte("message tag")?;
    let message = match tag {
        b'B' => decode_begin(&mut reader)?,
        b'C' => decode_commit(&mut reader)?,
        b'R' => PgOutputMessage::Relation(decode_relation(&mut reader)?),
        b'I' => decode_insert(&mut reader)?,
        b'U' => decode_update(&mut reader)?,
        b'D' => decode_delete(&mut reader)?,
        // ORIGIN is required on PG15: flush prune stamps `koldstore_flush` and
        // apply must skip those deletes (PG16+ also filters via origin=none).
        // Wire order matches logicalrep_write_origin: LSN then cstring name.
        b'O' => decode_origin(&mut reader)?,
        // Type, truncate, and logical-message records are intentionally ignored
        // by protocol-v1 mirror apply. The publication excludes messages and
        // managed tables reject TRUNCATE at their SQL boundary.
        b'Y' | b'T' | b'M' => PgOutputMessage::Ignored { tag },
        _ => return Err(PgOutputDecodeError::UnsupportedTag(tag)),
    };
    if !reader.is_empty() && !matches!(message, PgOutputMessage::Ignored { .. }) {
        return Err(PgOutputDecodeError::TrailingBytes(reader.remaining()));
    }
    Ok(message)
}

fn decode_begin(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let final_lsn = reader.u64("begin final_lsn")?;
    let _commit_time = reader.i64("begin commit_time")?;
    let xid = reader.u32("begin xid")?;
    Ok(PgOutputMessage::Begin { final_lsn, xid })
}

fn decode_commit(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let _flags = reader.byte("commit flags")?;
    let commit_lsn = reader.u64("commit lsn")?;
    let end_lsn = reader.u64("commit end_lsn")?;
    let _commit_time = reader.i64("commit time")?;
    Ok(PgOutputMessage::Commit {
        commit_lsn,
        end_lsn,
    })
}

fn decode_insert(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let relation_id = reader.u32("insert relation id")?;
    expect_marker(reader, b'N', "insert tuple")?;
    let new = decode_tuple(reader)?;
    Ok(PgOutputMessage::Insert { relation_id, new })
}

fn decode_delete(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let relation_id = reader.u32("delete relation id")?;
    let marker = reader.byte("delete tuple marker")?;
    if !matches!(marker, b'K' | b'O') {
        return Err(PgOutputDecodeError::InvalidMarker {
            kind: "delete tuple",
            marker,
        });
    }
    let old = decode_tuple(reader)?;
    Ok(PgOutputMessage::Delete { relation_id, old })
}

fn decode_origin(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let _origin_lsn = reader.u64("origin lsn")?;
    let name = reader.cstring("origin name")?.to_string();
    Ok(PgOutputMessage::Origin { name })
}

fn decode_relation(reader: &mut Reader<'_>) -> Result<PgOutputRelation, PgOutputDecodeError> {
    let id = reader.u32("relation id")?;
    let namespace = reader.cstring("relation namespace")?.to_string();
    let name = reader.cstring("relation name")?.to_string();
    let replica_identity = reader.byte("relation replica identity")?;
    let column_count = usize::from(reader.u16("relation column count")?);
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let flags = reader.byte("relation column flags")?;
        columns.push(PgOutputColumn {
            key: flags & 1 == 1,
            name: reader.cstring("relation column name")?.to_string(),
            type_oid: reader.u32("relation column type oid")?,
            typmod: reader.i32("relation column typmod")?,
        });
    }
    Ok(PgOutputRelation {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_update(reader: &mut Reader<'_>) -> Result<PgOutputMessage, PgOutputDecodeError> {
    let relation_id = reader.u32("update relation id")?;
    let marker = reader.byte("update tuple marker")?;
    let (old, new_marker) = match marker {
        b'K' | b'O' => (
            Some(decode_tuple(reader)?),
            reader.byte("update new tuple marker")?,
        ),
        b'N' => (None, b'N'),
        _ => {
            return Err(PgOutputDecodeError::InvalidMarker {
                kind: "update tuple",
                marker,
            })
        }
    };
    if new_marker != b'N' {
        return Err(PgOutputDecodeError::InvalidMarker {
            kind: "update new tuple",
            marker: new_marker,
        });
    }
    Ok(PgOutputMessage::Update {
        relation_id,
        old,
        new: decode_tuple(reader)?,
    })
}

fn decode_tuple(reader: &mut Reader<'_>) -> Result<PgOutputTuple, PgOutputDecodeError> {
    let count = usize::from(reader.u16("tuple column count")?);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let marker = reader.byte("tuple value marker")?;
        let value = match marker {
            b'n' => PgOutputValue::Null,
            b'u' => PgOutputValue::UnchangedToast,
            b't' | b'b' => {
                let length = reader.u32("tuple value length")? as usize;
                let bytes = reader.bytes(length, "tuple value")?.to_vec();
                if marker == b't' {
                    PgOutputValue::Text(bytes)
                } else {
                    PgOutputValue::Binary(bytes)
                }
            }
            _ => {
                return Err(PgOutputDecodeError::InvalidMarker {
                    kind: "tuple value",
                    marker,
                })
            }
        };
        values.push(value);
    }
    Ok(PgOutputTuple { values })
}

fn expect_marker(
    reader: &mut Reader<'_>,
    expected: u8,
    kind: &'static str,
) -> Result<(), PgOutputDecodeError> {
    let marker = reader.byte(kind)?;
    if marker == expected {
        Ok(())
    } else {
        Err(PgOutputDecodeError::InvalidMarker { kind, marker })
    }
}

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.offset)
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn bytes(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], PgOutputDecodeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PgOutputDecodeError::Truncated(field))?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(PgOutputDecodeError::Truncated(field))?;
        self.offset = end;
        Ok(bytes)
    }

    fn byte(&mut self, field: &'static str) -> Result<u8, PgOutputDecodeError> {
        Ok(self.bytes(1, field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, PgOutputDecodeError> {
        let bytes: [u8; 2] = self
            .bytes(2, field)?
            .try_into()
            .map_err(|_| PgOutputDecodeError::Truncated(field))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, PgOutputDecodeError> {
        let bytes: [u8; 4] = self
            .bytes(4, field)?
            .try_into()
            .map_err(|_| PgOutputDecodeError::Truncated(field))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, PgOutputDecodeError> {
        let bytes: [u8; 4] = self
            .bytes(4, field)?
            .try_into()
            .map_err(|_| PgOutputDecodeError::Truncated(field))?;
        Ok(i32::from_be_bytes(bytes))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, PgOutputDecodeError> {
        let bytes: [u8; 8] = self
            .bytes(8, field)?
            .try_into()
            .map_err(|_| PgOutputDecodeError::Truncated(field))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, PgOutputDecodeError> {
        let bytes: [u8; 8] = self
            .bytes(8, field)?
            .try_into()
            .map_err(|_| PgOutputDecodeError::Truncated(field))?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn cstring(&mut self, field: &'static str) -> Result<&'a str, PgOutputDecodeError> {
        let remaining = self
            .input
            .get(self.offset..)
            .ok_or(PgOutputDecodeError::Truncated(field))?;
        let terminator = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(PgOutputDecodeError::Truncated(field))?;
        let bytes = self.bytes(terminator, field)?;
        self.offset += 1;
        std::str::from_utf8(bytes).map_err(|_| PgOutputDecodeError::InvalidUtf8(field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_message_with_body_is_ignored_without_trailing_byte_error() {
        // pgoutput Truncate ('T'): nrels=1, flags=0, oid=12345 — body must not
        // trip TrailingBytes because Ignored intentionally skips the payload.
        let mut message = vec![b'T'];
        message.extend_from_slice(&1_i32.to_be_bytes());
        message.push(0);
        message.extend_from_slice(&12_345_u32.to_be_bytes());
        assert_eq!(
            decode_message(&message),
            Ok(PgOutputMessage::Ignored { tag: b'T' })
        );
    }

    #[test]
    fn origin_message_decodes_name_and_lsn() {
        let mut message = vec![b'O'];
        message.extend_from_slice(&0x11_22_33_44_55_66_77_88_u64.to_be_bytes());
        message.extend_from_slice(b"koldstore_flush\0");
        assert_eq!(
            decode_message(&message),
            Ok(PgOutputMessage::Origin {
                name: "koldstore_flush".to_string(),
            })
        );
    }

    #[test]
    fn type_and_logical_message_tags_are_ignored() {
        for tag in [b'Y', b'M'] {
            assert_eq!(
                decode_message(&[tag, 0x01, 0x02, 0x03]),
                Ok(PgOutputMessage::Ignored { tag })
            );
        }
    }
}
