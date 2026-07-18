use koldstore_mirror::pgoutput::{decode_message, PgOutputMessage, PgOutputValue};

fn cstring(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}

fn relation_message() -> Vec<u8> {
    let mut bytes = vec![b'R'];
    bytes.extend_from_slice(&42_u32.to_be_bytes());
    cstring(&mut bytes, "public");
    cstring(&mut bytes, "events");
    bytes.push(b'd');
    bytes.extend_from_slice(&2_u16.to_be_bytes());
    bytes.push(1);
    cstring(&mut bytes, "id");
    bytes.extend_from_slice(&20_u32.to_be_bytes());
    bytes.extend_from_slice(&(-1_i32).to_be_bytes());
    bytes.push(0);
    cstring(&mut bytes, "payload");
    bytes.extend_from_slice(&25_u32.to_be_bytes());
    bytes.extend_from_slice(&(-1_i32).to_be_bytes());
    bytes
}

fn tuple(values: &[Option<&str>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        match value {
            Some(value) => {
                bytes.push(b't');
                bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            None => bytes.push(b'n'),
        }
    }
    bytes
}

#[test]
fn decodes_relation_metadata_and_key_flags() {
    let message = decode_message(&relation_message()).unwrap();
    let PgOutputMessage::Relation(relation) = message else {
        panic!("expected relation message");
    };

    assert_eq!(relation.id, 42);
    assert_eq!(relation.namespace, "public");
    assert_eq!(relation.name, "events");
    assert_eq!(relation.columns.len(), 2);
    assert!(relation.columns[0].key);
    assert_eq!(relation.columns[0].name, "id");
    assert_eq!(relation.columns[0].type_oid, 20);
    assert!(!relation.columns[1].key);
}

#[test]
fn decodes_insert_update_delete_and_commit_messages() {
    let mut insert = vec![b'I'];
    insert.extend_from_slice(&42_u32.to_be_bytes());
    insert.push(b'N');
    insert.extend(tuple(&[Some("7"), Some("hello")]));
    let PgOutputMessage::Insert { relation_id, new } = decode_message(&insert).unwrap() else {
        panic!("expected insert");
    };
    assert_eq!(relation_id, 42);
    assert_eq!(new.values[0], PgOutputValue::Text(b"7".to_vec()));

    let mut update = vec![b'U'];
    update.extend_from_slice(&42_u32.to_be_bytes());
    update.push(b'K');
    update.extend(tuple(&[Some("7")]));
    update.push(b'N');
    update.extend(tuple(&[Some("7"), Some("updated")]));
    let PgOutputMessage::Update { old, new, .. } = decode_message(&update).unwrap() else {
        panic!("expected update");
    };
    assert_eq!(old.unwrap().values[0], PgOutputValue::Text(b"7".to_vec()));
    assert_eq!(new.values[1], PgOutputValue::Text(b"updated".to_vec()));

    let mut delete = vec![b'D'];
    delete.extend_from_slice(&42_u32.to_be_bytes());
    delete.push(b'K');
    delete.extend(tuple(&[Some("7")]));
    assert!(matches!(
        decode_message(&delete).unwrap(),
        PgOutputMessage::Delete {
            relation_id: 42,
            ..
        }
    ));

    let mut commit = vec![b'C', 0];
    commit.extend_from_slice(&100_u64.to_be_bytes());
    commit.extend_from_slice(&120_u64.to_be_bytes());
    commit.extend_from_slice(&0_i64.to_be_bytes());
    assert!(matches!(
        decode_message(&commit).unwrap(),
        PgOutputMessage::Commit {
            commit_lsn: 100,
            end_lsn: 120
        }
    ));
}

#[test]
fn rejects_truncated_and_unknown_messages() {
    assert!(decode_message(&[b'R', 0, 0]).is_err());
    assert!(decode_message(b"?").is_err());
}

#[test]
fn handles_truncate_origin_type_and_message_tags() {
    for tag in [b'T', b'Y', b'M'] {
        let message = decode_message(&[tag]).unwrap();
        assert_eq!(message, PgOutputMessage::Ignored { tag });
    }

    let mut origin = vec![b'O'];
    origin.extend_from_slice(&100_u64.to_be_bytes());
    cstring(&mut origin, "koldstore_flush");
    assert_eq!(
        decode_message(&origin),
        Ok(PgOutputMessage::Origin {
            name: "koldstore_flush".to_string(),
        })
    );
}
