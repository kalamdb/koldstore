//! Publication DDL helpers for async-mirror column lists (PostgreSQL-free).

use koldstore_common::{quote_ident, PrimaryKeyShape};

/// Quoted PK (+ optional segment-order) column list for `ALTER PUBLICATION … SET TABLE`.
#[must_use]
pub fn published_column_list(primary_key: &PrimaryKeyShape, order_column: Option<&str>) -> String {
    let mut published = primary_key
        .columns()
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    if let Some(order_column) = order_column {
        let quoted = quote_ident(order_column);
        if !published.iter().any(|column| column == &quoted) {
            published.push(quoted);
        }
    }
    published.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_common::{
        ColumnId, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
        PrimaryKeyShape,
    };

    fn shape(columns: &[&str]) -> PrimaryKeyShape {
        let cols = columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                PrimaryKeyColumnShape::new(
                    ColumnId::from_attnum(i as i16 + 1),
                    PkColumn::new(*name).unwrap(),
                    PkOrdinal::new(i as u16 + 1).unwrap(),
                    PgTypeOid::new(23).unwrap(),
                    PgTypeName::new("int4").unwrap(),
                    PgTypmod::new(-1),
                    None,
                    None,
                    true,
                )
            })
            .collect();
        PrimaryKeyShape::new(cols).unwrap()
    }

    #[test]
    fn includes_order_column_once() {
        let pk = shape(&["id"]);
        assert_eq!(published_column_list(&pk, Some("id")), quote_ident("id"));
        assert_eq!(
            published_column_list(&pk, Some("seg_order")),
            format!("{}, {}", quote_ident("id"), quote_ident("seg_order"))
        );
    }
}
