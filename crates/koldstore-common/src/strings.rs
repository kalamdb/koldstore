//! Shared string collection helpers.

/// Returns unique non-blank strings in first-seen order.
#[must_use]
pub fn dedupe_nonblank<I, S>(values: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    values.into_iter().fold(Vec::new(), |mut columns, value| {
        let column = value.into();
        let column = column.trim();
        if !column.is_empty() && !columns.iter().any(|existing| existing == column) {
            columns.push(column.to_string());
        }
        columns
    })
}
