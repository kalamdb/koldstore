//! Page-index pruning helpers for equality / IN probes.
//!
//! Builds a [`RowSelection`] from Parquet column + offset indexes so non-matching
//! pages can be skipped before decoding. Used only for pushdown-safe predicates
//! (PK equality / IN, trusted scope equality). Missing indexes fall back to a
//! full-row-group read (conservative).

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::schema::types::SchemaDescriptor;

use crate::prune::column_index;

/// How page indexes were used for one Parquet read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PageIndexPruneMode {
    /// No equality probe requested page-index pruning.
    #[default]
    NotRequested,
    /// Footer had no usable column/offset index for the probe column.
    Absent,
    /// Page min/max produced a [`RowSelection`].
    Applied,
}

impl PageIndexPruneMode {
    /// Short label for EXPLAIN / tracing.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Absent => "absent",
            Self::Applied => "applied",
        }
    }
}

/// Result of attempting page-index pruning for equality values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagePruneDecision {
    /// Row selection to apply after row-group filtering (`None` = read all rows
    /// in the selected row groups).
    pub selection: Option<RowSelection>,
    /// Whether page indexes were present and applied.
    pub mode: PageIndexPruneMode,
    /// Data pages considered across selected row groups.
    pub pages_total: usize,
    /// Pages whose min/max may contain a probe value.
    pub pages_selected: usize,
    /// Pages skipped by min/max (or all-null).
    pub pages_skipped: usize,
}

impl PagePruneDecision {
    /// No probe requested.
    #[must_use]
    pub fn not_requested() -> Self {
        Self {
            selection: None,
            mode: PageIndexPruneMode::NotRequested,
            pages_total: 0,
            pages_selected: 0,
            pages_skipped: 0,
        }
    }

    /// Indexes missing; caller should decode selected row groups fully.
    #[must_use]
    pub fn absent() -> Self {
        Self {
            selection: None,
            mode: PageIndexPruneMode::Absent,
            pages_total: 0,
            pages_selected: 0,
            pages_skipped: 0,
        }
    }
}

/// Builds a [`RowSelection`] from page indexes for string-encoded equality values.
///
/// Selection spans are relative to the concatenation of `selected_row_groups`
/// (the same contract as `ParquetRecordBatchStreamBuilder::with_row_selection`
/// after `with_row_groups`).
///
/// # Errors
///
/// Returns an error when the Parquet schema is missing `column`.
pub fn row_selection_for_equality_values(
    metadata: &ParquetMetaData,
    schema: &SchemaDescriptor,
    selected_row_groups: &[usize],
    column: &str,
    values: &[String],
) -> Result<PagePruneDecision, String> {
    if values.is_empty() || selected_row_groups.is_empty() {
        return Ok(PagePruneDecision {
            selection: Some(RowSelection::from(vec![])),
            mode: PageIndexPruneMode::Applied,
            pages_total: 0,
            pages_selected: 0,
            pages_skipped: 0,
        });
    }

    let Some(column_indexes) = metadata.column_index() else {
        return Ok(PagePruneDecision::absent());
    };
    let Some(offset_indexes) = metadata.offset_index() else {
        return Ok(PagePruneDecision::absent());
    };

    let column_idx = column_index(schema, column)?;
    let mut selectors = Vec::new();
    let mut pages_total = 0usize;
    let mut pages_selected = 0usize;
    let mut pages_skipped = 0usize;
    let mut saw_usable_index = false;

    for &rg_index in selected_row_groups {
        let rg_rows = usize::try_from(metadata.row_group(rg_index).num_rows())
            .map_err(|error| error.to_string())?;
        if rg_index >= column_indexes.len() || rg_index >= offset_indexes.len() {
            // Conservative: select the whole row group.
            if rg_rows > 0 {
                selectors.push(RowSelector::select(rg_rows));
            }
            continue;
        }
        if column_idx >= column_indexes[rg_index].len()
            || column_idx >= offset_indexes[rg_index].len()
        {
            if rg_rows > 0 {
                selectors.push(RowSelector::select(rg_rows));
            }
            continue;
        }

        let page_index = &column_indexes[rg_index][column_idx];
        let offset_index = &offset_indexes[rg_index][column_idx];
        if matches!(page_index, ColumnIndexMetaData::NONE) {
            if rg_rows > 0 {
                selectors.push(RowSelector::select(rg_rows));
            }
            continue;
        }

        saw_usable_index = true;
        let locations = offset_index.page_locations();
        let page_count = locations.len();
        pages_total = pages_total.saturating_add(page_count);

        let mut cursor = 0usize;
        for page_idx in 0..page_count {
            let start = usize::try_from(locations[page_idx].first_row_index)
                .map_err(|error| error.to_string())?;
            let end = if page_idx + 1 < page_count {
                usize::try_from(locations[page_idx + 1].first_row_index)
                    .map_err(|error| error.to_string())?
            } else {
                rg_rows
            };
            if end < start || end > rg_rows {
                return Err(format!(
                    "invalid page row span in row group {rg_index} page {page_idx}: {start}..{end} (rg_rows={rg_rows})"
                ));
            }
            let len = end - start;
            if len == 0 {
                continue;
            }
            if cursor < start {
                selectors.push(RowSelector::skip(start - cursor));
            }
            if page_may_contain_equality_values(page_index, page_idx, values) {
                selectors.push(RowSelector::select(len));
                pages_selected += 1;
            } else {
                selectors.push(RowSelector::skip(len));
                pages_skipped += 1;
            }
            cursor = end;
        }
        if cursor < rg_rows {
            selectors.push(RowSelector::skip(rg_rows - cursor));
        }
    }

    if !saw_usable_index {
        return Ok(PagePruneDecision::absent());
    }

    Ok(PagePruneDecision {
        selection: Some(RowSelection::from(selectors)),
        mode: PageIndexPruneMode::Applied,
        pages_total,
        pages_selected,
        pages_skipped,
    })
}

fn page_may_contain_equality_values(
    page_index: &ColumnIndexMetaData,
    page_idx: usize,
    values: &[String],
) -> bool {
    if page_index.is_null_page(page_idx) {
        return false;
    }
    match page_index {
        ColumnIndexMetaData::NONE => true,
        ColumnIndexMetaData::INT64(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<i64>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        ColumnIndexMetaData::INT32(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<i32>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        ColumnIndexMetaData::BYTE_ARRAY(index)
        | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                let bytes = value.as_bytes();
                bytes >= min && bytes <= max
            })
        }
        ColumnIndexMetaData::BOOLEAN(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                value.parse::<bool>().is_ok_and(|parsed| {
                    let parsed = u8::from(parsed);
                    parsed >= u8::from(*min) && parsed <= u8::from(*max)
                })
            })
        }
        ColumnIndexMetaData::FLOAT(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<f32>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        ColumnIndexMetaData::DOUBLE(index) => {
            let (Some(min), Some(max)) = (index.min_value(page_idx), index.max_value(page_idx))
            else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<f64>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        ColumnIndexMetaData::INT96(_) => true,
    }
}
