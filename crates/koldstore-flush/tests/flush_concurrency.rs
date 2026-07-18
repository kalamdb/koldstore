use std::thread;

use koldstore_common::{CommitSeq, SeqId, StablePkHash};
use koldstore_flush::job::{
    conditional_cleanup_allowed, FlushBatchBuilder, FlushBatchPush, FlushExecutionConfig,
    FlushWatermark, HotRowCandidate,
};
use koldstore_merge::dml::{delete_decision_with_flush_fence, DeleteDecision};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotState {
    Live,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotRowState {
    pk: u16,
    seq: SeqId,
    commit_seq: CommitSeq,
    state: HotState,
}

impl HotRowState {
    fn live(pk: u16, seq: i64, commit_seq: i64) -> Self {
        Self {
            pk,
            seq: SeqId::new(seq).unwrap(),
            commit_seq: CommitSeq::new(commit_seq).unwrap(),
            state: HotState::Live,
        }
    }

    fn tombstone(pk: u16, seq: i64, commit_seq: i64) -> Self {
        Self {
            pk,
            seq: SeqId::new(seq).unwrap(),
            commit_seq: CommitSeq::new(commit_seq).unwrap(),
            state: HotState::Tombstone,
        }
    }

    fn candidate(&self) -> HotRowCandidate {
        let hash = StablePkHash::from_hex(format!("{:04x}", self.pk)).unwrap();
        match self.state {
            HotState::Live => HotRowCandidate::live(hash, self.seq, self.commit_seq),
            HotState::Tombstone => HotRowCandidate::tombstone(hash, self.seq, self.commit_seq),
        }
    }
}

#[test]
fn mass_insert_update_delete_during_flush_only_removes_unchanged_watermarked_rows() {
    let watermark = FlushWatermark::new(SeqId::new(100).unwrap());
    let flushed = (1..=100).map(|pk| HotRowState::live(pk, i64::from(pk), 1000 + i64::from(pk)));

    let current_rows = flushed
        .clone()
        .map(|row| match row.pk {
            1..=20 => HotRowState::live(row.pk, 100 + i64::from(row.pk), 2000 + i64::from(row.pk)),
            21..=40 => {
                assert_eq!(
                    delete_decision_with_flush_fence(false, true),
                    DeleteDecision::Tombstone
                );
                HotRowState::tombstone(row.pk, 120 + i64::from(row.pk), 3000 + i64::from(row.pk))
            }
            _ => row,
        })
        .chain((101..=130).map(|pk| HotRowState::live(pk, i64::from(pk), 4000 + i64::from(pk))))
        .collect::<Vec<_>>();

    let removed = flushed
        .map(|candidate| {
            let current = current_rows
                .iter()
                .find(|row| row.pk == candidate.pk)
                .expect("current row exists for flushed candidate");
            (
                candidate.pk,
                conditional_cleanup_allowed(
                    &candidate.candidate(),
                    current.seq,
                    current.commit_seq,
                    watermark,
                ),
            )
        })
        .filter_map(|(pk, remove)| remove.then_some(pk))
        .collect::<Vec<_>>();

    assert_eq!(removed.len(), 60);
    assert_eq!(removed.first(), Some(&41));
    assert_eq!(removed.last(), Some(&100));

    assert!(current_rows
        .iter()
        .filter(|row| row.pk > 100)
        .all(|row| !watermark.includes(&row.candidate())));
}

#[test]
fn rollback_during_flush_does_not_block_cleanup_but_committed_update_does() {
    let watermark = FlushWatermark::new(SeqId::new(10).unwrap());
    let flushed = HotRowState::live(1, 10, 110).candidate();

    let rolled_back_update_visible_state = HotRowState::live(1, 10, 110);
    assert!(conditional_cleanup_allowed(
        &flushed,
        rolled_back_update_visible_state.seq,
        rolled_back_update_visible_state.commit_seq,
        watermark,
    ));

    let committed_update_visible_state = HotRowState::live(1, 11, 111);
    assert!(!conditional_cleanup_allowed(
        &flushed,
        committed_update_visible_state.seq,
        committed_update_visible_state.commit_seq,
        watermark,
    ));
}

#[test]
fn flush_batch_builder_keeps_mass_dml_memory_bounded() {
    let config = FlushExecutionConfig::new(512, 512 * 128, 8).unwrap();
    let mut builder = FlushBatchBuilder::new(config);
    let mut accepted = 0usize;

    for pk in 1..=10_000u16 {
        let row = HotRowState::live(pk, i64::from(pk), 10_000 + i64::from(pk)).candidate();
        match builder.push(row, 128) {
            FlushBatchPush::Accepted => accepted += 1,
            FlushBatchPush::Full => break,
        }
    }

    let batch = builder.finish();
    assert_eq!(accepted, 512);
    assert_eq!(batch.rows.len(), 512);
    assert_eq!(batch.batch_size, 512);
}

#[test]
fn multithreaded_cleanup_decisions_are_deterministic_for_same_flush_watermark() {
    let watermark = FlushWatermark::new(SeqId::new(1_000).unwrap());
    let flushed = (1..=1_000)
        .map(|pk| HotRowState::live(pk, i64::from(pk), 10_000 + i64::from(pk)))
        .collect::<Vec<_>>();
    let current = flushed
        .iter()
        .map(|row| {
            if row.pk % 3 == 0 {
                HotRowState::live(
                    row.pk,
                    1_000 + i64::from(row.pk),
                    20_000 + i64::from(row.pk),
                )
            } else if row.pk % 5 == 0 {
                HotRowState::tombstone(
                    row.pk,
                    2_000 + i64::from(row.pk),
                    30_000 + i64::from(row.pk),
                )
            } else {
                row.clone()
            }
        })
        .collect::<Vec<_>>();

    let expected = flushed
        .iter()
        .zip(current.iter())
        .filter(|(flushed, current)| {
            conditional_cleanup_allowed(
                &flushed.candidate(),
                current.seq,
                current.commit_seq,
                watermark,
            )
        })
        .count();

    let handles = (0..8)
        .map(|worker| {
            let flushed = flushed.clone();
            let current = current.clone();
            thread::spawn(move || {
                flushed
                    .iter()
                    .zip(current.iter())
                    .skip(worker)
                    .step_by(8)
                    .filter(|(flushed, current)| {
                        conditional_cleanup_allowed(
                            &flushed.candidate(),
                            current.seq,
                            current.commit_seq,
                            watermark,
                        )
                    })
                    .count()
            })
        })
        .collect::<Vec<_>>();

    let actual = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .sum::<usize>();

    assert_eq!(actual, expected);
    assert_eq!(actual, 533);
}

#[test]
fn delete_racing_with_flush_is_tombstoned_even_without_existing_cold_hint() {
    assert_eq!(
        delete_decision_with_flush_fence(false, true),
        DeleteDecision::Tombstone
    );
    assert_eq!(
        delete_decision_with_flush_fence(false, false),
        DeleteDecision::PhysicalDelete
    );
    assert_eq!(
        delete_decision_with_flush_fence(true, false),
        DeleteDecision::Tombstone
    );
}
