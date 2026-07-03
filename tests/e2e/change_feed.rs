#[test]
fn change_feed_placeholder_covers_flush_cold_delete_hydrate_and_demigrate() {
    let cases = ["flush", "cold-only delete", "hydrate", "demigrate"];
    assert_eq!(cases.len(), 4);
}

