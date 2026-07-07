Tasks:
- crates/koldstore-core - rename it to common and inside of it models so it have the shared models and enums
- the koldstore-mirror should have create/dml/cleanup/select/stats as a separate files to be easy to be read and understood
- we should divide the crate pg_kold
crates/pg_koldstore/src/sql/ddl.rs
crates/pg_koldstore/src/sql/dml.rs
crates/pg_koldstore/src/sql/ops.rs
into another crates if needed and check if we even need that long files

- everything which relates to the storage should be moved to the storage crate
- everything which belong to the parquert read/write should be moved to the parquet crate
- everything which belong to the koldstore-catalog should be moved to the catalog crate

- there is more things we can move to the commo crate which is scattered around the codebase
- the source crates/pg_koldstore/src/catalog i guess need to be moved to the catalog crate


- Check the explain to show if the manifest is read from hot table or from object store json if it didnt existed there, we already have a table which stores segments there.
i think it would be good to add to the explain:
-- reading the manifest's segment from the hot table
-- reading the manifest from object store
-- deserializing the manifest
to be included in the explain

- We should add more e2e tests which covers the explain/count/joining with 2 tables which has cold rows

