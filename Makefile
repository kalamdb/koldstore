.PHONY: fmt lint test pgrx-test pgrx-test-matrix e2e benchmarks memory verify

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets --no-default-features -- -D warnings

test:
	cargo nextest run --workspace --no-default-features \
		--exclude e2e --exclude examples --exclude storage-comparison \
		--exclude pg-koldstore-benchmarks --exclude koldstore-memory-tests

pgrx-test:
	cargo clippy -p pg_koldstore --all-targets --no-default-features --features "pg16 s3" -- -D warnings
	cargo pgrx install -p pg_koldstore --no-default-features --features "pg16 s3" --pg-config "$$(cargo pgrx info pg-config 16)"

pgrx-test-matrix:
	scripts/run-pgrx-matrix.sh --skip-unit --skip-e2e

e2e:
	scripts/run-pg-e2e.sh

benchmarks:
	cargo run -p pg-koldstore-benchmarks -- --rows 1000 --clients 2 --jobs 2 --seconds 1

memory:
	tests/memory/run_memory_checks.sh

verify:
	scripts/run-all-tests.sh

