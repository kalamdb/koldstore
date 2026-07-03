.PHONY: fmt lint test pgrx-test e2e benchmarks memory verify

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

pgrx-test:
	cargo pgrx test

e2e:
	tests/e2e/run_pg_matrix.sh

benchmarks:
	cargo run -p pg-koldstore-benchmarks -- --suite all

memory:
	tests/memory/run_memory_checks.sh

verify: fmt lint test e2e memory benchmarks

