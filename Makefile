.PHONY: fmt lint test pgrx-test pgrx-test-matrix e2e benchmarks memory verify

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

pgrx-test:
	cargo pgrx test pg16 -p pg_koldstore --no-default-features --features pg16

pgrx-test-matrix:
	for pg in 15 16 17; do \
		cargo pgrx test pg$$pg -p pg_koldstore --no-default-features --features pg$$pg; \
	done

e2e:
	tests/e2e/run_pg_matrix.sh

benchmarks:
	cargo run -p pg-koldstore-benchmarks -- --suite all

memory:
	tests/memory/run_memory_checks.sh

verify: fmt lint test pgrx-test e2e memory benchmarks

