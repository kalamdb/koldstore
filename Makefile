.PHONY: fmt lint test pgrx-test pgrx-test-matrix e2e benchmarks memory verify

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets --no-default-features -- -D warnings

test:
	cargo test

pgrx-test:
	cargo clippy -p pg_koldstore --all-targets --no-default-features --features pg16 -- -D warnings
	cargo pgrx install -p pg_koldstore --no-default-features --features pg16 --pg-config "$$(cargo pgrx info pg-config 16)"

pgrx-test-matrix:
	for pg in 15 16 17; do \
		cargo clippy -p pg_koldstore --all-targets --no-default-features --features pg$$pg -- -D warnings; \
		cargo pgrx install -p pg_koldstore --no-default-features --features pg$$pg --pg-config "$$(cargo pgrx info pg-config $$pg)"; \
	done

e2e:
	tests/e2e/run_pg_matrix.sh

benchmarks:
	cargo run -p pg-koldstore-benchmarks -- --rows 1000 --clients 2 --jobs 2 --seconds 1

memory:
	tests/memory/run_memory_checks.sh

verify: fmt lint test pgrx-test e2e memory benchmarks

