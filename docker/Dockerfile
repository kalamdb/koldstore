# syntax=docker/dockerfile:1

ARG PG_MAJOR=16

FROM rust:1.96-bookworm AS builder

ARG PG_MAJOR
ARG PGRX_VERSION=0.19.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      wget ca-certificates gnupg lsb-release \
      build-essential libssl-dev pkg-config clang llvm \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends "postgresql-server-dev-${PG_MAJOR}" \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked

WORKDIR /build
COPY . .

ENV CARGO_BUILD_JOBS=1 \
    CARGO_PROFILE_RELEASE_LTO=false \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16

RUN cargo pgrx init --pg"${PG_MAJOR}" "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" \
    && cargo pgrx package \
      -p pg_koldstore \
      --no-default-features \
      --features "pg${PG_MAJOR} s3" \
      --pg-config "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"

RUN mkdir -p /out/lib /out/extension \
    && SO_FILE="$(find "target/release/koldstore-pg${PG_MAJOR}" -type f -name '*.so' | head -n1)" \
    && test -n "${SO_FILE}" \
    && cp "${SO_FILE}" /out/lib/pg_koldstore.so \
    && cp "target/release/koldstore-pg${PG_MAJOR}/usr/share/postgresql/${PG_MAJOR}/extension/koldstore.control" /out/extension/koldstore.control \
    && cp "target/release/koldstore-pg${PG_MAJOR}/usr/share/postgresql/${PG_MAJOR}/extension/koldstore--0.1.0.sql" /out/extension/koldstore--0.1.0.sql

FROM postgres:${PG_MAJOR}-bookworm

ARG PG_MAJOR

COPY --from=builder /out/extension/ /tmp/koldstore-ext/
COPY --from=builder /out/lib/pg_koldstore.so /tmp/koldstore-lib/pg_koldstore.so
COPY docker/init/ /docker-entrypoint-initdb.d/

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && arch="$(dpkg --print-architecture)" \
    && case "${arch}" in \
      amd64) mc_arch="amd64" ;; \
      arm64) mc_arch="arm64" ;; \
      *) echo "unsupported architecture for mc: ${arch}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://dl.min.io/client/mc/release/linux-${mc_arch}/mc" -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc \
    && rm -rf /var/lib/apt/lists/*

RUN EXTDIR="$(pg_config --sharedir)/extension" \
    && LIBDIR="$(pg_config --pkglibdir)" \
    && install -d "${EXTDIR}" "${LIBDIR}" \
    && install -m 0644 /tmp/koldstore-ext/* "${EXTDIR}/" \
    && install -m 0755 /tmp/koldstore-lib/pg_koldstore.so "${LIBDIR}/" \
    && rm -rf /tmp/koldstore-ext /tmp/koldstore-lib
USER postgres
