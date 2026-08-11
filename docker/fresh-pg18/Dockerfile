# syntax=docker/dockerfile:1
#
# Fresh Ubuntu 24.04 + PostgreSQL 18 demo image.
# koldstore is NOT baked in at build time — the entrypoint downloads the latest
# GitHub Release asset on first start (see install-koldstore.sh).

ARG PG_MAJOR=18

FROM ubuntu:24.04

ARG PG_MAJOR

ENV DEBIAN_FRONTEND=noninteractive \
    PG_MAJOR=${PG_MAJOR} \
    PATH=/usr/lib/postgresql/${PG_MAJOR}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PGDATA=/var/lib/postgresql/data \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

COPY scripts/ci/keys/ACCC4CF8.asc /tmp/ACCC4CF8.asc

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      gosu \
      locales \
      lsb-release \
      libnss-wrapper \
      python3 \
      tzdata; \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen; \
    locale-gen; \
    gpg --batch --yes --dearmor -o /usr/share/keyrings/postgresql.gpg /tmp/ACCC4CF8.asc; \
    rm -f /tmp/ACCC4CF8.asc; \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      "postgresql-${PG_MAJOR}" \
      "postgresql-client-${PG_MAJOR}"; \
    if command -v pg_dropcluster >/dev/null 2>&1; then \
      pg_dropcluster --stop "${PG_MAJOR}" main || true; \
    fi; \
    rm -rf /var/lib/postgresql/"${PG_MAJOR}" /var/lib/apt/lists/*; \
    mkdir -p /var/lib/postgresql/data /docker-entrypoint-initdb.d /var/run/postgresql; \
    chown -R postgres:postgres /var/lib/postgresql /var/run/postgresql; \
    chmod 2777 /var/run/postgresql

# Official docker-entrypoint (initdb, POSTGRES_* env, init scripts).
COPY --from=postgres:18-bookworm /usr/local/bin/docker-entrypoint.sh /usr/local/bin/
COPY --from=postgres:18-bookworm /usr/local/bin/docker-ensure-initdb.sh /usr/local/bin/

COPY install-koldstore.sh /usr/local/bin/install-koldstore.sh
COPY entrypoint.sh /usr/local/bin/fresh-pg18-entrypoint.sh
COPY initdb.d/ /docker-entrypoint-initdb.d/

RUN chmod +x /usr/local/bin/install-koldstore.sh /usr/local/bin/fresh-pg18-entrypoint.sh

STOPSIGNAL SIGINT
EXPOSE 5432
VOLUME /var/lib/postgresql/data

ENTRYPOINT ["fresh-pg18-entrypoint.sh"]
CMD ["postgres"]
