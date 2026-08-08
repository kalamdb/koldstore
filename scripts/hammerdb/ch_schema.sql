-- CH-benCHmark extension tables for HammerDB TPROC-C (region / nation / supplier).
-- Compatible with the Citus citus-benchmark CH query set.
-- Variable: SUPPLIER_COUNT (default 10000 for non-smoke; smoke may pass 1000).
\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS region (
  r_regionkey int NOT NULL PRIMARY KEY,
  r_name char(55) NOT NULL,
  r_comment char(152) NOT NULL
);

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey int NOT NULL PRIMARY KEY,
  n_name char(25) NOT NULL,
  n_regionkey int NOT NULL,
  n_comment char(152) NOT NULL
);

CREATE TABLE IF NOT EXISTS supplier (
  su_suppkey int NOT NULL PRIMARY KEY,
  su_name char(25) NOT NULL,
  su_address varchar(40) NOT NULL,
  su_nationkey int NOT NULL,
  su_phone char(15) NOT NULL,
  su_acctbal numeric(12, 2) NOT NULL,
  su_comment char(101) NOT NULL
);

TRUNCATE supplier, nation, region;

-- Minimal CH-compatible geography (Europe + Germany/Cambodia required by several queries).
INSERT INTO region (r_regionkey, r_name, r_comment) VALUES
  (0, 'Africa', 'seed'),
  (1, 'America', 'seed'),
  (2, 'Asia', 'seed'),
  (3, 'Europe', 'seed'),
  (4, 'Middle East', 'seed');

INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES
  (0, 'Algeria', 0, 'seed'),
  (1, 'Argentina', 1, 'seed'),
  (2, 'Brazil', 1, 'seed'),
  (3, 'Canada', 1, 'seed'),
  (4, 'Egypt', 4, 'seed'),
  (5, 'Ethiopia', 0, 'seed'),
  (6, 'France', 3, 'seed'),
  (7, 'Germany', 3, 'Germany must be present for Q7/Q8/Q11/Q20/Q21'),
  (8, 'India', 2, 'seed'),
  (9, 'Indonesia', 2, 'seed'),
  (10, 'Iran', 4, 'seed'),
  (11, 'Iraq', 4, 'seed'),
  (12, 'Japan', 2, 'seed'),
  (13, 'Jordan', 4, 'seed'),
  (14, 'Kenya', 0, 'seed'),
  (15, 'Morocco', 0, 'seed'),
  (16, 'Mozambique', 0, 'seed'),
  (17, 'Peru', 1, 'seed'),
  (18, 'China', 2, 'seed'),
  (19, 'Romania', 3, 'seed'),
  (20, 'Saudi Arabia', 4, 'seed'),
  (21, 'Vietnam', 2, 'seed'),
  (22, 'Russia', 3, 'seed'),
  (23, 'United Kingdom', 3, 'seed'),
  (24, 'United States', 1, 'seed'),
  (67, 'Cambodia', 3, 'required for Q7 pairing with Germany');

-- CH queries join stock↔supplier via mod((s_w_id * s_i_id), 10000) = su_suppkey.
INSERT INTO supplier (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment)
SELECT
  g,
  'Supplier#' || lpad(g::text, 9, '0'),
  'addr-' || g,
  (g % 25),
  '00-000-000-0000',
  (g % 1000)::numeric / 10,
  CASE WHEN g % 17 = 0 THEN 'bad Customer complaints' ELSE 'ok' END
FROM generate_series(0, GREATEST((:'SUPPLIER_COUNT')::int - 1, 0)) AS g;
