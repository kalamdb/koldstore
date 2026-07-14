# HammerDB schema build only. Placeholders filled by scripts/hammerdb/run.sh.
# Build VU must be <= warehouse count.

puts "SETTING CONFIGURATION"
dbset db pg
dbset bm TPC-C

diset connection pg_host {{PG_HOST}}
diset connection pg_port {{PG_PORT}}
diset connection pg_sslmode disable

diset tpcc pg_count_ware {{WAREHOUSES}}
diset tpcc pg_num_vu {{BUILD_VU}}
diset tpcc pg_superuser {{PG_USER}}
diset tpcc pg_superuserpass {{PG_PASSWORD}}
diset tpcc pg_defaultdbase {{PG_DATABASE}}
diset tpcc pg_user {{PG_USER}}
diset tpcc pg_pass {{PG_PASSWORD}}
diset tpcc pg_dbase {{PG_DATABASE}}

print dict
puts "BUILDING SCHEMA"
buildschema
puts "SCHEMA BUILD COMPLETE"
