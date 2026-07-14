# HammerDB timed TPROC-C run only. Placeholders filled by scripts/hammerdb/run.sh.
# Expects schema already present; KoldStore manage applied to HISTORY beforehand.

puts "SETTING CONFIGURATION"
dbset db pg
dbset bm TPC-C

diset connection pg_host {{PG_HOST}}
diset connection pg_port {{PG_PORT}}
diset connection pg_sslmode disable

diset tpcc pg_count_ware {{WAREHOUSES}}
diset tpcc pg_num_vu {{VIRTUAL_USERS}}
diset tpcc pg_superuser {{PG_USER}}
diset tpcc pg_superuserpass {{PG_PASSWORD}}
diset tpcc pg_defaultdbase {{PG_DATABASE}}
diset tpcc pg_user {{PG_USER}}
diset tpcc pg_pass {{PG_PASSWORD}}
diset tpcc pg_dbase {{PG_DATABASE}}
diset tpcc pg_driver timed
diset tpcc pg_rampup {{RAMPUP}}
diset tpcc pg_duration {{DURATION}}
diset tpcc pg_timeprofile true
diset tpcc pg_allwarehouse false
diset tpcc pg_vacuum false

print dict
vuset logtotemp 1
puts "RUNNING VIRTUAL USERS"
vuset vu {{VIRTUAL_USERS}}
loadscript
vucreate
vurun
vudestroy
puts "HAMMERDB RUN COMPLETE"
