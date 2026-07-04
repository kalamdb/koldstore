#!/usr/bin/env bash
set -euo pipefail

MODE="${1:?usage: collect_system_stats.sh <mode> <phase> <output-json>}"
PHASE="${2:?usage: collect_system_stats.sh <mode> <phase> <output-json>}"
OUTPUT_JSON="${3:?usage: collect_system_stats.sh <mode> <phase> <output-json>}"

mkdir -p "$(dirname "$OUTPUT_JSON")"

postgres_pids=()
while IFS= read -r pid; do
  [[ -n "$pid" ]] && postgres_pids+=("$pid")
done < <(pgrep -f 'postgres' || true)

rss_kb=0
cpu_seconds="null"
if [[ "${#postgres_pids[@]}" -gt 0 ]]; then
  rss_kb="$(ps -o rss= -p "$(IFS=,; echo "${postgres_pids[*]}")" 2>/dev/null | awk '{ total += $1 } END { print total + 0 }')"
  cpu_seconds="$(ps -o time= -p "${postgres_pids[0]}" 2>/dev/null | awk -F: '
    NF == 3 { print ($1 * 3600) + ($2 * 60) + $3; next }
    NF == 2 { print ($1 * 60) + $2; next }
    { print "null" }
  ')"
fi

total_memory_bytes="null"
if [[ -r /proc/meminfo ]]; then
  total_memory_bytes="$(awk '/MemTotal:/ { print $2 * 1024 }' /proc/meminfo)"
elif command -v sysctl >/dev/null 2>&1; then
  total_memory_bytes="$(sysctl -n hw.memsize 2>/dev/null || echo null)"
fi

cat >"$OUTPUT_JSON" <<JSON
{
  "mode": "$MODE",
  "phase": "$PHASE",
  "collected_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "approx_postgres_rss_bytes": $((rss_kb * 1024)),
  "approx_postgres_cpu_seconds": $cpu_seconds,
  "total_memory_bytes": $total_memory_bytes,
  "measurement_note": "Approximate process RSS and CPU time from ps/pgrep; useful for trends, not absolute CI comparisons."
}
JSON
