#!/bin/bash
# Collect host hardware specs and insert one row into host_info table.

# 1) Read & validate CLI arguments
# Usage: ./scripts/host_info.sh psql_host psql_port db_name psql_user psql_password
psql_host=$1
psql_port=$2
db_name=$3
psql_user=$4
psql_password=$5

if [ "$#" -ne 5 ]; then
  echo "Illegal number of parameters"
  echo "Usage: ./scripts/host_info.sh psql_host psql_port db_name psql_user psql_password"
  exit 1
fi

# 2) Parse host hardware specifications
# TIP: Keep newlines by capturing command output in double quotes.
specs="$(lscpu)"

hostname="$(hostname -f)"
cpu_number="$(echo "$specs" | egrep '^CPU\(s\):'        | awk -F: '{print $2}' | xargs)"
cpu_architecture="$(echo "$specs" | egrep '^Architecture:' | awk -F: '{print $2}' | xargs)"
cpu_model="$(echo "$specs" | egrep '^Model name:'      | awk -F: '{print $2}' | xargs)"
cpu_mhz="$(
  awk -F: '/^cpu MHz/ { gsub(/^[ \t]+/, "", $2); sum += $2; n++ } END { if (n) printf "%.3f", sum/n }' /proc/cpuinfo
)"
# Strip non-digits to store L2 cache size as an integer (e.g., "256K" -> 256).
l2_cache="$(echo "$specs" | egrep '^L2 cache:' | awk -F: '{print $2}' | xargs | sed 's/[^0-9]//g')"

# Total memory from /proc/meminfo (in kB), which matches the table comment.
total_mem="$(grep -i '^MemTotal:' /proc/meminfo | awk '{print $2}')"

# UTC timestamp in the required format.
timestamp="$(date -u '+%Y-%m-%d %H:%M:%S')"

#  3) Build the INSERT statement
# NOTE: "timestamp" is quoted because it is a reserved word in SQL.
insert_stmt="
INSERT INTO host_info(
  hostname, cpu_number, cpu_architecture, cpu_model, cpu_mhz, l2_cache, total_mem, \"timestamp\"
) VALUES (
  '$hostname', $cpu_number, '$cpu_architecture', '$cpu_model', $cpu_mhz, $l2_cache, $total_mem, '$timestamp'
)
ON CONFLICT (hostname) DO NOTHING;
"

#  4) Execute via psql CLI
# TIP: Use psql -c "<SQL>" to run the statement from bash.
export PGPASSWORD="$psql_password"
psql -h "$psql_host" -p "$psql_port" -d "$db_name" -U "$psql_user" -c "$insert_stmt"
exit $?
