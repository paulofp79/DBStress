#!/usr/bin/env bash
#
# Controlled Oracle RAC CPU workload using SQL*Plus.
#
# The SQL executed by each worker is read-only but intentionally CPU-heavy.
# Use dedicated instance services for each --target when spreading the load
# across RAC nodes. This script never creates, modifies, or drops objects.

set -u
set -o pipefail

DURATION_SECONDS=60
SESSIONS_PER_TARGET=4
WORK_UNITS=1000
PAYLOAD_BYTES=1024
ACKNOWLEDGED=false
STOP_REQUESTED=false
COMPLETED_SUCCESSFULLY=false
TARGET_NAMES=()
TARGET_CONNECTIONS=()
PIDS=()

usage() {
  cat <<'EOF'
Controlled Oracle RAC CPU workload (SQL*Plus, read-only)

Required environment variables:
  ORACLE_USER and ORACLE_PASSWORD

Required options:
  --target NAME=CONNECT_IDENTIFIER  Repeat for each RAC instance service
  --i-understand-this-can-saturate-cpu

Optional:
  --sessions-per-target N           Concurrent sessions per target (default: 4)
  --duration-seconds N              Finite run duration (default: 60)
  --work-units N                    Hash operations per SQL call (default: 1000)
  --payload-bytes N                 Bytes hashed per operation, 1-32767 (default: 1024)

Example:
  export ORACLE_USER=lab
  export ORACLE_PASSWORD='your-password'
  ./scripts/rac-cpu-burn.sh \
    --target rac1='rac-node-1:1521/cpu_service_1' \
    --target rac2='rac-node-2:1521/cpu_service_2' \
    --sessions-per-target 32 --duration-seconds 300 \
    --i-understand-this-can-saturate-cpu

Sessions are tagged MODULE=DBSTRESS_RAC_CPU and ACTION=<target>-<worker>.
Press Ctrl-C to terminate SQL*Plus workers and disconnect their sessions.
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

positive_integer() {
  local value="$1"
  local option="$2"
  local maximum="${3:-}"
  [[ "$value" =~ ^[0-9]+$ ]] && (( value >= 1 )) || die "$option must be a positive integer"
  if [[ -n "$maximum" ]] && (( value > maximum )); then
    die "$option must not exceed $maximum"
  fi
}

require_sqlplus() {
  command -v sqlplus >/dev/null 2>&1 || die "sqlplus is required but was not found in PATH"
}

parse_target() {
  local target="$1"
  local name="${target%%=*}"
  local connection="${target#*=}"
  [[ "$target" == *=* && -n "$name" && -n "$connection" ]] || die "--target must be NAME=CONNECT_IDENTIFIER"
  [[ "$name" =~ ^[A-Za-z0-9_-]+$ ]] || die "Target names may contain only letters, digits, underscores, and hyphens"
  TARGET_NAMES+=("$name")
  TARGET_CONNECTIONS+=("$connection")
}

while (( $# > 0 )); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --i-understand-this-can-saturate-cpu) ACKNOWLEDGED=true; shift ;;
    --target) (( $# >= 2 )) || die "Missing value for --target"; parse_target "$2"; shift 2 ;;
    --sessions-per-target) (( $# >= 2 )) || die "Missing value for --sessions-per-target"; positive_integer "$2" "$1"; SESSIONS_PER_TARGET="$2"; shift 2 ;;
    --duration-seconds) (( $# >= 2 )) || die "Missing value for --duration-seconds"; positive_integer "$2" "$1" 86400; DURATION_SECONDS="$2"; shift 2 ;;
    --work-units) (( $# >= 2 )) || die "Missing value for --work-units"; positive_integer "$2" "$1" 1000000; WORK_UNITS="$2"; shift 2 ;;
    --payload-bytes) (( $# >= 2 )) || die "Missing value for --payload-bytes"; positive_integer "$2" "$1" 32767; PAYLOAD_BYTES="$2"; shift 2 ;;
    *) die "Unknown option: $1" ;;
  esac
done

[[ "$ACKNOWLEDGED" == true ]] || die "Add --i-understand-this-can-saturate-cpu to run this workload"
(( ${#TARGET_NAMES[@]} > 0 )) || die "Specify at least one --target"
[[ -n "${ORACLE_USER:-}" && -n "${ORACLE_PASSWORD:-}" ]] || die "Set ORACLE_USER and ORACLE_PASSWORD before running"
require_sqlplus

run_directory="$(mktemp -d "${TMPDIR:-/tmp}/dbstress-rac-cpu.XXXXXX")"

cleanup() {
  local pid
  if [[ "$STOP_REQUESTED" == false ]]; then
    STOP_REQUESTED=true
    echo
    echo "Stop requested; terminating SQL*Plus workers..."
  fi
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
}

on_exit() {
  if [[ "$COMPLETED_SUCCESSFULLY" == true ]]; then
    rm -rf "$run_directory"
  else
    cleanup
    echo "Worker logs were preserved in ${run_directory}." >&2
  fi
}

trap cleanup INT TERM
trap on_exit EXIT

run_worker() {
  local target_name="$1"
  local connect_identifier="$2"
  local worker_number="$3"
  local action="${target_name}-${worker_number}"
  action="${action:0:32}"
  local log_file="$run_directory/${action}.log"

  sqlplus -s /nolog >"$log_file" 2>&1 <<SQL
whenever oserror exit failure rollback
whenever sqlerror exit failure rollback
connect ${ORACLE_USER}/"${ORACLE_PASSWORD}"@${connect_identifier}
set feedback off heading off pagesize 0 verify off serveroutput on
declare
  l_stop_at timestamp with time zone := systimestamp + numtodsinterval(${DURATION_SECONDS}, 'SECOND');
  l_hashes   pls_integer;
  l_hash     raw(64);
begin
  dbms_application_info.set_module('DBSTRESS_RAC_CPU', '${action}');
  while systimestamp < l_stop_at loop
    select count(*), max(hash_value)
      into l_hashes, l_hash
      from (
        select standard_hash(
                 rpad(to_char(level) || rawtohex(sys_guid()), ${PAYLOAD_BYTES}, 'x'),
                 'SHA512'
               ) as hash_value
          from dual
        connect by level <= ${WORK_UNITS}
      );
  end loop;
  dbms_application_info.set_module(null, null);
end;
/
exit success
SQL
}

total_sessions=$(( ${#TARGET_NAMES[@]} * SESSIONS_PER_TARGET ))
echo "Starting ${total_sessions} CPU workers for ${DURATION_SECONDS}s across ${#TARGET_NAMES[@]} target(s)."
echo "Use Ctrl-C to stop. Monitor tagged sessions with MODULE = DBSTRESS_RAC_CPU."

for target_index in "${!TARGET_NAMES[@]}"; do
  for (( worker_number = 1; worker_number <= SESSIONS_PER_TARGET; worker_number += 1 )); do
    run_worker "${TARGET_NAMES[$target_index]}" "${TARGET_CONNECTIONS[$target_index]}" "$worker_number" &
    PIDS+=("$!")
  done
done

failures=0
for pid in "${PIDS[@]}"; do
  if ! wait "$pid"; then
    failures=$(( failures + 1 ))
  fi
done

if (( failures > 0 )); then
  echo "${failures} worker(s) failed. Worker logs were written to ${run_directory}." >&2
  exit 1
fi

COMPLETED_SUCCESSFULLY=true
echo "Finished successfully."
