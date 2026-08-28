#!/usr/bin/env bash
set -euo pipefail

# Run Swingbench's SOE data-duplication workflow from DBStress.  SBUtil owns
# the SOE-specific key remapping, index/constraint rebuild, metadata refresh,
# and sequence updates, so this wrapper deliberately invokes it rather than
# attempting a generic INSERT ... SELECT that would corrupt SOE relationships.

SCRIPT_NAME="$(basename "$0")"
ACTION="grow"
SBUTIL_BIN="${SBUTIL_BIN:-}"
SWINGBENCH_HOME="${SWINGBENCH_HOME:-}"
CONNECT_STRING="${ORACLE_CONNECT_STRING:-}"
DB_USER="${ORACLE_DB_USER:-}"
DB_PASSWORD="${ORACLE_DB_PASSWORD:-}"
DUPLICATES="1"
PARALLEL_DEGREE="16"
SEED_SCALE="5"
THREAD_COUNT="4"
TABLESPACE=""
DBA_USER=""
DBA_PASSWORD=""
HASH_PARTITION="false"
ASYNC_OFF="false"
NO_INDEXES="false"
LOG_FILE=""
DRY_RUN="false"

usage() {
  cat <<'EOF'
Grow an existing Swingbench SOE schema using SBUtil's fast, database-side
parallel direct-operation workflow.

Usage:
  scripts/soe-grow.sh [create|grow|create-grow] --swingbench-home PATH --connect CONNECT --user USER [options]

Required:
  --connect CONNECT              JDBC/Swingbench connect string, e.g. //host/service
  --user USER                    SOE schema owner
  --password PASSWORD            SOE password; alternatively set ORACLE_DB_PASSWORD
  --swingbench-home PATH         Swingbench installation root containing bin/oewizard and bin/sbutil
                                 Alternatively set SWINGBENCH_HOME or SBUTIL_BIN.

Create options (required for create and create-grow):
  --dba-user USER                DBA account used by oewizard to create the SOE user
  --dba-password PASSWORD        DBA password used by oewizard
  --tablespace NAME              Target SOE tablespace
  --seed-scale NUMBER            oewizard seed scale. Default: 5
  --threads NUMBER               oewizard generation threads. Default: 4
  --hashpart true|false          Request SOE hash partitioning. Default: false
  --async-off true|false         Disable asynchronous data generation. Default: false
  --no-indexes true|false        Skip initial indexes; grow will build them afterward. Default: false

Growth options:
  --duplicates NUMBER            Final size multiplier. Default: 1
  --parallel-degree NUMBER       SBUtil parallel degree. Default: 16
  --log-file PATH                Save SBUtil output to this file as well as stdout
  --dry-run                      Print the command without running it

Examples:
  export ORACLE_DB_PASSWORD='your-password'
  scripts/soe-grow.sh create-grow --swingbench-home /opt/swingbench \
    --connect '//dbhost.example.com/service' --user soe \
    --dba-user 'sys as sysdba' --dba-password 'dba-password' --tablespace SOETBS \
    --seed-scale 5 --threads 4 --duplicates 10 --parallel-degree 64 --log-file soe-grow.log

Notes:
  create runs oewizard only. grow runs SBUtil only. create-grow runs oewizard
  followed by SBUtil. SBUtil -dup N grows the schema to N times its original data volume; N=1 is
  therefore a no-op. It temporarily removes and recreates SOE indexes and
  constraints, then refreshes metadata and sequences. Run against an existing
  SOE schema only, and ensure enough data, index, TEMP, and undo space first.
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

to_bool() {
  case "${1,,}" in
    true|yes|y|1|on) printf '%s' "true" ;;
    false|no|n|0|off) printf '%s' "false" ;;
    *) die "Expected true or false, got '$1'." ;;
  esac
}

resolve_sbutil() {
  if [[ -n "$SBUTIL_BIN" ]]; then
    [[ -x "$SBUTIL_BIN" ]] || die "SBUTIL_BIN is not executable: $SBUTIL_BIN"
    return
  fi

  if [[ -n "$SWINGBENCH_HOME" ]]; then
    SBUTIL_BIN="${SWINGBENCH_HOME%/}/bin/sbutil"
    [[ -x "$SBUTIL_BIN" ]] || die "No executable sbutil found at $SBUTIL_BIN"
    return
  fi

  SBUTIL_BIN="$(command -v sbutil || true)"
  [[ -n "$SBUTIL_BIN" ]] || die "Provide --swingbench-home PATH, set SBUTIL_BIN, or put sbutil on PATH."
}

resolve_oewizard() {
  [[ -n "$SWINGBENCH_HOME" ]] || die "--swingbench-home is required for create and create-grow."
  OEWIZARD_BIN="${SWINGBENCH_HOME%/}/bin/oewizard"
  [[ -x "$OEWIZARD_BIN" ]] || die "No executable oewizard found at $OEWIZARD_BIN"
}

parse_args() {
  case "${1:-}" in
    create|grow|create-grow) ACTION="$1"; shift ;;
  esac
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --swingbench-home) SWINGBENCH_HOME="${2:-}"; shift 2 ;;
      --sbutil) SBUTIL_BIN="${2:-}"; shift 2 ;;
      --connect) CONNECT_STRING="${2:-}"; shift 2 ;;
      --user) DB_USER="${2:-}"; shift 2 ;;
      --password) DB_PASSWORD="${2:-}"; shift 2 ;;
      --duplicates|--dup) DUPLICATES="${2:-}"; shift 2 ;;
      --parallel-degree|--parallel) PARALLEL_DEGREE="${2:-}"; shift 2 ;;
      --dba-user) DBA_USER="${2:-}"; shift 2 ;;
      --dba-password) DBA_PASSWORD="${2:-}"; shift 2 ;;
      --tablespace) TABLESPACE="${2:-}"; shift 2 ;;
      --seed-scale|--scale) SEED_SCALE="${2:-}"; shift 2 ;;
      --threads|--thread-count) THREAD_COUNT="${2:-}"; shift 2 ;;
      --hashpart) HASH_PARTITION="$(to_bool "${2:-}")"; shift 2 ;;
      --async-off) ASYNC_OFF="$(to_bool "${2:-}")"; shift 2 ;;
      --no-indexes) NO_INDEXES="$(to_bool "${2:-}")"; shift 2 ;;
      --log-file) LOG_FILE="${2:-}"; shift 2 ;;
      --dry-run) DRY_RUN="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option '$1'. Use --help for usage." ;;
    esac
  done
}

create_schema() {
  resolve_oewizard
  [[ -n "$DBA_USER" ]] || die "Provide --dba-user for ${ACTION}."
  [[ -n "$DBA_PASSWORD" ]] || die "Provide --dba-password for ${ACTION}."
  [[ -n "$TABLESPACE" ]] || die "Provide --tablespace for ${ACTION}."

  local command=("$OEWIZARD_BIN" -cl -create -cs "$CONNECT_STRING" -u "$DB_USER" -p "$DB_PASSWORD" -scale "$SEED_SCALE" -tc "$THREAD_COUNT" -dba "$DBA_USER" -dbap "$DBA_PASSWORD" -ts "$TABLESPACE")
  [[ "$HASH_PARTITION" == "true" ]] && command+=(-hashpart)
  [[ "$ASYNC_OFF" == "true" ]] && command+=(-async_off)
  [[ "$NO_INDEXES" == "true" ]] && command+=(-noindexes)

  echo "Creating SOE seed schema: scale=${SEED_SCALE}, threads=${THREAD_COUNT}, tablespace=${TABLESPACE}"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "Dry run: oewizard command validated; passwords intentionally omitted."
    return
  fi
  "${command[@]}"
}

grow_schema() {
  resolve_sbutil
  local command=("$SBUTIL_BIN" -soe -cs "$CONNECT_STRING" -u "$DB_USER" -p "$DB_PASSWORD" -dup "$DUPLICATES" -parallel "$PARALLEL_DEGREE")
  echo "SOE growth: multiplier=${DUPLICATES}, parallelDegree=${PARALLEL_DEGREE}"
  echo "Using SBUtil: $SBUTIL_BIN"
  echo "SBUtil will change the target SOE schema by dropping and rebuilding indexes/constraints."

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "Dry run: SBUtil command validated; password intentionally omitted."
    return
  fi

  if [[ -n "$LOG_FILE" ]]; then
    mkdir -p "$(dirname "$LOG_FILE")"
    "${command[@]}" 2>&1 | tee "$LOG_FILE"
  else
    "${command[@]}"
  fi
}

main() {
  parse_args "$@"
  [[ -n "$CONNECT_STRING" ]] || die "Provide --connect or set ORACLE_CONNECT_STRING."
  [[ -n "$DB_USER" ]] || die "Provide --user or set ORACLE_DB_USER."
  [[ -n "$DB_PASSWORD" ]] || die "Provide --password or set ORACLE_DB_PASSWORD."
  is_positive_integer "$SEED_SCALE" || die "--seed-scale must be a positive integer."
  is_positive_integer "$THREAD_COUNT" || die "--threads must be a positive integer."
  is_positive_integer "$DUPLICATES" || die "--duplicates must be a positive integer."
  is_positive_integer "$PARALLEL_DEGREE" || die "--parallel-degree must be a positive integer."

  case "$ACTION" in
    create) create_schema ;;
    grow) grow_schema ;;
    create-grow) create_schema; grow_schema ;;
  esac
}

main "$@"
