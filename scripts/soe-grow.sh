#!/usr/bin/env bash
set -euo pipefail

# Run Swingbench's SOE data-duplication workflow from DBStress.  SBUtil owns
# the SOE-specific key remapping, index/constraint rebuild, metadata refresh,
# and sequence updates, so this wrapper deliberately invokes it rather than
# attempting a generic INSERT ... SELECT that would corrupt SOE relationships.

SCRIPT_NAME="$(basename "$0")"
SBUTIL_BIN="${SBUTIL_BIN:-}"
SWINGBENCH_HOME="${SWINGBENCH_HOME:-}"
CONNECT_STRING="${ORACLE_CONNECT_STRING:-}"
DB_USER="${ORACLE_DB_USER:-}"
DB_PASSWORD="${ORACLE_DB_PASSWORD:-}"
DUPLICATES="1"
PARALLEL_DEGREE="16"
LOG_FILE=""
DRY_RUN="false"

usage() {
  cat <<'EOF'
Grow an existing Swingbench SOE schema using SBUtil's fast, database-side
parallel direct-operation workflow.

Usage:
  scripts/soe-grow.sh --swingbench-home PATH --connect CONNECT --user USER [options]

Required:
  --connect CONNECT              JDBC/Swingbench connect string, e.g. //host/service
  --user USER                    SOE schema owner
  --password PASSWORD            SOE password; alternatively set ORACLE_DB_PASSWORD
  --swingbench-home PATH         Swingbench installation root containing bin/sbutil
                                 Alternatively set SWINGBENCH_HOME or SBUTIL_BIN.

Growth options:
  --duplicates NUMBER            Final size multiplier. Default: 1
  --parallel-degree NUMBER       SBUtil parallel degree. Default: 16
  --log-file PATH                Save SBUtil output to this file as well as stdout
  --dry-run                      Print the command without running it

Examples:
  export ORACLE_DB_PASSWORD='your-password'
  scripts/soe-grow.sh --swingbench-home /opt/swingbench \
    --connect '//dbhost.example.com/service' --user soe \
    --duplicates 10 --parallel-degree 64 --log-file soe-grow.log

Notes:
  SBUtil -dup N grows the schema to N times its original data volume; N=1 is
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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --swingbench-home) SWINGBENCH_HOME="${2:-}"; shift 2 ;;
      --sbutil) SBUTIL_BIN="${2:-}"; shift 2 ;;
      --connect) CONNECT_STRING="${2:-}"; shift 2 ;;
      --user) DB_USER="${2:-}"; shift 2 ;;
      --password) DB_PASSWORD="${2:-}"; shift 2 ;;
      --duplicates|--dup) DUPLICATES="${2:-}"; shift 2 ;;
      --parallel-degree|--parallel) PARALLEL_DEGREE="${2:-}"; shift 2 ;;
      --log-file) LOG_FILE="${2:-}"; shift 2 ;;
      --dry-run) DRY_RUN="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option '$1'. Use --help for usage." ;;
    esac
  done
}

main() {
  parse_args "$@"
  [[ -n "$CONNECT_STRING" ]] || die "Provide --connect or set ORACLE_CONNECT_STRING."
  [[ -n "$DB_USER" ]] || die "Provide --user or set ORACLE_DB_USER."
  [[ -n "$DB_PASSWORD" ]] || die "Provide --password or set ORACLE_DB_PASSWORD."
  is_positive_integer "$DUPLICATES" || die "--duplicates must be a positive integer."
  is_positive_integer "$PARALLEL_DEGREE" || die "--parallel-degree must be a positive integer."
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

main "$@"
