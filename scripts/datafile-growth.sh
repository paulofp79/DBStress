#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"

CONNECT_STRING="${ORACLE_CONNECT_STRING:-}"
INCREMENT_MB="100"
BATCH_SIZE="1"
INTERVAL_SECONDS="60"
DURATION_SECONDS="0"
MAX_SIZE_GB="31"
PROMPT_BEFORE_RUN="true"

STOP_REQUESTED="false"

FILE_IDS=()
TABLESPACES=()
FILE_NAMES=()
SIZE_MBS=()

usage() {
  cat <<'EOF'
Usage:
  scripts/datafile-growth.sh list --connect USER/PASSWORD@DB [options]
  scripts/datafile-growth.sh once --connect USER/PASSWORD@DB [options]
  scripts/datafile-growth.sh run  --connect USER/PASSWORD@DB [options]

Connection:
  --connect VALUE                 SQL*Plus connect string. Can also use ORACLE_CONNECT_STRING.

Resize options:
  --increment-mb NUMBER           Resize amount per datafile. Default: 100
  --batch-size NUMBER             Number of datafiles to resize per cycle. Default: 1
  --interval-seconds NUMBER       Seconds between cycles for run mode. Default: 60
  --duration-seconds NUMBER       0 means run until Ctrl-C. Default: 0
  --max-size-gb NUMBER            Only process datafiles smaller than this size. Default: 31
  --yes                           Skip confirmation prompt for once/run

Behavior:
  - The script connects with SQL*Plus using the supplied connect string.
  - It discovers all permanent datafiles smaller than <max-size-gb>.
  - In each cycle it resizes the next <batch-size> eligible datafiles by <increment-mb>.
  - On the next interval it moves to the next batch, wrapping when needed.
  - It stops on Ctrl-C or when --duration-seconds is reached.

Examples:
  scripts/datafile-growth.sh list --connect system/oracle@dbhost:1521/ORCLPDB1
  scripts/datafile-growth.sh once --connect system/oracle@dbhost:1521/ORCLPDB1 \
    --increment-mb 512 --batch-size 8
  scripts/datafile-growth.sh run --connect system/oracle@dbhost:1521/ORCLPDB1 \
    --increment-mb 1024 --batch-size 20 --interval-seconds 300 --duration-seconds 7200 --yes
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

info() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
}

to_int() {
  local value="${1:-}"
  local min="$2"
  local max="$3"
  local fallback="$4"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    value="$fallback"
  fi

  if (( value < min )); then
    value="$min"
  elif (( value > max )); then
    value="$max"
  fi

  printf '%s' "$value"
}

require_sqlplus() {
  command -v sqlplus >/dev/null 2>&1 || die "sqlplus is required but was not found in PATH."
}

require_connection() {
  [[ -n "$CONNECT_STRING" ]] || die "Provide --connect USER/PASSWORD@DB or set ORACLE_CONNECT_STRING."
}

run_sql_quiet() {
  local sql="$1"
  require_sqlplus
  require_connection
  sqlplus -s "$CONNECT_STRING" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK OFF HEADING OFF PAGESIZE 0 LINESIZE 32767 TRIMSPOOL ON VERIFY OFF TERMOUT ON TAB OFF SERVEROUTPUT ON
$sql
EXIT
SQL
}

normalize_config() {
  INCREMENT_MB="$(to_int "$INCREMENT_MB" 1 1048576 100)"
  BATCH_SIZE="$(to_int "$BATCH_SIZE" 1 100000 1)"
  INTERVAL_SECONDS="$(to_int "$INTERVAL_SECONDS" 1 86400 60)"
  DURATION_SECONDS="$(to_int "$DURATION_SECONDS" 0 31536000 0)"
  MAX_SIZE_GB="$(to_int "$MAX_SIZE_GB" 1 1024 31)"
}

max_size_mb() {
  printf '%s' $(( MAX_SIZE_GB * 1024 ))
}

connect_summary() {
  local output
  output="$(run_sql_quiet "
SELECT
  USER || CHR(9) ||
  SYS_CONTEXT('USERENV','DB_NAME') || CHR(9) ||
  SYS_CONTEXT('USERENV','INSTANCE_NAME')
FROM dual;
")"
  output="$(printf '%s\n' "$output" | sed '/^[[:space:]]*$/d' | head -n 1)"
  [[ -n "$output" ]] || die "Connected, but could not read database identity."
  printf '%s' "$output"
}

query_datafiles_primary() {
  local limit_mb="$1"
  run_sql_quiet "
SELECT
  TO_CHAR(df.file_id) || CHR(9) ||
  df.tablespace_name || CHR(9) ||
  REPLACE(df.file_name, CHR(9), ' ') || CHR(9) ||
  TO_CHAR(CEIL(df.bytes / 1024 / 1024))
FROM dba_data_files df
WHERE CEIL(df.bytes / 1024 / 1024) < ${limit_mb}
ORDER BY df.tablespace_name, df.file_id;
"
}

query_datafiles_fallback() {
  local limit_mb="$1"
  run_sql_quiet "
SELECT
  TO_CHAR(df.file#) || CHR(9) ||
  ts.name || CHR(9) ||
  REPLACE(df.name, CHR(9), ' ') || CHR(9) ||
  TO_CHAR(CEIL(df.bytes / 1024 / 1024))
FROM v\$datafile df
JOIN v\$tablespace ts
  ON ts.ts# = df.ts#
WHERE CEIL(df.bytes / 1024 / 1024) < ${limit_mb}
ORDER BY ts.name, df.file#;
"
}

load_eligible_datafiles() {
  local limit_mb="$1"
  local output

  FILE_IDS=()
  TABLESPACES=()
  FILE_NAMES=()
  SIZE_MBS=()

  if ! output="$(query_datafiles_primary "$limit_mb" 2>&1)"; then
    output="$(query_datafiles_fallback "$limit_mb" 2>&1)" || die "Unable to read eligible datafiles. ${output}"
  fi

  while IFS=$'\t' read -r file_id tablespace file_name size_mb; do
    [[ -z "${file_id:-}" ]] && continue
    FILE_IDS+=("$file_id")
    TABLESPACES+=("$tablespace")
    FILE_NAMES+=("$file_name")
    SIZE_MBS+=("$size_mb")
  done < <(printf '%s\n' "$output")
}

print_datafile_table() {
  local count="${#FILE_IDS[@]}"
  local limit="${1:-$count}"
  local display_count="$count"

  if (( display_count > limit )); then
    display_count="$limit"
  fi

  if (( count == 0 )); then
    info "No eligible datafiles found below $(max_size_mb) MB (${MAX_SIZE_GB} GB)."
    return
  fi

  printf '%-6s %-30s %-10s %s\n' "FILE#" "TABLESPACE" "SIZE_MB" "FILE_NAME"
  printf '%-6s %-30s %-10s %s\n' "-----" "------------------------------" "----------" "---------"
  local index
  for ((index = 0; index < display_count; index += 1)); do
    printf '%-6s %-30s %-10s %s\n' \
      "${FILE_IDS[$index]}" \
      "${TABLESPACES[$index]}" \
      "${SIZE_MBS[$index]}" \
      "${FILE_NAMES[$index]}"
  done

  if (( count > display_count )); then
    info "Showing first ${display_count} of ${count} eligible datafiles."
  else
    info "Showing all ${count} eligible datafiles."
  fi
}

confirm_run() {
  [[ "$PROMPT_BEFORE_RUN" == "false" ]] && return 0

  local mode_label="$1"
  echo
  echo "${mode_label} will resize up to ${BATCH_SIZE} datafile(s) by ${INCREMENT_MB} MB each."
  if [[ "$mode_label" == "Run mode" ]]; then
    if (( DURATION_SECONDS > 0 )); then
      echo "It will repeat every ${INTERVAL_SECONDS} second(s) for ${DURATION_SECONDS} second(s)."
    else
      echo "It will repeat every ${INTERVAL_SECONDS} second(s) until you stop it with Ctrl-C."
    fi
  fi
  printf 'Proceed? [y/N] '
  local reply
  read -r reply || true
  case "${reply,,}" in
    y|yes) ;;
    *) die "Cancelled by user." ;;
  esac
}

resize_one_datafile() {
  local file_name="$1"
  local target_mb="$2"
  local escaped_file_name
  local output

  escaped_file_name="$(printf '%s' "$file_name" | sed "s/'/''/g")"

  if output="$(run_sql_quiet "ALTER DATABASE DATAFILE '${escaped_file_name}' RESIZE ${target_mb}M;" 2>&1)"; then
    return 0
  fi

  printf '%s' "$output"
  return 1
}

sleep_with_stop_check() {
  local remaining="$1"
  while (( remaining > 0 )); do
    [[ "$STOP_REQUESTED" == "true" ]] && return 1
    sleep 1
    remaining=$(( remaining - 1 ))
  done
  return 0
}

select_batch_indexes() {
  local count="$1"
  local cursor="$2"
  local batch_limit="$3"
  local selected=()
  local max_items="$batch_limit"
  local idx

  if (( max_items > count )); then
    max_items="$count"
  fi

  for ((idx = 0; idx < max_items; idx += 1)); do
    selected+=($(( (cursor + idx) % count )))
  done

  printf '%s\n' "${selected[@]}"
}

run_one_cycle() {
  local cursor="$1"
  local limit_mb
  local selected_indexes=()
  local selected_count
  local count
  local index
  local current_mb
  local target_mb
  local cycle_errors=0
  local next_cursor
  local command_output

  limit_mb="$(max_size_mb)"
  load_eligible_datafiles "$limit_mb"
  count="${#FILE_IDS[@]}"
  if (( count == 0 )); then
    info "All datafiles are already at or above ${MAX_SIZE_GB} GB."
    printf 'DONE\t%s\n' "$cursor"
    return 0
  fi

  while IFS= read -r index; do
    [[ -n "$index" ]] && selected_indexes+=("$index")
  done < <(select_batch_indexes "$count" "$cursor" "$BATCH_SIZE")

  selected_count="${#selected_indexes[@]}"
  info "Processing ${selected_count} datafile(s) out of ${count} eligible."

  for index in "${selected_indexes[@]}"; do
    current_mb="${SIZE_MBS[$index]}"
    target_mb=$(( current_mb + INCREMENT_MB ))
    if (( target_mb > limit_mb )); then
      target_mb="$limit_mb"
    fi

    if (( target_mb <= current_mb )); then
      info "Skipping file# ${FILE_IDS[$index]} (${FILE_NAMES[$index]}) because it is already at ${current_mb} MB."
      continue
    fi

    info "Resizing file# ${FILE_IDS[$index]} (${TABLESPACES[$index]}) from ${current_mb} MB to ${target_mb} MB"
    if ! command_output="$(resize_one_datafile "${FILE_NAMES[$index]}" "$target_mb" 2>&1)"; then
      cycle_errors=$(( cycle_errors + 1 ))
      info "Resize failed for file# ${FILE_IDS[$index]}: ${command_output}"
    fi
  done

  next_cursor=$(( (cursor + selected_count) % count ))
  printf 'NEXT\t%s\t%s\n' "$next_cursor" "$cycle_errors"
}

handle_signal() {
  STOP_REQUESTED="true"
  info "Stop requested. Finishing the current cycle before exit."
}

main() {
  local command="${1:-}"
  local cursor=0
  local start_epoch end_epoch now_epoch
  local cycle_result status next_cursor cycle_errors
  local connect_info connect_user connect_db connect_instance

  [[ -n "$command" ]] || {
    usage
    exit 1
  }

  if [[ "$command" == "--help" || "$command" == "-h" ]]; then
    usage
    exit 0
  fi
  shift

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --connect)
        CONNECT_STRING="${2:-}"
        shift 2
        ;;
      --increment-mb)
        INCREMENT_MB="${2:-}"
        shift 2
        ;;
      --batch-size)
        BATCH_SIZE="${2:-}"
        shift 2
        ;;
      --interval-seconds)
        INTERVAL_SECONDS="${2:-}"
        shift 2
        ;;
      --duration-seconds)
        DURATION_SECONDS="${2:-}"
        shift 2
        ;;
      --max-size-gb)
        MAX_SIZE_GB="${2:-}"
        shift 2
        ;;
      --yes)
        PROMPT_BEFORE_RUN="false"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1"
        ;;
    esac
  done

  case "$command" in
    list|once|run) ;;
    *)
      die "Unknown command '$command'. Use list, once, or run."
      ;;
  esac

  normalize_config
  require_connection
  require_sqlplus

  connect_info="$(connect_summary)"
  IFS=$'\t' read -r connect_user connect_db connect_instance <<<"$connect_info"
  info "Connected as ${connect_user} to ${connect_db} (${connect_instance})"

  if [[ "$command" == "list" ]]; then
    load_eligible_datafiles "$(max_size_mb)"
    print_datafile_table "${#FILE_IDS[@]}"
    return 0
  fi

  load_eligible_datafiles "$(max_size_mb)"
  print_datafile_table 50
  if (( ${#FILE_IDS[@]} == 0 )); then
    return 0
  fi

  confirm_run "$([[ "$command" == "once" ]] && printf 'One-time resize' || printf 'Run mode')"

  trap handle_signal INT TERM

  if [[ "$command" == "once" ]]; then
    cycle_result="$(run_one_cycle "$cursor")"
    IFS=$'\t' read -r status next_cursor cycle_errors <<<"$cycle_result"
    [[ "$status" == "DONE" ]] && return 0
    info "Cycle complete. Resize errors: ${cycle_errors}"
    return 0
  fi

  start_epoch="$(date +%s)"
  if (( DURATION_SECONDS > 0 )); then
    end_epoch=$(( start_epoch + DURATION_SECONDS ))
    info "Run will stop after ${DURATION_SECONDS} second(s)."
  else
    end_epoch=0
    info "Run will continue until you stop the script."
  fi

  while true; do
    cycle_result="$(run_one_cycle "$cursor")"
    IFS=$'\t' read -r status next_cursor cycle_errors <<<"$cycle_result"
    if [[ "$status" == "DONE" ]]; then
      break
    fi

    cursor="$next_cursor"
    info "Cycle complete. Resize errors: ${cycle_errors}. Next cursor: ${cursor}"

    [[ "$STOP_REQUESTED" == "true" ]] && break

    if (( end_epoch > 0 )); then
      now_epoch="$(date +%s)"
      if (( now_epoch >= end_epoch )); then
        info "Reached requested duration."
        break
      fi
      if (( now_epoch + INTERVAL_SECONDS > end_epoch )); then
        if ! sleep_with_stop_check "$(( end_epoch - now_epoch ))"; then
          break
        fi
        info "Reached requested duration."
        break
      fi
    fi

    if ! sleep_with_stop_check "$INTERVAL_SECONDS"; then
      break
    fi
  done

  info "Script finished."
}

main "$@"
