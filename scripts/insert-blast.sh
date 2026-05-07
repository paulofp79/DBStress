#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"

CONNECT_STRING="${ORACLE_CONNECT_STRING:-}"
TABLE_PREFIX="IBLAST"
TABLE_COUNT="8"
COLUMNS_PER_TABLE="24"
CREATE_TABLESPACES="false"
DROP_TABLESPACES="false"
USE_EXISTING_TABLESPACES="false"
TABLESPACE_PREFIX=""
TABLESPACE_INITIAL_MB="1024"
TABLESPACE_NEXT_MB="1024"
TABLESPACE_DATAFILE_LOCATION=""
TABLESPACE_ENCRYPTION_ENABLED="false"
TABLESPACE_ENCRYPTION_ALGORITHM="AES256"
WORKLOADS=()
HW_MITIGATION_ENABLED="false"
PREALLOCATE_ON_START="true"
EXTENT_SIZE_MB="128"
ALLOCATE_EVERY_INSERTS="100000"
MONITOR_INTERVAL="5"
MONITOR_ITERATIONS="0"
LOG_DIR="${INSERT_BLAST_LOG_DIR:-./insert-blast-logs}"

usage() {
  cat <<'EOF'
Usage:
  scripts/insert-blast.sh create  --connect USER/PASSWORD@DB [options]
  scripts/insert-blast.sh run     --connect USER/PASSWORD@DB [options]
  scripts/insert-blast.sh monitor --connect USER/PASSWORD@DB [options]
  scripts/insert-blast.sh status  --connect USER/PASSWORD@DB [options]
  scripts/insert-blast.sh drop    --connect USER/PASSWORD@DB [options]

Connection:
  --connect VALUE                         SQL*Plus connect string. Can also use ORACLE_CONNECT_STRING.

Schema options, matching Insert Blast defaults:
  --prefix VALUE                          Table prefix. Default: IBLAST
  --tables NUMBER                         Number of tables. Default: 8
  --columns NUMBER                        Insert payload columns per table. Default: 24
  --create-tablespaces true|false         Create one BIGFILE tablespace per table. Default: false
  --use-existing-tablespaces true|false   Place tables in existing tablespaces. Default: false
  --tablespace-prefix VALUE               Default: <prefix>_TS
  --tablespace-initial-mb NUMBER          Default: 1024
  --tablespace-next-mb NUMBER             Default: 1024
  --tablespace-datafile-location VALUE    Directory or ASM diskgroup for datafiles.
  --tablespace-encryption true|false      Create encrypted tablespaces. Default: false
  --tablespace-encryption-algorithm VALUE TDE algorithm. Default: AES256

Drop options:
  drop discovers matching <prefix>_T% tables from USER_TABLES; --tables is not required.
  --drop-tablespaces true|false           Also drop matching <tablespace-prefix>% tablespaces. Default: false

Workload options:
  --workload NAME:TABLES:SESSIONS:DURATION:COMMIT_EVERY:MODE[:OPERATION]
                                          Repeat for multiple workloads.
                                          MODE is reuse or reconnect.
                                          OPERATION is insert or select. Default: insert
                                          COMMIT_EVERY is ignored for select workloads.
                                          Default: Workload_1:<tables>:8:60:50:reuse:insert
  --hw-mitigation true|false              Allocate extents during the run. Default: false
  --preallocate-on-start true|false       Allocate one extent per table before workers start. Default: true
  --extent-size-mb NUMBER                 Default: 128
  --allocate-every-inserts NUMBER         Default: 100000
  --log-dir PATH                          Worker logs for run mode. Default: ./insert-blast-logs

Monitor options:
  --interval SECONDS                      Default: 5
  --iterations NUMBER                     0 means run until Ctrl-C. Default: 0

Examples:
  export ORACLE_CONNECT_STRING='app_user/app_password@racdb'
  scripts/insert-blast.sh create --prefix IBLAST --tables 8 --columns 24
  scripts/insert-blast.sh run --prefix IBLAST --tables 8 \
    --workload Workload_1:8:8:60:50:reuse:insert
  scripts/insert-blast.sh run --prefix IBLAST --tables 8 \
    --workload Select_Only:8:32:120:1:reuse:select
  scripts/insert-blast.sh monitor --interval 5
  scripts/insert-blast.sh drop --prefix IBLAST --drop-tablespaces true
  scripts/insert-blast.sh create --prefix IBLAST4 --tables 1000 \
    --use-existing-tablespaces true --tablespace-prefix IBLAST3_TS
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

to_bool() {
  case "${1,,}" in
    true|yes|y|1|on) echo "true" ;;
    false|no|n|0|off) echo "false" ;;
    *) die "Expected boolean true/false, got '$1'" ;;
  esac
}

sanitize_identifier() {
  local value="${1:-}"
  local fallback="${2:-}"
  local sanitized
  sanitized="$(printf '%s' "${value:-$fallback}" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9_$#]//g')"
  if [[ -z "$sanitized" ]]; then
    sanitized="$fallback"
  fi
  if [[ ! "$sanitized" =~ ^[A-Z] ]]; then
    sanitized="T${sanitized}"
  fi
  printf '%s' "$sanitized"
}

sql_literal() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/''/g")"
}

clamp_int() {
  local value="$1"
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

normalize_encryption_algorithm() {
  local algorithm
  algorithm="$(printf '%s' "${1:-AES256}" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]//g')"
  case "$algorithm" in
    AES128|AES192|AES256|3DES168) printf '%s' "$algorithm" ;;
    *) die "Invalid tablespace encryption algorithm '${1:-}'. Use AES128, AES192, AES256, or 3DES168." ;;
  esac
}

normalize_config() {
  TABLE_PREFIX="$(sanitize_identifier "$TABLE_PREFIX" "IBLAST")"
  TABLE_COUNT="$(clamp_int "$TABLE_COUNT" 1 5000 8)"
  COLUMNS_PER_TABLE="$(clamp_int "$COLUMNS_PER_TABLE" 4 200 24)"
  TABLESPACE_PREFIX="$(sanitize_identifier "${TABLESPACE_PREFIX:-${TABLE_PREFIX}_TS}" "${TABLE_PREFIX}_TS")"
  TABLESPACE_PREFIX="${TABLESPACE_PREFIX:0:27}"
  TABLESPACE_INITIAL_MB="$(clamp_int "$TABLESPACE_INITIAL_MB" 64 1048576 1024)"
  TABLESPACE_NEXT_MB="$(clamp_int "$TABLESPACE_NEXT_MB" 16 65536 1024)"
  EXTENT_SIZE_MB="$(clamp_int "$EXTENT_SIZE_MB" 8 1024 128)"
  ALLOCATE_EVERY_INSERTS="$(clamp_int "$ALLOCATE_EVERY_INSERTS" 1000 10000000 100000)"
  TABLESPACE_ENCRYPTION_ALGORITHM="$(normalize_encryption_algorithm "$TABLESPACE_ENCRYPTION_ALGORITHM")"
  MONITOR_INTERVAL="$(clamp_int "$MONITOR_INTERVAL" 1 86400 5)"
  MONITOR_ITERATIONS="$(clamp_int "$MONITOR_ITERATIONS" 0 1000000 0)"
}

table_name() {
  printf '%s_T%03d' "$TABLE_PREFIX" "$1"
}

tablespace_name() {
  printf '%s%03d' "$TABLESPACE_PREFIX" "$1"
}

resolve_datafile_name() {
  local location="$1"
  local ts_name="$2"
  [[ -z "$location" ]] && return 0
  if [[ "$location" == +* ]]; then
    printf '%s' "$location"
  elif [[ "$location" == */ ]]; then
    printf '%s%s.dbf' "$location" "$ts_name"
  else
    printf '%s/%s.dbf' "$location" "$ts_name"
  fi
}

require_sqlplus() {
  command -v sqlplus >/dev/null 2>&1 || die "sqlplus is required but was not found in PATH."
}

require_connection() {
  [[ -n "$CONNECT_STRING" ]] || die "Provide --connect USER/PASSWORD@DB or set ORACLE_CONNECT_STRING."
}

run_sql() {
  local sql="$1"
  require_sqlplus
  require_connection
  sqlplus -s "$CONNECT_STRING" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK ON HEADING ON PAGESIZE 200 LINESIZE 220 TRIMSPOOL ON SERVEROUTPUT ON
$sql
EXIT
SQL
}

run_sql_quiet() {
  local sql="$1"
  require_sqlplus
  require_connection
  sqlplus -s "$CONNECT_STRING" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK OFF HEADING OFF PAGESIZE 0 LINESIZE 32767 TRIMSPOOL ON SERVEROUTPUT ON
$sql
EXIT
SQL
}

build_column_definitions() {
  local columns="id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL"
  local index suffix
  for ((index = 1; index <= COLUMNS_PER_TABLE; index += 1)); do
    suffix="$(printf '%03d' "$index")"
    if (( index % 3 == 1 )); then
      columns+=",
  vc_${suffix} VARCHAR2(100)"
    elif (( index % 3 == 2 )); then
      columns+=",
  num_${suffix} NUMBER(18,2)"
    else
      columns+=",
  dt_${suffix} DATE"
    fi
  done
  printf '%s' "$columns"
}

insert_columns_csv() {
  local columns=()
  local index suffix
  for ((index = 1; index <= COLUMNS_PER_TABLE; index += 1)); do
    suffix="$(printf '%03d' "$index")"
    if (( index % 3 == 1 )); then
      columns+=("vc_${suffix}")
    elif (( index % 3 == 2 )); then
      columns+=("num_${suffix}")
    else
      columns+=("dt_${suffix}")
    fi
  done
  local IFS=", "
  printf '%s' "${columns[*]}"
}

create_tables() {
  normalize_config
  local column_defs table ts datafile datafile_clause tablespace_clause encryption_clause sql=""
  column_defs="$(build_column_definitions)"

  if [[ "$CREATE_TABLESPACES" == "true" && "$USE_EXISTING_TABLESPACES" == "true" ]]; then
    die "Use either --create-tablespaces true or --use-existing-tablespaces true, not both."
  fi

  for ((i = 1; i <= TABLE_COUNT; i += 1)); do
    table="$(table_name "$i")"
    tablespace_clause=""
    if [[ "$CREATE_TABLESPACES" == "true" || "$USE_EXISTING_TABLESPACES" == "true" ]]; then
      ts="$(tablespace_name "$i")"
      if [[ "$CREATE_TABLESPACES" == "true" ]]; then
        datafile="$(resolve_datafile_name "$TABLESPACE_DATAFILE_LOCATION" "$ts")"
        if [[ -n "$datafile" ]]; then
          datafile_clause="DATAFILE $(sql_literal "$datafile") SIZE ${TABLESPACE_INITIAL_MB}M"
        else
          datafile_clause="DATAFILE SIZE ${TABLESPACE_INITIAL_MB}M"
        fi
        sql+="PROMPT Creating BIGFILE tablespace ${ts}
CREATE BIGFILE TABLESPACE ${ts}
  ${datafile_clause}
  AUTOEXTEND ON NEXT ${TABLESPACE_NEXT_MB}M
  MAXSIZE UNLIMITED"
        if [[ "$TABLESPACE_ENCRYPTION_ENABLED" == "true" ]]; then
          encryption_clause="
  ENCRYPTION USING '${TABLESPACE_ENCRYPTION_ALGORITHM}'
  DEFAULT STORAGE (ENCRYPT)"
        else
          encryption_clause=""
        fi
        sql+="${encryption_clause};
"
      fi
      tablespace_clause="
TABLESPACE ${ts}"
    fi

    sql+="PROMPT Creating ${table}
CREATE TABLE ${table} (
  ${column_defs}
)${tablespace_clause}
NOLOGGING;
"
  done

  run_sql "$sql"
}

drop_tables() {
  normalize_config
  local escaped_prefix like_pattern escaped_tablespace_prefix tablespace_like_pattern
  escaped_prefix="${TABLE_PREFIX//\\/\\\\}"
  escaped_prefix="${escaped_prefix//_/\\_}"
  escaped_prefix="${escaped_prefix//%/\\%}"
  like_pattern="${escaped_prefix}\\_T%"
  escaped_tablespace_prefix="${TABLESPACE_PREFIX//\\/\\\\}"
  escaped_tablespace_prefix="${escaped_tablespace_prefix//_/\\_}"
  escaped_tablespace_prefix="${escaped_tablespace_prefix//%/\\%}"
  tablespace_like_pattern="${escaped_tablespace_prefix}%"
  run_sql "SET SERVEROUTPUT ON
DECLARE
  TYPE t_name_list IS TABLE OF VARCHAR2(128);
  l_tablespaces t_name_list := t_name_list();

  PROCEDURE remember_tablespace(p_tablespace_name VARCHAR2) IS
  BEGIN
    IF p_tablespace_name IS NULL THEN
      RETURN;
    END IF;

    FOR i IN 1 .. l_tablespaces.COUNT LOOP
      IF l_tablespaces(i) = p_tablespace_name THEN
        RETURN;
      END IF;
    END LOOP;

    l_tablespaces.EXTEND;
    l_tablespaces(l_tablespaces.COUNT) := p_tablespace_name;
  END;
BEGIN
  FOR table_rec IN (
    SELECT table_name, tablespace_name
    FROM user_tables
    WHERE table_name LIKE $(sql_literal "$like_pattern") ESCAPE '\'
    ORDER BY table_name
  ) LOOP
    IF $(sql_literal "$DROP_TABLESPACES") = 'true'
       AND table_rec.tablespace_name LIKE $(sql_literal "$tablespace_like_pattern") ESCAPE '\' THEN
      remember_tablespace(table_rec.tablespace_name);
    END IF;

    EXECUTE IMMEDIATE 'DROP TABLE ' || DBMS_ASSERT.SIMPLE_SQL_NAME(table_rec.table_name) || ' PURGE';
    DBMS_OUTPUT.PUT_LINE('Dropped table ' || table_rec.table_name);
  END LOOP;

  IF $(sql_literal "$DROP_TABLESPACES") = 'true' THEN
    FOR i IN 1 .. l_tablespaces.COUNT LOOP
      BEGIN
        EXECUTE IMMEDIATE 'DROP TABLESPACE ' || DBMS_ASSERT.SIMPLE_SQL_NAME(l_tablespaces(i)) || ' INCLUDING CONTENTS AND DATAFILES';
        DBMS_OUTPUT.PUT_LINE('Dropped tablespace ' || l_tablespaces(i));
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -959 THEN
            DBMS_OUTPUT.PUT_LINE('Tablespace ' || l_tablespaces(i) || ' does not exist');
          ELSE
            RAISE;
          END IF;
      END;
    END LOOP;
  END IF;
END;
/
"
}

status_tables() {
  normalize_config
  local escaped_prefix like_pattern
  escaped_prefix="${TABLE_PREFIX//\\/\\\\}"
  escaped_prefix="${escaped_prefix//_/\\_}"
  escaped_prefix="${escaped_prefix//%/\\%}"
  like_pattern="${escaped_prefix}\\_T%"
  run_sql "COLUMN table_name FORMAT A32
COLUMN tablespace_name FORMAT A32
SELECT table_name, num_rows, tablespace_name
FROM user_tables
WHERE table_name LIKE $(sql_literal "$like_pattern") ESCAPE '\'
ORDER BY table_name;
"
}

allocate_extent_for_table() {
  local table="$1"
  run_sql_quiet "ALTER TABLE ${table} ALLOCATE EXTENT (SIZE ${EXTENT_SIZE_MB}M);" >/dev/null
}

preallocate_extents() {
  [[ "$HW_MITIGATION_ENABLED" == "true" && "$PREALLOCATE_ON_START" == "true" ]] || return 0
  local parsed _name _tables _sessions _duration _commit_every _mode operation has_insert_workload="false"
  for spec in "${WORKLOADS[@]}"; do
    parsed="$(parse_workload_spec "$spec")"
    IFS=':' read -r _name _tables _sessions _duration _commit_every _mode operation <<<"$parsed"
    if [[ "$operation" == "insert" ]]; then
      has_insert_workload="true"
      break
    fi
  done
  [[ "$has_insert_workload" == "true" ]] || return 0
  echo "Pre-allocating ${EXTENT_SIZE_MB} MB extents for ${TABLE_COUNT} table(s)..."
  local table
  for ((i = 1; i <= TABLE_COUNT; i += 1)); do
    table="$(table_name "$i")"
    if allocate_extent_for_table "$table"; then
      echo "  ${table}: allocated"
    else
      echo "  ${table}: extent allocation failed, continuing" >&2
    fi
  done
}

parse_workload_spec() {
  local spec="$1"
  local name tables sessions duration commit_every mode operation
  IFS=':' read -r name tables sessions duration commit_every mode operation <<<"$spec"
  [[ -n "${name:-}" ]] || name="Workload_1"
  tables="$(clamp_int "${tables:-$TABLE_COUNT}" 1 "$TABLE_COUNT" "$TABLE_COUNT")"
  sessions="$(clamp_int "${sessions:-8}" 1 100000 8)"
  duration="$(clamp_int "${duration:-60}" 1 86400 60)"
  commit_every="$(clamp_int "${commit_every:-50}" 1 100000000 50)"
  mode="${mode:-reuse}"
  operation="${operation:-insert}"
  case "${mode,,}" in
    reuse|reconnect) mode="${mode,,}" ;;
    *) die "Invalid workload mode '${mode}'. Use reuse or reconnect." ;;
  esac
  case "${operation,,}" in
    insert|select) operation="${operation,,}" ;;
    *) die "Invalid workload operation '${operation}'. Use insert or select." ;;
  esac
  printf '%s:%s:%s:%s:%s:%s:%s' "$name" "$tables" "$sessions" "$duration" "$commit_every" "$mode" "$operation"
}

default_workloads_if_needed() {
  if (( ${#WORKLOADS[@]} == 0 )); then
    WORKLOADS=("Workload_1:${TABLE_COUNT}:8:60:50:reuse:insert")
  fi
}

sql_name_list() {
  local count="$1"
  local values=()
  for ((i = 1; i <= count; i += 1)); do
    values+=("$(sql_literal "$(table_name "$i")")")
  done
  local IFS=", "
  printf '%s' "${values[*]}"
}

sql_value_concat_expression() {
  local workload_id="$1"
  local worker_id="$2"
  local parts=()
  local escaped_workload
  escaped_workload="$(printf '%s' "$workload_id" | sed "s/'/''/g")"
  local index suffix
  for ((index = 1; index <= COLUMNS_PER_TABLE; index += 1)); do
    suffix="$(printf '%03d' "$index")"
    if (( index % 3 == 1 )); then
      parts+=("'''' || '${escaped_workload}_W${worker_id}_R' || TO_CHAR(l_sequence) || '_C${suffix}' || ''''")
    elif (( index % 3 == 2 )); then
      parts+=("TO_CHAR((${worker_id} * 1000000) + l_sequence + ${index})")
    else
      parts+=("'SYSDATE - (MOD(' || TO_CHAR(l_sequence + ${index}) || ', 86400) / 86400)'")
    fi
  done
  local expression="${parts[0]}"
  for ((index = 1; index < ${#parts[@]}; index += 1)); do
    expression+=" || ', ' || ${parts[$index]}"
  done
  printf '%s' "$expression"
}

worker_sql() {
  local workload_id="$1"
  local workload_name="$2"
  local table_limit="$3"
  local duration="$4"
  local commit_every="$5"
  local operation="$6"
  local worker_id="$7"
  local run_id="$8"
  local columns values table_list hw_enabled
  columns="$(insert_columns_csv)"
  values="$(sql_value_concat_expression "$workload_id" "$worker_id")"
  table_list="$(sql_name_list "$table_limit")"
  hw_enabled="$HW_MITIGATION_ENABLED"

  cat <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK OFF HEADING OFF PAGESIZE 0 LINESIZE 32767 TRIMSPOOL ON SERVEROUTPUT ON
DECLARE
  TYPE t_table_names IS TABLE OF VARCHAR2(128);
  TYPE t_counts IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(128);
  l_tables t_table_names := t_table_names(${table_list});
  l_counts t_counts;
  l_next_alloc t_counts;
  l_end_time TIMESTAMP := SYSTIMESTAMP + NUMTODSINTERVAL(${duration}, 'SECOND');
  l_table_name VARCHAR2(128);
  l_sql CLOB;
  l_sequence PLS_INTEGER := 0;
  l_pending PLS_INTEGER := 0;
  l_operations PLS_INTEGER := 0;
  l_inserts PLS_INTEGER := 0;
  l_selects PLS_INTEGER := 0;
  l_errors PLS_INTEGER := 0;
  l_sample_rows PLS_INTEGER := 0;

  PROCEDURE maybe_allocate_extent(p_table_name VARCHAR2) IS
  BEGIN
    IF '${hw_enabled}' <> 'true' THEN
      RETURN;
    END IF;

    IF NOT l_next_alloc.EXISTS(p_table_name) THEN
      l_next_alloc(p_table_name) := ${ALLOCATE_EVERY_INSERTS};
    END IF;

    IF l_counts(p_table_name) >= l_next_alloc(p_table_name) THEN
      BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name || ' ALLOCATE EXTENT (SIZE ${EXTENT_SIZE_MB}M)';
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
      l_next_alloc(p_table_name) := l_next_alloc(p_table_name) + ${ALLOCATE_EVERY_INSERTS};
    END IF;
  END;
BEGIN
  DBMS_APPLICATION_INFO.SET_MODULE('DBSTRESS_INSERT_BLAST', '${run_id}');
  DBMS_SESSION.SET_IDENTIFIER('IBLAST:${run_id}:${workload_id}:W${worker_id}');

  WHILE SYSTIMESTAMP < l_end_time LOOP
    BEGIN
      l_table_name := l_tables(TRUNC(DBMS_RANDOM.VALUE(1, l_tables.COUNT + 1)));
      l_sequence := l_sequence + 1;
      IF '${operation}' = 'select' THEN
        l_sql := 'SELECT COUNT(*) FROM ' || l_table_name || ' WHERE ROWNUM <= 10';
        EXECUTE IMMEDIATE l_sql INTO l_sample_rows;
        l_selects := l_selects + 1;
        l_operations := l_operations + 1;
      ELSE
        l_sql := 'INSERT INTO ' || l_table_name || ' (${columns}) VALUES (' || ${values} || ')';
        EXECUTE IMMEDIATE l_sql;

        IF NOT l_counts.EXISTS(l_table_name) THEN
          l_counts(l_table_name) := 0;
        END IF;
        l_counts(l_table_name) := l_counts(l_table_name) + 1;
        maybe_allocate_extent(l_table_name);

        l_pending := l_pending + 1;
        l_inserts := l_inserts + 1;
        l_operations := l_operations + 1;
        IF l_pending >= ${commit_every} THEN
          COMMIT;
          l_pending := 0;
        END IF;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        l_errors := l_errors + 1;
        ROLLBACK;
        l_pending := 0;
    END;
  END LOOP;

  IF l_pending > 0 THEN
    COMMIT;
  END IF;

  DBMS_OUTPUT.PUT_LINE('${workload_name} worker ${worker_id}: operation=${operation} operations=' || l_operations || ' inserts=' || l_inserts || ' selects=' || l_selects || ' errors=' || l_errors);
END;
/
EXIT
SQL
}

run_worker_reuse() {
  local workload_id="$1"
  local workload_name="$2"
  local table_limit="$3"
  local duration="$4"
  local commit_every="$5"
  local operation="$6"
  local worker_id="$7"
  local run_id="$8"
  local log_file="${LOG_DIR}/${workload_id}_worker_${worker_id}.log"
  worker_sql "$workload_id" "$workload_name" "$table_limit" "$duration" "$commit_every" "$operation" "$worker_id" "$run_id" |
    sqlplus -s "$CONNECT_STRING" >"$log_file" 2>&1
}

run_worker_reconnect() {
  local workload_id="$1"
  local workload_name="$2"
  local table_limit="$3"
  local duration="$4"
  local commit_every="$5"
  local operation="$6"
  local worker_id="$7"
  local run_id="$8"
  local log_file="${LOG_DIR}/${workload_id}_worker_${worker_id}.log"
  local end_epoch now remaining chunk
  end_epoch=$(( $(date +%s) + duration ))
  : >"$log_file"

  while true; do
    now="$(date +%s)"
    (( now >= end_epoch )) && break
    remaining=$(( end_epoch - now ))
    chunk="$remaining"
    if (( chunk > 1 )); then
      chunk=1
    fi
    worker_sql "$workload_id" "$workload_name" "$table_limit" "$chunk" "$commit_every" "$operation" "$worker_id" "$run_id" |
      sqlplus -s "$CONNECT_STRING" >>"$log_file" 2>&1 || true
  done
}

print_worker_failure_summary() {
  [[ -d "$LOG_DIR" ]] || return 0

  local log_count
  log_count="$(find "$LOG_DIR" -maxdepth 1 -type f | wc -l | tr -d ' ')"
  echo "Worker log files: ${log_count}"
  echo "Sample worker errors:"
  grep -RnhE 'ORA-|SP2-|TNS-|ERROR|error|maximum number|resource busy|insufficient privileges|no listener' "$LOG_DIR" | head -n 20 || echo "  No ORA/TNS/SP2 error lines found in ${LOG_DIR}"
}

run_workload() {
  normalize_config
  default_workloads_if_needed
  mkdir -p "$LOG_DIR"
  require_sqlplus
  require_connection

  local run_id="manual_$(date +%Y%m%d%H%M%S)_$$"
  local pids=()
  local parsed workload_id workload_name table_limit sessions duration commit_every mode operation worker

  preallocate_extents
  echo "Starting Insert Blast run ${run_id}. Logs: ${LOG_DIR}"
  trap 'echo "Stopping Insert Blast workers..."; kill "${pids[@]}" >/dev/null 2>&1 || true; wait "${pids[@]}" >/dev/null 2>&1 || true; exit 130' INT TERM

  for spec in "${WORKLOADS[@]}"; do
    parsed="$(parse_workload_spec "$spec")"
    IFS=':' read -r workload_name table_limit sessions duration commit_every mode operation <<<"$parsed"
    workload_id="$(printf '%s' "$workload_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g')"
    [[ -n "$workload_id" ]] || workload_id="workload"

    echo "  ${workload_name}: tables=${table_limit}, sessions=${sessions}, duration=${duration}s, commitEvery=${commit_every}, mode=${mode}, operation=${operation}"
    for ((worker = 1; worker <= sessions; worker += 1)); do
      if [[ "$mode" == "reconnect" ]]; then
        run_worker_reconnect "$workload_id" "$workload_name" "$table_limit" "$duration" "$commit_every" "$operation" "$worker" "$run_id" &
      else
        run_worker_reuse "$workload_id" "$workload_name" "$table_limit" "$duration" "$commit_every" "$operation" "$worker" "$run_id" &
      fi
      pids+=("$!")
    done
  done

  local alive elapsed=0
  while true; do
    alive="$(jobs -pr | wc -l | tr -d ' ')"
    printf 'Elapsed=%ss ActiveWorkers=%s\n' "$elapsed" "$alive"
    (( alive == 0 )) && break
    sleep 5
    elapsed=$((elapsed + 5))
  done

  local failures=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failures=$((failures + 1))
    fi
  done

  echo "Insert Blast run complete. Worker failures: ${failures}"
  if (( failures > 0 )); then
    print_worker_failure_summary
  fi
  echo "Worker summaries:"
  grep -h "worker .*: operation=" "${LOG_DIR}"/*.log 2>/dev/null || true
  trap - INT TERM
  (( failures == 0 ))
}

monitor_once() {
  run_sql "COLUMN username FORMAT A24
COLUMN source FORMAT A18
COLUMN process_name FORMAT A12
COLUMN event FORMAT A42
COLUMN wait_class FORMAT A18
PROMPT === User sessions by instance ===
SELECT inst_id,
       username,
       COUNT(*) AS total_sessions,
       SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_sessions,
       SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive_sessions
FROM gv\$session
WHERE type = 'USER'
  AND username = SYS_CONTEXT('USERENV', 'SESSION_USER')
GROUP BY inst_id, username
ORDER BY inst_id;

PROMPT === LMS process memory ===
SELECT p.inst_id,
       p.pname AS process_name,
       p.spid AS os_pid,
       ROUND(p.pga_used_mem / 1024 / 1024, 2) AS pga_used_mb,
       ROUND(p.pga_alloc_mem / 1024 / 1024, 2) AS pga_alloc_mb
FROM gv\$process p
WHERE p.pname LIKE 'LMS%'
ORDER BY p.pga_used_mem DESC
FETCH FIRST 20 ROWS ONLY;

PROMPT === Top wait events ===
SELECT inst_id,
       event,
       wait_class,
       total_waits,
       ROUND(time_waited_micro / 1000000, 2) AS time_waited_seconds,
       ROUND(CASE WHEN total_waits > 0 THEN time_waited_micro / total_waits / 1000 ELSE 0 END, 3) AS average_wait_ms
FROM gv\$system_event
WHERE wait_class <> 'Idle'
  AND total_waits > 0
ORDER BY time_waited_micro DESC
FETCH FIRST 20 ROWS ONLY;
"
}

monitor_loop() {
  normalize_config
  local iteration=0
  while true; do
    iteration=$((iteration + 1))
    echo
    echo "===== Insert Blast monitor snapshot $(date '+%Y-%m-%d %H:%M:%S') ====="
    monitor_once
    if (( MONITOR_ITERATIONS > 0 && iteration >= MONITOR_ITERATIONS )); then
      break
    fi
    sleep "$MONITOR_INTERVAL"
  done
}

parse_args() {
  [[ $# -gt 0 ]] || { usage; exit 1; }
  COMMAND="$1"
  shift

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --connect) CONNECT_STRING="${2:-}"; shift 2 ;;
      --prefix) TABLE_PREFIX="${2:-}"; shift 2 ;;
      --tables|--table-count) TABLE_COUNT="${2:-}"; shift 2 ;;
      --columns|--columns-per-table) COLUMNS_PER_TABLE="${2:-}"; shift 2 ;;
      --create-tablespaces) CREATE_TABLESPACES="$(to_bool "${2:-}")"; shift 2 ;;
      --use-existing-tablespaces) USE_EXISTING_TABLESPACES="$(to_bool "${2:-}")"; shift 2 ;;
      --drop-tablespaces) DROP_TABLESPACES="$(to_bool "${2:-}")"; shift 2 ;;
      --tablespace-prefix) TABLESPACE_PREFIX="${2:-}"; shift 2 ;;
      --tablespace-initial-mb) TABLESPACE_INITIAL_MB="${2:-}"; shift 2 ;;
      --tablespace-next-mb|--tablespace-autoextend-next-mb) TABLESPACE_NEXT_MB="${2:-}"; shift 2 ;;
      --tablespace-datafile-location) TABLESPACE_DATAFILE_LOCATION="${2:-}"; shift 2 ;;
      --tablespace-encryption|--encrypt-tablespaces) TABLESPACE_ENCRYPTION_ENABLED="$(to_bool "${2:-}")"; shift 2 ;;
      --tablespace-encryption-algorithm|--tablespace-encryption-cipher) TABLESPACE_ENCRYPTION_ALGORITHM="${2:-}"; shift 2 ;;
      --workload) WORKLOADS+=("${2:-}"); shift 2 ;;
      --hw-mitigation) HW_MITIGATION_ENABLED="$(to_bool "${2:-}")"; shift 2 ;;
      --preallocate-on-start) PREALLOCATE_ON_START="$(to_bool "${2:-}")"; shift 2 ;;
      --extent-size-mb) EXTENT_SIZE_MB="${2:-}"; shift 2 ;;
      --allocate-every-inserts) ALLOCATE_EVERY_INSERTS="${2:-}"; shift 2 ;;
      --interval) MONITOR_INTERVAL="${2:-}"; shift 2 ;;
      --iterations) MONITOR_ITERATIONS="${2:-}"; shift 2 ;;
      --log-dir) LOG_DIR="${2:-}"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option '$1'. Use --help for usage." ;;
    esac
  done
}

main() {
  parse_args "$@"
  case "$COMMAND" in
    -h|--help|help) usage; exit 0 ;;
  esac
  require_connection
  case "$COMMAND" in
    create) create_tables ;;
    drop) drop_tables ;;
    status) status_tables ;;
    run) run_workload ;;
    monitor) monitor_loop ;;
    *) die "Unknown command '$COMMAND'. Use --help for usage." ;;
  esac
}

main "$@"
