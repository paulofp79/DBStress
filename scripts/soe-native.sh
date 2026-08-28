#!/usr/bin/env bash
set -euo pipefail

# Native replacement for the create/grow data path: sqlplus and Oracle SQL only.
# It does not invoke oewizard, sbutil, Java, or any Swingbench executable.

ACTION="grow"
CONNECT_STRING="${ORACLE_CONNECT_STRING:-}"
SCHEMA_USER="${ORACLE_DB_USER:-}"
SCHEMA_PASSWORD="${ORACLE_DB_PASSWORD:-}"
DBA_CONNECT="${ORACLE_DBA_CONNECT:-}"
SEED_CUSTOMERS=500000
TARGET_MULTIPLIER=10
DOP=16

usage() {
  cat <<'EOF'
Native SOE-style direct-path schema loader (sqlplus only).

Usage:
  scripts/soe-native.sh [create|grow|create-grow] --connect //host/service
    --user USER --password PASSWORD [options]

Create options:
  --dba-connect USER/PASSWORD@//host/service
  --seed-customers NUMBER             Default: 500000

Grow options:
  --target-multiplier NUMBER          Final multiple of the seed. Default: 10
  --parallel-degree NUMBER            Default: 16

create requires a DBA connection to create the schema user. grow works only on a
schema previously created by this script. It creates linked CUSTOMER, ADDRESS,
ORDERS, and ORDER_ITEMS data using APPEND and parallel DML.
EOF
}
die(){ echo "Error: $*" >&2; exit 1; }
pos(){ [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
ident(){ [[ "$1" =~ ^[A-Za-z][A-Za-z0-9_$#]*$ ]]; }
run_sql(){ sqlplus -s "$1" <<SQL
WHENEVER OSERROR EXIT 9
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET ECHO OFF FEEDBACK ON HEADING ON PAGESIZE 200 LINESIZE 220 SERVEROUTPUT ON
$2
EXIT
SQL
}
schema_connect(){ printf '%s/%s@%s' "$SCHEMA_USER" "$SCHEMA_PASSWORD" "$CONNECT_STRING"; }
parse(){
  case "${1:-}" in create|grow|create-grow) ACTION=$1; shift;; esac
  while [[ $# -gt 0 ]]; do case "$1" in
    --connect) CONNECT_STRING=${2:-};shift 2;; --user) SCHEMA_USER=${2:-};shift 2;; --password) SCHEMA_PASSWORD=${2:-};shift 2;;
    --dba-connect) DBA_CONNECT=${2:-};shift 2;; --seed-customers) SEED_CUSTOMERS=${2:-};shift 2;;
    --target-multiplier|--duplicates|--dup) TARGET_MULTIPLIER=${2:-};shift 2;; --parallel-degree|--parallel) DOP=${2:-};shift 2;;
    -h|--help) usage;exit 0;; *) die "Unknown option '$1'.";; esac; done
}
create(){
  [[ -n "$DBA_CONNECT" ]] || die "create requires --dba-connect."
  local user_upper="${SCHEMA_USER^^}" addresses=$((SEED_CUSTOMERS*3/2)) orders=$((SEED_CUSTOMERS*14/10)) items=$((orders*3))
  echo "Creating ${user_upper}; seed customers=${SEED_CUSTOMERS}, orders=${orders}, items=${items}, DOP=${DOP}."
  run_sql "$DBA_CONNECT" "CREATE USER ${user_upper} IDENTIFIED BY \"$SCHEMA_PASSWORD\"; GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO ${user_upper};"
  run_sql "$(schema_connect)" "
ALTER SESSION ENABLE PARALLEL DML;
CREATE TABLE customers (customer_id NUMBER NOT NULL, customer_name VARCHAR2(100) NOT NULL, email VARCHAR2(120) NOT NULL) NOLOGGING;
CREATE TABLE addresses (address_id NUMBER NOT NULL, customer_id NUMBER NOT NULL, address_text VARCHAR2(160) NOT NULL) NOLOGGING;
CREATE TABLE orders (order_id NUMBER NOT NULL, customer_id NUMBER NOT NULL, address_id NUMBER NOT NULL, order_date DATE NOT NULL, order_total NUMBER(12,2) NOT NULL) NOLOGGING;
CREATE TABLE order_items (order_id NUMBER NOT NULL, line_item_id NUMBER NOT NULL, product_id NUMBER NOT NULL, quantity NUMBER NOT NULL, unit_price NUMBER(10,2) NOT NULL) NOLOGGING;
CREATE TABLE soe_native_metadata (name VARCHAR2(30) PRIMARY KEY, value_number NUMBER NOT NULL) NOLOGGING;
INSERT /*+ APPEND PARALLEL(customers,${DOP}) */ INTO customers SELECT LEVEL,'Customer '||LEVEL,'customer'||LEVEL||'@example.test' FROM dual CONNECT BY LEVEL<=${SEED_CUSTOMERS};
INSERT /*+ APPEND PARALLEL(addresses,${DOP}) */ INTO addresses SELECT LEVEL,MOD(LEVEL-1,${SEED_CUSTOMERS})+1,'Address '||LEVEL FROM dual CONNECT BY LEVEL<=${addresses};
INSERT /*+ APPEND PARALLEL(orders,${DOP}) */ INTO orders SELECT LEVEL,MOD(LEVEL-1,${SEED_CUSTOMERS})+1,MOD(LEVEL-1,${addresses})+1,TRUNC(SYSDATE)-MOD(LEVEL,3650),MOD(LEVEL,5000)+10 FROM dual CONNECT BY LEVEL<=${orders};
INSERT /*+ APPEND PARALLEL(order_items,${DOP}) */ INTO order_items SELECT CEIL(LEVEL/3),MOD(LEVEL-1,3)+1,MOD(LEVEL-1,1000)+1,MOD(LEVEL,5)+1,MOD(LEVEL,500)+1 FROM dual CONNECT BY LEVEL<=${items};
INSERT INTO soe_native_metadata VALUES('BASE_CUSTOMERS',${SEED_CUSTOMERS}); INSERT INTO soe_native_metadata VALUES('BASE_ADDRESSES',${addresses}); INSERT INTO soe_native_metadata VALUES('BASE_ORDERS',${orders}); INSERT INTO soe_native_metadata VALUES('CURRENT_MULTIPLIER',1); COMMIT;
CREATE UNIQUE INDEX soe_native_customers_pk ON customers(customer_id) NOLOGGING PARALLEL ${DOP}; CREATE UNIQUE INDEX soe_native_orders_pk ON orders(order_id) NOLOGGING PARALLEL ${DOP}; CREATE UNIQUE INDEX soe_native_items_pk ON order_items(order_id,line_item_id) NOLOGGING PARALLEL ${DOP};"
}
grow(){
  echo "Growing native SOE-style schema to ${TARGET_MULTIPLIER}x its seed at DOP=${DOP}."
  run_sql "$(schema_connect)" "
ALTER SESSION ENABLE PARALLEL DML;
DECLARE c NUMBER;a NUMBER;o NUMBER;m NUMBER;n NUMBER; BEGIN
SELECT MAX(CASE name WHEN 'BASE_CUSTOMERS' THEN value_number END),MAX(CASE name WHEN 'BASE_ADDRESSES' THEN value_number END),MAX(CASE name WHEN 'BASE_ORDERS' THEN value_number END),MAX(CASE name WHEN 'CURRENT_MULTIPLIER' THEN value_number END) INTO c,a,o,m FROM soe_native_metadata;
IF ${TARGET_MULTIPLIER}<m THEN RAISE_APPLICATION_ERROR(-20001,'Target multiplier is below current multiplier'); END IF; n:=${TARGET_MULTIPLIER}-m; IF n=0 THEN RETURN; END IF;
EXECUTE IMMEDIATE 'DROP INDEX soe_native_customers_pk'; EXECUTE IMMEDIATE 'DROP INDEX soe_native_orders_pk'; EXECUTE IMMEDIATE 'DROP INDEX soe_native_items_pk';
EXECUTE IMMEDIATE q'~INSERT /*+ APPEND PARALLEL */ INTO customers SELECT x.customer_id+f.k*:1,x.customer_name||' #'||f.k,'copy'||f.k||'_'||x.email FROM customers x CROSS JOIN (SELECT :2+LEVEL-1 k FROM dual CONNECT BY LEVEL<=:3) f WHERE x.customer_id<=:1~' USING c,m,n,c;
EXECUTE IMMEDIATE q'~INSERT /*+ APPEND PARALLEL */ INTO addresses SELECT x.address_id+f.k*:1,x.customer_id+f.k*:2,x.address_text FROM addresses x CROSS JOIN (SELECT :3+LEVEL-1 k FROM dual CONNECT BY LEVEL<=:4) f WHERE x.address_id<=:1~' USING a,c,m,n,a;
EXECUTE IMMEDIATE q'~INSERT /*+ APPEND PARALLEL */ INTO orders SELECT x.order_id+f.k*:1,x.customer_id+f.k*:2,x.address_id+f.k*:3,x.order_date,x.order_total FROM orders x CROSS JOIN (SELECT :4+LEVEL-1 k FROM dual CONNECT BY LEVEL<=:5) f WHERE x.order_id<=:1~' USING o,c,a,m,n,o;
EXECUTE IMMEDIATE q'~INSERT /*+ APPEND PARALLEL */ INTO order_items SELECT x.order_id+f.k*:1,x.line_item_id,x.product_id,x.quantity,x.unit_price FROM order_items x CROSS JOIN (SELECT :2+LEVEL-1 k FROM dual CONNECT BY LEVEL<=:3) f WHERE x.order_id<=:1~' USING o,m,n,o;
UPDATE soe_native_metadata SET value_number=${TARGET_MULTIPLIER} WHERE name='CURRENT_MULTIPLIER'; COMMIT;
END;
/
CREATE UNIQUE INDEX soe_native_customers_pk ON customers(customer_id) NOLOGGING PARALLEL ${DOP}; CREATE UNIQUE INDEX soe_native_orders_pk ON orders(order_id) NOLOGGING PARALLEL ${DOP}; CREATE UNIQUE INDEX soe_native_items_pk ON order_items(order_id,line_item_id) NOLOGGING PARALLEL ${DOP};"
}
main(){ parse "$@"; command -v sqlplus >/dev/null || die "sqlplus is required."; [[ -n "$CONNECT_STRING" && -n "$SCHEMA_USER" && -n "$SCHEMA_PASSWORD" ]] || die "Provide --connect, --user, and --password."; ident "$SCHEMA_USER" || die "User must be an unquoted Oracle identifier."; pos "$SEED_CUSTOMERS" && pos "$TARGET_MULTIPLIER" && pos "$DOP" || die "Numeric options must be positive integers."; case "$ACTION" in create)create;;grow)grow;;create-grow)create;grow;;esac; }
main "$@"
