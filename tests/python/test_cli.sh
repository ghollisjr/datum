#!/bin/bash
# CLI (non-interactive) mode integration tests for datum.
#
# Tests the --query, --command, and --format flags against live databases.
# Mirrors the backend detection and connection logic of test_integration.sh.
#
# Usage:
#   bash tests/python/test_cli.sh
#
# Environment variables (same as test_integration.sh):
#   DATUM_BACKENDS     Comma-separated list (default: mysql,postgres,mssql,sqlite,oracle)
#   DATUM_STRICT=1     No silent skips; every requested backend must pass
#   DATUM_<BACKEND>_*  Backend-specific credentials

set -o pipefail

TIMEOUT_CMD="timeout"
TIMEOUT_SECS=10
STRICT="${DATUM_STRICT:-0}"

if ! command -v datum &>/dev/null; then
    DATUM_CMD="python -m datum"
else
    DATUM_CMD="datum"
fi

# --- Test helpers ---

PASS=0
FAIL=0
ERRORS=""
BACKENDS_RAN=0

# Run datum in non-interactive CLI mode.
run_datum_cli() {
    local conn="$1"
    local sql_type="$2"
    shift 2
    # Remaining args are the CLI flags (--query/--command/--format)
    $TIMEOUT_CMD ${TIMEOUT_SECS} $DATUM_CMD \
        --conn-string="$conn" --sql-type="$sql_type" "$@" 2>/dev/null
}

# Run datum CLI and capture stderr too (for error tests).
run_datum_cli_stderr() {
    local conn="$1"
    local sql_type="$2"
    shift 2
    $TIMEOUT_CMD ${TIMEOUT_SECS} $DATUM_CMD \
        --conn-string="$conn" --sql-type="$sql_type" "$@" 2>&1
}

# Check that stdout matches a pattern. Stderr is discarded (envelope goes there).
check_cli() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    local pattern="$4"
    shift 4
    # Remaining args are passed to datum

    local output
    output=$(run_datum_cli "$conn" "$sql_type" "$@")
    local rc=$?

    if [ $rc -ne 0 ] && [ $rc -ne 124 ]; then
        echo "  FAIL: $test_name (exit code $rc)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name (exit code $rc)"
        return
    fi

    if echo "$output" | grep -qi "$pattern"; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (pattern '$pattern' not found)"
        echo "    Output (first 5 lines):"
        echo "$output" | head -5 | sed 's/^/      /'
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name"
    fi
}

# Check that stdout does NOT contain envelope lines (they should be on stderr).
check_no_envelope() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    shift 3

    local output
    output=$(run_datum_cli "$conn" "$sql_type" "$@")

    if echo "$output" | grep -q '##DATUM:'; then
        echo "  FAIL: $test_name (envelope lines found on stdout)"
        echo "    Output (first 3 lines with envelope):"
        echo "$output" | grep '##DATUM:' | head -3 | sed 's/^/      /'
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name"
    else
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    fi
}

# Check that exit code is 0.
check_exit_code() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    shift 3

    run_datum_cli "$conn" "$sql_type" "$@" >/dev/null
    local rc=$?

    if [ $rc -eq 0 ]; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (expected exit code 0, got $rc)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name"
    fi
}

# Check that JSON output is valid JSON.
check_valid_json() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    shift 3

    local output
    output=$(run_datum_cli "$conn" "$sql_type" "$@")
    local rc=$?

    if [ $rc -ne 0 ] && [ $rc -ne 124 ]; then
        echo "  FAIL: $test_name (exit code $rc)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name (exit code $rc)"
        return
    fi

    if echo "$output" | python3 -c "import sys, json; json.load(sys.stdin)" 2>/dev/null; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (invalid JSON)"
        echo "    Output (first 5 lines):"
        echo "$output" | head -5 | sed 's/^/      /'
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name"
    fi
}

# Check CSV output has expected header and data lines.
check_csv() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    local header_pattern="$4"
    local data_pattern="$5"
    shift 5

    local output
    output=$(run_datum_cli "$conn" "$sql_type" "$@")
    local rc=$?

    if [ $rc -ne 0 ] && [ $rc -ne 124 ]; then
        echo "  FAIL: $test_name (exit code $rc)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name (exit code $rc)"
        return
    fi

    local header
    header=$(echo "$output" | head -1)
    if ! echo "$header" | grep -qi "$header_pattern"; then
        echo "  FAIL: $test_name (header pattern '$header_pattern' not found)"
        echo "    Header: $header"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name (header)"
        return
    fi

    if echo "$output" | grep -qi "$data_pattern"; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (data pattern '$data_pattern' not found)"
        echo "    Output (first 5 lines):"
        echo "$output" | head -5 | sed 's/^/      /'
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name (data)"
    fi
}

# --- Driver detection (same as test_integration.sh) ---

detect_driver() {
    local pattern="$1"
    python3 -c "
import pyodbc, re
for d in pyodbc.drivers():
    if re.search('$pattern', d, re.IGNORECASE):
        print(d)
        break
" 2>/dev/null
}

# --- Connectivity probe (non-interactive) ---

probe_backend() {
    local conn="$1"
    local sql_type="$2"
    local output
    output=$(run_datum_cli_stderr "$conn" "$sql_type" --command ":version")
    local rc=$?

    if echo "$output" | grep -qi "CONNECTION ERROR\|Can't open lib\|Data source name not found\|Login failed\|Access denied"; then
        return 1
    fi
    if [ $rc -eq 0 ]; then
        return 0
    fi
    return 1
}

skip_backend() {
    local backend="$1"
    local reason="$2"
    if [ "$STRICT" = "1" ]; then
        echo "FATAL: [$backend] $reason (strict mode)"
        exit 1
    else
        echo "[$backend] $reason — skipping."
    fi
}

# --- Common CLI tests (all backends) ---

run_common_cli_tests() {
    local backend="$1"
    local conn="$2"
    local sql_type="$3"

    echo ""
    echo "--- Common CLI tests ---"

    # --query with default table format
    check_cli "SELECT 1 (table format)" "$conn" "$sql_type" \
        "1" \
        --query "SELECT 1 AS n"

    # --query with JSON format
    check_valid_json "SELECT 1 (valid JSON)" "$conn" "$sql_type" \
        --query "SELECT 1 AS n" --format json

    check_cli "SELECT 1 JSON has key" "$conn" "$sql_type" \
        '"n"' \
        --query "SELECT 1 AS n" --format json

    # --query with CSV format
    check_csv "SELECT 1 (CSV format)" "$conn" "$sql_type" \
        "n" "1" \
        --query "SELECT 1 AS n" --format csv

    # --query with multiple rows
    check_cli "multi-row query (table)" "$conn" "$sql_type" \
        "alice\|Alice\|smith\|Smith" \
        --query "SELECT first_name, last_name FROM customers ORDER BY customer_id"

    check_valid_json "multi-row query (valid JSON)" "$conn" "$sql_type" \
        --query "SELECT first_name, email FROM customers ORDER BY customer_id" --format json

    check_cli "multi-row JSON has data" "$conn" "$sql_type" \
        "alice@example.com\|bob@example.com" \
        --query "SELECT first_name, email FROM customers ORDER BY customer_id" --format json

    check_csv "multi-row CSV" "$conn" "$sql_type" \
        "first_name" "alice\|Alice" \
        --query "SELECT first_name FROM customers ORDER BY customer_id" --format csv

    # --command :tables
    check_cli ":tables lists customers" "$conn" "$sql_type" \
        "customers" \
        --command ":tables"

    check_cli ":tables lists orders" "$conn" "$sql_type" \
        "orders" \
        --command ":tables"

    check_cli ":tables lists products" "$conn" "$sql_type" \
        "products" \
        --command ":tables"

    # --command :columns
    check_cli ":columns customers has customer_id" "$conn" "$sql_type" \
        "customer_id" \
        --command ":columns customers"

    check_cli ":columns customers has email" "$conn" "$sql_type" \
        "email" \
        --command ":columns customers"

    # --command :version
    check_cli ":version returns something" "$conn" "$sql_type" \
        "." \
        --command ":version"

    # --command :user
    check_cli ":user returns something" "$conn" "$sql_type" \
        "." \
        --command ":user"

    # Envelope lines should NOT be on stdout
    check_no_envelope "no envelope on stdout (query)" "$conn" "$sql_type" \
        --query "SELECT 1 AS n"

    check_no_envelope "no envelope on stdout (command)" "$conn" "$sql_type" \
        --command ":tables"

    # Exit code 0 for valid operations
    check_exit_code "exit code 0 for query" "$conn" "$sql_type" \
        --query "SELECT 1 AS n"

    check_exit_code "exit code 0 for command" "$conn" "$sql_type" \
        --command ":tables"

    # --command with and without colon prefix
    check_cli ":tables with colon prefix" "$conn" "$sql_type" \
        "customers" \
        --command ":tables"

    check_cli "tables without colon prefix" "$conn" "$sql_type" \
        "customers" \
        --command "tables"

    # JSON format for commands
    check_cli ":tables as JSON" "$conn" "$sql_type" \
        "customers" \
        --command ":tables" --format json

    # CSV format for commands
    check_cli ":tables as CSV" "$conn" "$sql_type" \
        "customers" \
        --command ":tables" --format csv

    # :databases command
    local db_pattern="datum_test"
    if [ "$sql_type" = "sqlite" ]; then
        db_pattern="main"
    fi
    check_cli ":databases lists database" "$conn" "$sql_type" \
        "$db_pattern" \
        --command ":databases"
}

# --- MySQL/MariaDB-specific CLI tests ---

run_mysql_cli_tests() {
    local conn="$1"
    local sql_type="mysql"

    echo ""
    echo "--- MySQL CLI-specific tests ---"

    check_cli ":schemas lists datum_test" "$conn" "$sql_type" \
        "datum_test" \
        --command ":schemas"

    check_cli ":routines lists format_currency" "$conn" "$sql_type" \
        "format_currency" \
        --command ":routines"

    check_cli ":routines lists place_order" "$conn" "$sql_type" \
        "place_order" \
        --command ":routines"

    check_cli ":version prints MySQL/MariaDB" "$conn" "$sql_type" \
        "MariaDB\|MySQL\|mariadb\|mysql" \
        --command ":version"

    check_valid_json "customers JSON from MySQL" "$conn" "$sql_type" \
        --query "SELECT customer_id, first_name, email FROM customers ORDER BY customer_id LIMIT 3" --format json
}

# --- PostgreSQL-specific CLI tests ---

run_postgres_cli_tests() {
    local conn="$1"
    local sql_type="postgres"

    echo ""
    echo "--- PostgreSQL CLI-specific tests ---"

    check_cli ":schemas lists public" "$conn" "$sql_type" \
        "public" \
        --command ":schemas"

    check_cli ":routines lists format_currency" "$conn" "$sql_type" \
        "format_currency" \
        --command ":routines"

    check_cli ":version prints PostgreSQL" "$conn" "$sql_type" \
        "PostgreSQL\|postgresql" \
        --command ":version"

    check_valid_json "customers JSON from PG" "$conn" "$sql_type" \
        --query "SELECT customer_id, first_name, email FROM customers ORDER BY customer_id LIMIT 3" --format json

    check_csv "customers CSV from PG" "$conn" "$sql_type" \
        "customer_id" "alice\|Alice" \
        --query "SELECT customer_id, first_name FROM customers ORDER BY customer_id LIMIT 3" --format csv
}

# --- MSSQL-specific CLI tests ---

run_mssql_cli_tests() {
    local conn="$1"
    local sql_type="mssql"

    echo ""
    echo "--- MSSQL CLI-specific tests ---"

    check_cli ":schemas lists dbo" "$conn" "$sql_type" \
        "dbo" \
        --command ":schemas"

    check_cli ":routines lists format_currency" "$conn" "$sql_type" \
        "format_currency" \
        --command ":routines"

    check_cli ":routines lists place_order" "$conn" "$sql_type" \
        "place_order" \
        --command ":routines"

    check_cli ":version prints Microsoft" "$conn" "$sql_type" \
        "Microsoft" \
        --command ":version"

    check_valid_json "customers JSON from MSSQL" "$conn" "$sql_type" \
        --query "SELECT TOP 3 customer_id, first_name, email FROM customers ORDER BY customer_id" --format json
}

# --- SQLite-specific CLI tests ---

run_sqlite_cli_tests() {
    local conn="$1"
    local sql_type="sqlite"

    echo ""
    echo "--- SQLite CLI-specific tests ---"

    check_cli ":version prints SQLite version" "$conn" "$sql_type" \
        "3\." \
        --command ":version"

    check_valid_json "customers JSON from SQLite" "$conn" "$sql_type" \
        --query "SELECT customer_id, first_name, email FROM customers ORDER BY customer_id LIMIT 3" --format json

    check_csv "customers CSV from SQLite" "$conn" "$sql_type" \
        "customer_id" "alice\|Alice" \
        --query "SELECT customer_id, first_name FROM customers ORDER BY customer_id LIMIT 3" --format csv

    # View query via CLI
    check_cli "view query via CLI" "$conn" "$sql_type" \
        "order_count\|total_spent\|customer_name" \
        --query "SELECT * FROM customer_order_summary ORDER BY customer_id"
}

# --- Oracle-specific CLI tests ---

run_oracle_cli_tests() {
    local conn="$1"
    local sql_type="oracle"

    echo ""
    echo "--- Oracle CLI-specific tests ---"

    check_cli ":schemas lists DATUM_TEST" "$conn" "$sql_type" \
        "DATUM_TEST" \
        --command ":schemas"

    check_cli ":routines lists FORMAT_CURRENCY" "$conn" "$sql_type" \
        "FORMAT_CURRENCY" \
        --command ":routines"

    check_cli ":version prints Oracle" "$conn" "$sql_type" \
        "Oracle" \
        --command ":version"

    check_valid_json "customers JSON from Oracle" "$conn" "$sql_type" \
        --query "SELECT customer_id, first_name, email FROM customers ORDER BY customer_id FETCH FIRST 3 ROWS ONLY" --format json
}

# --- Backend runner ---

run_backend() {
    local backend="$1"
    local conn="$2"
    local sql_type="$3"

    echo ""
    echo "=== Testing $backend CLI mode ==="
    echo "Connection: $conn"
    echo ""

    echo "Checking connectivity..."
    if ! probe_backend "$conn" "$sql_type"; then
        if [ "$STRICT" = "1" ]; then
            echo "FATAL: Cannot connect to $backend database (strict mode)."
            echo "  Connection string: $conn"
            exit 1
        else
            echo "SKIPPED: Cannot connect to $backend database."
            echo "  Connection string: $conn"
            return
        fi
    fi
    echo "Connection OK."

    run_common_cli_tests "$backend" "$conn" "$sql_type"

    if [ "$sql_type" = "mysql" ]; then
        run_mysql_cli_tests "$conn"
    elif [ "$sql_type" = "postgres" ]; then
        run_postgres_cli_tests "$conn"
    elif [ "$sql_type" = "mssql" ]; then
        run_mssql_cli_tests "$conn"
    elif [ "$sql_type" = "oracle" ]; then
        run_oracle_cli_tests "$conn"
    elif [ "$sql_type" = "sqlite" ]; then
        run_sqlite_cli_tests "$conn"
    fi

    BACKENDS_RAN=$((BACKENDS_RAN + 1))
}

# --- Main ---

echo "=== Datum CLI Mode Integration Tests ==="
echo ""

if [ -n "$DATUM_CONN" ]; then
    SQL_TYPE="${DATUM_SQL_TYPE:-mysql}"
    echo "Using explicit DATUM_CONN (sql_type=$SQL_TYPE)"
    run_backend "manual" "$DATUM_CONN" "$SQL_TYPE"
else
    if [ -n "$DATUM_BACKENDS" ]; then
        IFS=',' read -ra BACKENDS <<< "$DATUM_BACKENDS"
    else
        BACKENDS=(mysql postgres mssql sqlite oracle)
    fi

    for backend in "${BACKENDS[@]}"; do
        case "$backend" in
            mysql)
                DRIVER=$(detect_driver 'mysql|mariadb')
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No MySQL/MariaDB ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                MYSQL_USER="${DATUM_MYSQL_USER:-${DATUM_USER:-root}}"
                MYSQL_PASS="${DATUM_MYSQL_PASS:-${DATUM_PASS:-}}"
                MYSQL_SERVER="${DATUM_MYSQL_SERVER:-${DATUM_SERVER:-localhost}}"
                MYSQL_DB="${DATUM_MYSQL_DB:-${DATUM_DB:-datum_test}}"
                CONN="Driver={${DRIVER}};Server=${MYSQL_SERVER};Database=${MYSQL_DB};User=${MYSQL_USER}"
                if [ -n "$MYSQL_PASS" ]; then
                    CONN="${CONN};Password=${MYSQL_PASS}"
                fi

                run_backend "$backend" "$CONN" "mysql"
                ;;
            postgres)
                DRIVER=$(detect_driver 'PostgreSQL Unicode')
                if [ -z "$DRIVER" ]; then
                    DRIVER=$(detect_driver 'postgres')
                fi
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No PostgreSQL ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                PG_USER="${DATUM_PG_USER:-${DATUM_USER:-${PGUSER:-$(whoami)}}}"
                PG_PASS="${DATUM_PG_PASS:-${DATUM_PASS:-${PGPASSWORD:-}}}"
                PG_SERVER="${DATUM_PG_SERVER:-${DATUM_SERVER:-${PGHOST:-localhost}}}"
                PG_PORT="${DATUM_PG_PORT:-${DATUM_PORT:-${PGPORT:-5432}}}"
                PG_DB="${DATUM_PG_DB:-${DATUM_DB:-${PGDATABASE:-datum_test}}}"
                CONN="Driver={${DRIVER}};Server=${PG_SERVER};Port=${PG_PORT};Database=${PG_DB};Uid=${PG_USER}"
                if [ -n "$PG_PASS" ]; then
                    CONN="${CONN};Pwd=${PG_PASS}"
                fi

                run_backend "$backend" "$CONN" "postgres"
                ;;
            sqlite)
                DRIVER=$(detect_driver 'SQLite3')
                if [ -z "$DRIVER" ]; then
                    DRIVER=$(detect_driver 'sqlite')
                fi
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No SQLite ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                SQLITE_DB="${DATUM_SQLITE_DB:-/tmp/datum_test.db}"
                if [ ! -f "$SQLITE_DB" ]; then
                    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
                    SETUP_SCRIPT="$SCRIPT_DIR/../../test-sqlite.sh"
                    if [ -f "$SETUP_SCRIPT" ] && command -v sqlite3 &>/dev/null; then
                        echo "[$backend] Creating test database..."
                        bash "$SETUP_SCRIPT"
                    fi
                    if [ ! -f "$SQLITE_DB" ]; then
                        skip_backend "$backend" "Database file $SQLITE_DB not found"
                        continue
                    fi
                fi

                CONN="Driver={${DRIVER}};Database=${SQLITE_DB}"
                run_backend "$backend" "$CONN" "sqlite"
                ;;
            mssql)
                DRIVER=$(detect_driver 'ODBC Driver.*SQL Server')
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No SQL Server ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                MSSQL_USER="${DATUM_MSSQL_USER:-${DATUM_USER:-sa}}"
                MSSQL_PASS="${DATUM_MSSQL_PASS:-${DATUM_PASS:-DatumTest1!}}"
                MSSQL_SERVER="${DATUM_MSSQL_SERVER:-${DATUM_SERVER:-localhost}}"
                MSSQL_PORT="${DATUM_MSSQL_PORT:-${DATUM_PORT:-1433}}"
                MSSQL_DB="${DATUM_MSSQL_DB:-${DATUM_DB:-datum_test}}"
                CONN="Driver={${DRIVER}};Server=${MSSQL_SERVER},${MSSQL_PORT};Database=${MSSQL_DB};Uid=${MSSQL_USER};Pwd=${MSSQL_PASS};TrustServerCertificate=yes"

                run_backend "$backend" "$CONN" "mssql"
                ;;
            oracle)
                DRIVER=$(detect_driver 'Oracle')
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No Oracle ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                ORA_USER="${DATUM_ORACLE_USER:-${DATUM_USER:-datum_test}}"
                ORA_PASS="${DATUM_ORACLE_PASS:-${DATUM_PASS:-datum_test}}"
                ORA_SERVER="${DATUM_ORACLE_SERVER:-${DATUM_SERVER:-localhost}}"
                ORA_PORT="${DATUM_ORACLE_PORT:-${DATUM_PORT:-1521}}"
                ORA_SERVICE="${DATUM_ORACLE_SERVICE:-FREEPDB1}"
                CONN="Driver={${DRIVER}};DBQ=${ORA_SERVER}:${ORA_PORT}/${ORA_SERVICE};Uid=${ORA_USER};Pwd=${ORA_PASS}"

                run_backend "$backend" "$CONN" "oracle"
                ;;
            *)
                echo "Unknown backend: $backend — skipping."
                ;;
        esac
    done
fi

# --- Summary ---

echo ""
echo "=== Results ==="
echo "Backends tested: $BACKENDS_RAN"
echo "Passed: $PASS"
echo "Failed: $FAIL"

if [ $BACKENDS_RAN -eq 0 ]; then
    echo ""
    if [ "$STRICT" = "1" ]; then
        echo "FATAL: No backends were available (strict mode)."
        exit 1
    else
        echo "No backends were available. Skipping."
        echo "Install an ODBC driver and ensure a datum_test database is accessible."
        exit 0
    fi
fi

if [ $FAIL -gt 0 ]; then
    echo -e "\nFailures:$ERRORS"
    exit 1
fi

echo "All tests passed."
exit 0
