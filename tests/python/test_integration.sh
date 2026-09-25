#!/bin/bash
# Integration tests for datum against live databases.
#
# Supports MySQL/MariaDB, PostgreSQL, and SQLite backends. Auto-detects
# available ODBC drivers and skips unavailable backends gracefully.
#
# Usage:
#   bash tests/python/test_integration.sh
#
# Override with explicit connection:
#   DATUM_CONN="Driver={...};Server=...;Database=datum_test;..." \
#     bash tests/python/test_integration.sh
#
# Control which backends to test:
#   DATUM_BACKENDS=postgres bash tests/python/test_integration.sh
#   DATUM_BACKENDS=mysql,postgres,sqlite bash tests/python/test_integration.sh
#
# Strict mode (for CI/Docker — no silent skips, every requested backend must pass):
#   DATUM_STRICT=1 DATUM_BACKENDS=mysql,postgres,mssql,sqlite \
#     bash tests/python/test_integration.sh

set -o pipefail

TIMEOUT_CMD="timeout"
TIMEOUT_SECS=10
STRICT="${DATUM_STRICT:-0}"

# Check that datum is available
if ! command -v datum &>/dev/null; then
    DATUM_CMD="python -m datum"
else
    DATUM_CMD="datum"
fi

# In strict mode, skips are fatal errors.
skip_backend() {
    local backend="$1"
    local reason="$2"
    if [ "$STRICT" = "1" ]; then
        echo "FATAL: [$backend] $reason (strict mode — skips are not allowed)"
        exit 1
    else
        echo "[$backend] $reason — skipping."
    fi
}

# --- Test helpers ---

PASS=0
FAIL=0
ERRORS=""
BACKENDS_RAN=0

run_datum() {
    local conn="$1"
    local sql_type="$2"
    local input="$3"
    $TIMEOUT_CMD ${TIMEOUT_SECS} $DATUM_CMD \
        --conn-string="$conn" --sql-type="$sql_type" <<< "$input" 2>&1
}

check_output() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    local input="$4"
    local pattern="$5"

    local output
    output=$(run_datum "$conn" "$sql_type" "$input")
    local rc=$?

    # Exit code 1 is expected when stdin is piped (EOFError after input consumed).
    # Only treat exit codes other than 0, 1, and 124 (timeout) as failures.
    if [ $rc -ne 0 ] && [ $rc -ne 1 ] && [ $rc -ne 124 ]; then
        echo "  FAIL: $test_name (datum exited with code $rc)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: $test_name (exit code $rc)"
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

check_envelope() {
    local test_name="$1"
    local conn="$2"
    local sql_type="$3"
    local input="$4"

    local output
    output=$(run_datum "$conn" "$sql_type" "$input")

    if echo "$output" | grep -q '##DATUM:'; then
        echo "  PASS: $test_name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (no ##DATUM: envelope lines found)"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  FAIL: [$sql_type] $test_name"
    fi
}

# --- Driver detection ---

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

# --- Connectivity probe ---

probe_backend() {
    local conn="$1"
    local sql_type="$2"
    local output
    output=$(run_datum "$conn" "$sql_type" ":version")
    local rc=$?

    if echo "$output" | grep -qi "CONNECTION ERROR\|Can't open lib\|Data source name not found\|Login failed\|Access denied"; then
        return 1
    fi
    # A successful connection prints "Server version:" or a ##DATUM:meta:version line.
    # Exit code 1 is expected when stdin is piped (EOFError), so check output instead.
    if echo "$output" | grep -qi "Server version\|meta:version"; then
        return 0
    fi
    if [ $rc -ne 0 ] && [ $rc -ne 124 ]; then
        return 1
    fi
    return 0
}

# --- Common tests (run for all backends) ---

run_common_tests() {
    local backend="$1"
    local conn="$2"
    local sql_type="$3"

    # SQLite returns "main" for databases, others return "datum_test"
    local db_pattern="datum_test"
    if [ "$sql_type" = "sqlite" ]; then
        db_pattern="main"
    fi
    check_output ":databases lists database" "$conn" "$sql_type" \
        ":databases" "$db_pattern"

    check_output ":tables lists customers" "$conn" "$sql_type" \
        ":tables" "customers"

    check_output ":tables lists orders" "$conn" "$sql_type" \
        ":tables" "orders"

    check_output ":tables lists customer_order_summary" "$conn" "$sql_type" \
        ":tables" "customer_order_summary"

    check_output ":columns customers has customer_id" "$conn" "$sql_type" \
        ":columns customers" "customer_id"

    check_output ":columns customers has first_name" "$conn" "$sql_type" \
        ":columns customers" "first_name"

    check_output ":columns customers has email" "$conn" "$sql_type" \
        ":columns customers" "email"

    check_output ":user prints current user" "$conn" "$sql_type" \
        ":user" "."

    # ANSI driver does not support :definition (tested separately in run_ansi_tests)
    if [ "$sql_type" != "ansi" ]; then
        check_output ":definition customers shows column info" "$conn" "$sql_type" \
            ":definition customers" "customer_id\|COLUMN_NAME\|CREATE"
    fi

    check_output "SELECT query returns data" "$conn" "$sql_type" \
        "SELECT 1 AS test_value;" "1\|test_value"

    check_envelope "Envelope lines appear in stdout" "$conn" "$sql_type" \
        ":tables"
}

# --- MySQL/MariaDB-specific tests ---

run_mysql_tests() {
    local conn="$1"
    local sql_type="mysql"

    echo ""
    echo "--- MySQL/MariaDB-specific tests ---"

    check_output ":schemas lists datum_test" "$conn" "$sql_type" \
        ":schemas" "datum_test"

    check_output ":tables lists products" "$conn" "$sql_type" \
        ":tables" "products"

    check_output ":routines lists format_currency" "$conn" "$sql_type" \
        ":routines" "format_currency"

    check_output ":routines lists place_order" "$conn" "$sql_type" \
        ":routines" "place_order"

    check_output ":routines lists deactivate_customer" "$conn" "$sql_type" \
        ":routines" "deactivate_customer"

    check_output ":version prints MySQL/MariaDB" "$conn" "$sql_type" \
        ":version" "MariaDB\|MySQL\|mariadb\|mysql"

    check_output ":definition customer_order_summary shows view" "$conn" "$sql_type" \
        ":definition customer_order_summary" "definition\|VIEW\|SELECT\|view"

    check_output ":definition format_currency shows function" "$conn" "$sql_type" \
        ":definition format_currency" "definition\|ROUTINE\|FUNCTION\|function"

    check_output ":definition datum_test shows database properties" "$conn" "$sql_type" \
        ":definition datum_test" "charset\|collation\|CHARACTER_SET\|character_set"
}

# --- PostgreSQL-specific tests ---

run_postgres_tests() {
    local conn="$1"
    local sql_type="postgres"

    echo ""
    echo "--- PostgreSQL-specific tests ---"

    check_output ":schemas lists public" "$conn" "$sql_type" \
        ":schemas" "public"

    check_output ":tables lists products" "$conn" "$sql_type" \
        ":tables" "products"

    check_output ":routines lists format_currency" "$conn" "$sql_type" \
        ":routines" "format_currency"

    check_output ":version prints PostgreSQL" "$conn" "$sql_type" \
        ":version" "PostgreSQL\|postgresql"

    check_output ":definition customer_order_summary shows view" "$conn" "$sql_type" \
        ":definition customer_order_summary" "definition\|VIEW\|SELECT\|view"

    check_output ":definition deactivate_customer shows procedure" "$conn" "$sql_type" \
        ":definition deactivate_customer" "definition\|ROUTINE\|FUNCTION\|PROCEDURE\|function\|procedure"

    check_output ":definition datum_test shows database properties" "$conn" "$sql_type" \
        ":definition datum_test" "charset\|collation\|encoding\|ctype\|owner"
}

# --- SQLite-specific tests ---

run_sqlite_tests() {
    local conn="$1"
    local sql_type="sqlite"

    echo ""
    echo "--- SQLite-specific tests ---"

    check_output ":version prints SQLite version" "$conn" "$sql_type" \
        ":version" "3\."

    check_output ":tables lists products" "$conn" "$sql_type" \
        ":tables" "products"

    check_output ":definition customer_order_summary shows view" "$conn" "$sql_type" \
        ":definition customer_order_summary" "VIEW\|SELECT\|view"

    check_output ":routines gracefully errors" "$conn" "$sql_type" \
        ":routines" "not supported\|NotImplementedError\|error\|Error"
}

# --- MSSQL-specific tests ---

run_mssql_tests() {
    local conn="$1"
    local sql_type="mssql"

    echo ""
    echo "--- MSSQL-specific tests ---"

    check_output ":schemas lists dbo" "$conn" "$sql_type" \
        ":schemas" "dbo"

    check_output ":tables lists products" "$conn" "$sql_type" \
        ":tables" "products"

    check_output ":routines lists format_currency" "$conn" "$sql_type" \
        ":routines" "format_currency"

    check_output ":routines lists place_order" "$conn" "$sql_type" \
        ":routines" "place_order"

    check_output ":routines lists deactivate_customer" "$conn" "$sql_type" \
        ":routines" "deactivate_customer"

    check_output ":version prints Microsoft" "$conn" "$sql_type" \
        ":version" "Microsoft"

    check_output ":definition customer_order_summary shows view" "$conn" "$sql_type" \
        ":definition customer_order_summary" "definition\|VIEW\|SELECT\|view"

    check_output ":definition format_currency shows function" "$conn" "$sql_type" \
        ":definition format_currency" "definition\|ROUTINE\|FUNCTION\|function"

    check_output ":definition datum_test shows database properties" "$conn" "$sql_type" \
        ":definition datum_test" "collation\|owner\|compatibility"
}

# --- Oracle-specific tests ---

run_oracle_tests() {
    local conn="$1"
    local sql_type="oracle"

    echo ""
    echo "--- Oracle-specific tests ---"

    check_output ":schemas lists DATUM_TEST" "$conn" "$sql_type" \
        ":schemas" "DATUM_TEST"

    check_output ":tables lists PRODUCTS" "$conn" "$sql_type" \
        ":tables" "PRODUCTS"

    check_output ":routines lists FORMAT_CURRENCY" "$conn" "$sql_type" \
        ":routines" "FORMAT_CURRENCY"

    check_output ":routines lists PLACE_ORDER" "$conn" "$sql_type" \
        ":routines" "PLACE_ORDER"

    check_output ":routines lists DEACTIVATE_CUSTOMER" "$conn" "$sql_type" \
        ":routines" "DEACTIVATE_CUSTOMER"

    check_output ":version prints Oracle" "$conn" "$sql_type" \
        ":version" "Oracle"

    check_output ":definition CUSTOMER_ORDER_SUMMARY shows view" "$conn" "$sql_type" \
        ":definition CUSTOMER_ORDER_SUMMARY" "definition\|VIEW\|SELECT\|view"

    check_output ":definition FORMAT_CURRENCY shows function" "$conn" "$sql_type" \
        ":definition FORMAT_CURRENCY" "definition\|FUNCTION\|RETURN\|function"
}

# --- ANSI-specific tests ---
# Tests the generic ANSI/INFORMATION_SCHEMA driver against PostgreSQL.

run_ansi_tests() {
    local conn="$1"
    local sql_type="ansi"

    echo ""
    echo "--- ANSI-specific tests ---"

    check_output ":schemas lists public" "$conn" "$sql_type" \
        ":schemas" "public"

    check_output ":tables lists customers" "$conn" "$sql_type" \
        ":tables" "customers"

    check_output ":version returns unknown" "$conn" "$sql_type" \
        ":version" "unknown"

    check_output ":routines gracefully errors" "$conn" "$sql_type" \
        ":routines" "not supported\|NotImplementedError\|error\|Error"

    check_output ":definition gracefully errors" "$conn" "$sql_type" \
        ":definition customers" "not supported\|NotImplementedError\|error\|Error"
}

# --- Backend runner ---

run_backend() {
    local backend="$1"
    local conn="$2"
    local sql_type="$3"

    echo ""
    echo "=== Testing $backend backend ==="
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

    echo ""
    echo "--- Common tests ---"
    run_common_tests "$backend" "$conn" "$sql_type"

    if [ "$sql_type" = "mysql" ]; then
        run_mysql_tests "$conn"
    elif [ "$sql_type" = "postgres" ]; then
        run_postgres_tests "$conn"
    elif [ "$sql_type" = "mssql" ]; then
        run_mssql_tests "$conn"
    elif [ "$sql_type" = "oracle" ]; then
        run_oracle_tests "$conn"
    elif [ "$sql_type" = "ansi" ]; then
        run_ansi_tests "$conn"
    elif [ "$sql_type" = "sqlite" ]; then
        run_sqlite_tests "$conn"
    fi

    BACKENDS_RAN=$((BACKENDS_RAN + 1))
}

# --- Main ---

echo "=== Datum Multi-Backend Integration Tests ==="
echo ""

# If DATUM_CONN is set, use it directly (legacy single-backend mode)
if [ -n "$DATUM_CONN" ]; then
    SQL_TYPE="${DATUM_SQL_TYPE:-mysql}"
    echo "Using explicit DATUM_CONN (sql_type=$SQL_TYPE)"
    run_backend "manual" "$DATUM_CONN" "$SQL_TYPE"
else
    # Determine which backends to try
    if [ -n "$DATUM_BACKENDS" ]; then
        IFS=',' read -ra BACKENDS <<< "$DATUM_BACKENDS"
    else
        BACKENDS=(mysql postgres mssql sqlite ansi)
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
                    # Fall back to any PostgreSQL driver
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
                    # Fall back to any SQLite driver
                    DRIVER=$(detect_driver 'sqlite')
                fi
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No SQLite ODBC driver found"
                    continue
                fi
                echo "[$backend] Detected driver: $DRIVER"

                SQLITE_DB="${DATUM_SQLITE_DB:-/tmp/datum_test.db}"
                if [ ! -f "$SQLITE_DB" ]; then
                    # Try to create it automatically
                    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
                    SETUP_SCRIPT="$SCRIPT_DIR/../../test-sqlite.sh"
                    if [ -f "$SETUP_SCRIPT" ] && command -v sqlite3 &>/dev/null; then
                        echo "[$backend] Creating test database..."
                        bash "$SETUP_SCRIPT"
                    fi
                    if [ ! -f "$SQLITE_DB" ]; then
                        skip_backend "$backend" "Database file $SQLITE_DB not found and could not be created"
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
            ansi)
                # ANSI driver test: reuses the PostgreSQL connection with sql_type=ansi.
                # This verifies the generic INFORMATION_SCHEMA fallback driver works.
                DRIVER=$(detect_driver 'PostgreSQL Unicode')
                if [ -z "$DRIVER" ]; then
                    DRIVER=$(detect_driver 'postgres')
                fi
                if [ -z "$DRIVER" ]; then
                    skip_backend "$backend" "No PostgreSQL ODBC driver found (needed for ANSI test)"
                    continue
                fi
                echo "[$backend] Using PostgreSQL driver for ANSI test: $DRIVER"

                ANSI_USER="${DATUM_PG_USER:-${DATUM_USER:-${PGUSER:-$(whoami)}}}"
                ANSI_PASS="${DATUM_PG_PASS:-${DATUM_PASS:-${PGPASSWORD:-}}}"
                ANSI_SERVER="${DATUM_PG_SERVER:-${DATUM_SERVER:-${PGHOST:-localhost}}}"
                ANSI_PORT="${DATUM_PG_PORT:-${DATUM_PORT:-${PGPORT:-5432}}}"
                ANSI_DB="${DATUM_PG_DB:-${DATUM_DB:-${PGDATABASE:-datum_test}}}"
                CONN="Driver={${DRIVER}};Server=${ANSI_SERVER};Port=${ANSI_PORT};Database=${ANSI_DB};Uid=${ANSI_USER}"
                if [ -n "$ANSI_PASS" ]; then
                    CONN="${CONN};Pwd=${ANSI_PASS}"
                fi

                run_backend "$backend" "$CONN" "ansi"
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
        echo "FATAL: No backends were available (strict mode — at least one must pass)."
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
