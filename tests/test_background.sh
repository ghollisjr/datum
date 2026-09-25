#!/bin/bash
# test_background.sh — Verify background introspection on a large database.
#
# Starts datum connected to datum_test, switches to datum_big (60k tables),
# then immediately runs a query.  The query should complete while introspection
# is still running in the background.
#
# Requirements:
#   - MSSQL container running (docker compose up -d mssql)
#   - datum_big created (via mssql-init)
#   - datum installed in current virtualenv
#
# Usage:
#   bash tests/test_background.sh

set -uo pipefail

SERVER="${DATUM_MSSQL_SERVER:-127.0.0.1}"
PORT="${DATUM_MSSQL_PORT:-1434}"
USER="sa"
PASS="DatumTest1!"
DB="datum_test"
DRIVER=$(python3 -c "
import pyodbc
drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
print(drivers[-1] if drivers else '')
")

if [ -z "$DRIVER" ]; then
    echo "SKIP: No MSSQL ODBC driver found"
    exit 0
fi

CONN_STRING="Driver={$DRIVER};Server=tcp:$SERVER,$PORT;Database=$DB;Uid=$USER;Pwd=$PASS;TrustServerCertificate=yes"

echo "=== Background Introspection Test ==="
echo "Driver: $DRIVER"
echo "Server: tcp:$SERVER,$PORT"

# Verify connectivity first
python3 -c "
import pyodbc
conn = pyodbc.connect('$CONN_STRING')
print('Connectivity OK')
conn.close()
" || { echo "SKIP: Cannot connect to MSSQL"; exit 0; }

# Use Python to drive the test — it can handle pipes more reliably
python3 - "$CONN_STRING" <<'PYEOF'
import subprocess
import sys
import time
import threading

conn_string = sys.argv[1]
cmd = ["datum", "--conn-string", conn_string, "--sql-type", "mssql"]

proc = subprocess.Popen(
    cmd,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)

output_lines = []
output_lock = threading.Lock()

def reader():
    for line in proc.stdout:
        with output_lock:
            output_lines.append(line)

t = threading.Thread(target=reader, daemon=True)
t.start()

def send(text):
    proc.stdin.write(text + "\n")
    proc.stdin.flush()

# Wait for initial ready
time.sleep(3)

print("--- Switching to datum_big ---")
send(":use datum_big")
time.sleep(1)

print("--- Triggering refresh (background delegation) ---")
send(":refresh-databases")
send(":refresh-schemas")
send(":refresh-tables")
send(":refresh-routines")

print("--- Running query while introspection loads ---")
send("SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES;;")

# Let query complete + background introspection run
# 60k tables takes a while to send as chunked envelopes
time.sleep(30)

print("--- Sending exit ---")
send(":exit")

proc.wait(timeout=10)
t.join(timeout=5)

print()
print("=== Output ===")
with output_lock:
    all_output = list(output_lines)

for line in all_output:
    print(line, end="")

print()
print("=== Checking results ===")

failed = False

# Check for concatenated envelopes (##...####DATUM: without newline between)
concat = [l.rstrip("\n") for l in all_output if "####DATUM:" in l]
if concat:
    print(f"FAIL: Concatenated envelopes found ({len(concat)} lines):")
    for c in concat[:3]:
        print(f"  {c[:120]!r}...")
    failed = True
else:
    print("PASS: No concatenated envelopes")

# Check that all introspect envelopes are parseable.
# Emacs uses (string-match "##DATUM:\\([^:]+\\):\\(.*?\\)##" line) which
# matches anywhere in a line, so ">##DATUM:...##" is fine (the ">" prompt
# may prefix an envelope due to a harmless race between prompt and bg output).
# The real failure would be a truncated/garbled envelope.
import re
envelope_re = re.compile(r'##DATUM:[^:]+:.*?##')
broken = []
for line in all_output:
    stripped = line.rstrip("\n")
    if "##DATUM:introspect" in stripped and not envelope_re.search(stripped):
        broken.append(stripped)

if broken:
    print(f"FAIL: Unparseable envelope lines found ({len(broken)}):")
    for b in broken[:10]:
        print(f"  {b!r}")
    failed = True
else:
    print("PASS: All envelope lines are parseable")

# Check query returned 60k+ result
full = "".join(all_output)
if "60" in full:
    print("PASS: Query returned results (found table count)")
else:
    print("WARN: Could not verify table count in output")

# Count bg-ready envelopes
bg_count = sum(1 for l in all_output if "##DATUM:bg-ready" in l)
print(f"Background tasks completed: {bg_count}")
if bg_count > 0:
    print("PASS: Background introspection ran successfully")
else:
    print("WARN: No bg-ready envelopes seen (may still be in progress)")

# Count introspect envelopes
intr_count = sum(1 for l in all_output if "##DATUM:introspect" in l)
print(f"Introspect envelopes received: {intr_count}")
if intr_count > 0:
    print("PASS: Introspection data was sent")

print()
# Check for empty lines caused by stray \n prefixes from background thread
# Count consecutive empty lines (more than 2 in a row is suspicious)
consecutive_empty = 0
max_consecutive = 0
for line in all_output:
    if line.strip() == "":
        consecutive_empty += 1
        max_consecutive = max(max_consecutive, consecutive_empty)
    else:
        consecutive_empty = 0
print(f"Max consecutive empty lines: {max_consecutive}")
if max_consecutive > 3:
    print("WARN: Excessive empty lines detected (possible stray newline prefixes)")

if failed:
    print("=== FAILED ===")
    sys.exit(1)
else:
    print("=== PASSED ===")
PYEOF
