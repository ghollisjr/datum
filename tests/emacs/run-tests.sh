#!/bin/bash
# Run every Emacs batch-mode test suite for sql-datum.
#
# Usage: bash tests/emacs/run-tests.sh
#
# Each suite is a test-*.el file that loads sql-datum.el and prints its
# own pass/fail tally.  A suite fails the run if it prints a FAIL line
# or a non-zero failure count, so adding a new test-*.el file is all it
# takes to have it run here.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

status=0
total_suites=0
failed_suites=0

for suite in "$SCRIPT_DIR"/test-*.el; do
    name="$(basename "$suite")"
    total_suites=$((total_suites + 1))
    echo "=== $name ==="

    output="$(emacs --batch -l "$REPO_ROOT/sql-datum.el" -l "$suite" 2>&1)"
    echo "$output"

    if [ -n "$(printf '%s\n' "$output" | grep -E 'FAIL|Failed: [1-9]|[1-9][0-9]* failed')" ]; then
        failed_suites=$((failed_suites + 1))
        status=1
        echo ">>> $name FAILED"
    fi
    echo ""
done

echo "=== $((total_suites - failed_suites))/$total_suites suites passed ==="
exit $status
