#!/bin/bash
# Master test runner for datum.
#
# Usage: cd tests && bash run-all.sh
#    or: bash tests/run-all.sh

set -e

cd "$(dirname "$0")"

echo "=== Python unit tests ==="
python -m pytest python/ -v
echo ""

echo "=== Python integration tests ==="
echo "(Requires test databases and ODBC drivers; set DATUM_STRICT=1 to fail on skips)"
bash python/test_integration.sh
echo ""

echo "=== Emacs client tests ==="
if command -v emacs &>/dev/null; then
    bash emacs/run-tests.sh
else
    echo "Emacs not found — skipping Emacs tests."
fi
echo ""

echo "=== All tests complete ==="
