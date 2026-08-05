#!/bin/bash
# Initialize MSSQL test databases. Runs as a one-shot container
# after the mssql service is healthy.
set -e

/opt/mssql-tools18/bin/sqlcmd \
    -S mssql \
    -U sa \
    -P 'DatumTest1!' \
    -C \
    -i /tmp/test-mssql.sql

echo "MSSQL test database initialized successfully."

# Large database for background-introspection stress testing (optional).
if [ -f /tmp/test-mssql-big.sql ]; then
    echo "Creating datum_big (60k tables) — this may take a few minutes..."
    /opt/mssql-tools18/bin/sqlcmd \
        -S mssql \
        -U sa \
        -P 'DatumTest1!' \
        -C \
        -i /tmp/test-mssql-big.sql
    echo "datum_big initialized successfully."
fi
