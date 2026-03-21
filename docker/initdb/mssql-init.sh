#!/bin/bash
# Initialize MSSQL test database. Runs as a one-shot container
# after the mssql service is healthy.
set -e

/opt/mssql-tools18/bin/sqlcmd \
    -S mssql \
    -U sa \
    -P 'DatumTest1!' \
    -C \
    -i /tmp/test-mssql.sql

echo "MSSQL test database initialized successfully."
