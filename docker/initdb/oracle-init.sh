#!/bin/bash
# Initialize Oracle test database. Runs as a one-shot container
# after the oracle service is healthy.
set -e

sqlplus datum_test/datum_test@oracle:1521/FREEPDB1 @/tmp/test-oracle.sql

echo "Oracle test database initialized successfully."
