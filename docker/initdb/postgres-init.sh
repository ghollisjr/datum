#!/bin/bash
# Strip database creation lines from test-postgres.sql since Docker
# already creates datum_test from POSTGRES_DB env var.
sed '/^DROP DATABASE/d; /^CREATE DATABASE/d; /^\\connect/d' \
  /tmp/test-postgres.sql \
  | psql -U postgres -d datum_test
