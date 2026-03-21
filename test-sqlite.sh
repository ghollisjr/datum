#!/bin/bash
# Create a SQLite test database for datum integration tests.
#
# Usage: bash test-sqlite.sh

DB="/tmp/datum_test.db"

rm -f "$DB"

sqlite3 "$DB" <<'SQL'
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name  TEXT NOT NULL,
    last_name   TEXT NOT NULL,
    email       TEXT UNIQUE,
    active      INTEGER DEFAULT 1
);

CREATE TABLE products (
    product_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    price       REAL NOT NULL
);

CREATE TABLE orders (
    order_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    order_date  TEXT DEFAULT (date('now')),
    total       REAL DEFAULT 0,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    item_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 1,
    unit_price  REAL NOT NULL,
    FOREIGN KEY (order_id)   REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE VIEW customer_order_summary AS
SELECT c.customer_id,
       c.first_name || ' ' || c.last_name AS customer_name,
       COUNT(o.order_id) AS order_count,
       COALESCE(SUM(o.total), 0) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Sample data
INSERT INTO customers (first_name, last_name, email) VALUES
    ('Alice', 'Smith', 'alice@example.com'),
    ('Bob',   'Jones', 'bob@example.com'),
    ('Carol', 'White', 'carol@example.com');

INSERT INTO products (name, price) VALUES
    ('Widget', 9.99),
    ('Gadget', 24.99);

INSERT INTO orders (customer_id, order_date, total) VALUES
    (1, '2025-01-15', 34.98),
    (2, '2025-01-16', 9.99);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 9.99),
    (1, 2, 1, 24.99);
SQL

echo "Created test database: $DB"
sqlite3 "$DB" "SELECT 'Tables: ' || COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
sqlite3 "$DB" "SELECT 'Views:  ' || COUNT(*) FROM sqlite_master WHERE type='view';"
sqlite3 "$DB" "SELECT 'Rows:   ' || COUNT(*) FROM customers;"
