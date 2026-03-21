-- test-mysql.sql
-- Creates a test database with tables, views, functions & procedures
-- for exercising datum's MySQL/MariaDB introspection features.
--
-- Usage:
--   1. Connect as root:  mysql -u root
--   2. Run:  source test-mysql.sql
--      (or:  mysql -u root < test-mysql.sql)
--   3. Connect to the new db:  mysql -u root datum_test
--      (or use datum with the datum_test database)
--
-- Note: MySQL has no separate schema layer — each database IS a schema.
--       To test cross-schema objects, create additional databases.

-- ============================================================
-- Database
-- ============================================================
DROP DATABASE IF EXISTS datum_test;
CREATE DATABASE datum_test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
USE datum_test;

-- ============================================================
-- Tables
-- ============================================================

CREATE TABLE customers (
    customer_id   INT AUTO_INCREMENT PRIMARY KEY,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) UNIQUE,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_active     TINYINT(1) DEFAULT 1
) ENGINE=InnoDB;

CREATE TABLE orders (
    order_id      INT AUTO_INCREMENT PRIMARY KEY,
    customer_id   INT NOT NULL,
    order_date    DATE NOT NULL DEFAULT (CURRENT_DATE),
    total_amount  DECIMAL(12,2) NOT NULL DEFAULT 0,
    status        VARCHAR(20) DEFAULT 'pending',
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

CREATE TABLE order_items (
    item_id       INT AUTO_INCREMENT PRIMARY KEY,
    order_id      INT NOT NULL,
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
) ENGINE=InnoDB;

CREATE TABLE products (
    product_id    INT AUTO_INCREMENT PRIMARY KEY,
    sku           VARCHAR(50) UNIQUE NOT NULL,
    name          VARCHAR(200) NOT NULL,
    description   TEXT,
    category      VARCHAR(100),
    unit_price    DECIMAL(10,2) NOT NULL,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE stock_levels (
    stock_id      INT AUTO_INCREMENT PRIMARY KEY,
    product_id    INT NOT NULL,
    warehouse     VARCHAR(100) NOT NULL,
    quantity      INT NOT NULL DEFAULT 0,
    last_updated  DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_stock_product
        FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

CREATE TABLE suppliers (
    supplier_id   INT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(200) NOT NULL,
    contact_email VARCHAR(255),
    phone         VARCHAR(50),
    country       VARCHAR(100)
) ENGINE=InnoDB;

CREATE TABLE daily_sales (
    report_date   DATE PRIMARY KEY,
    total_orders  INT NOT NULL DEFAULT 0,
    total_revenue DECIMAL(14,2) NOT NULL DEFAULT 0,
    avg_order     DECIMAL(10,2)
) ENGINE=InnoDB;

CREATE TABLE customer_segments (
    segment_id    INT AUTO_INCREMENT PRIMARY KEY,
    segment_name  VARCHAR(100) NOT NULL,
    min_spend     DECIMAL(12,2),
    max_spend     DECIMAL(12,2),
    description   TEXT
) ENGINE=InnoDB;

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW customer_order_summary AS
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.email,
    COUNT(o.order_id) AS order_count,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_spend,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

CREATE VIEW low_stock AS
SELECT
    p.product_id,
    p.sku,
    p.name,
    sl.warehouse,
    sl.quantity,
    sl.last_updated
FROM products p
JOIN stock_levels sl ON p.product_id = sl.product_id
WHERE sl.quantity < 10;

CREATE VIEW revenue_by_category AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category;

-- ============================================================
-- Functions
-- ============================================================

DELIMITER //

-- Simple scalar function
CREATE FUNCTION format_currency(amount DECIMAL(12,2), symbol VARCHAR(5))
RETURNS VARCHAR(50)
DETERMINISTIC
BEGIN
    RETURN CONCAT(symbol, FORMAT(amount, 2));
END //

-- Function with default-style logic
CREATE FUNCTION is_valid_email(addr VARCHAR(255))
RETURNS TINYINT(1)
DETERMINISTIC
BEGIN
    RETURN addr REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z]{2,}$';
END //

-- Function returning computed value
CREATE FUNCTION customer_lifetime_value(p_customer_id INT)
RETURNS DECIMAL(14,2)
READS SQL DATA
BEGIN
    DECLARE total DECIMAL(14,2);
    SELECT COALESCE(SUM(total_amount), 0) INTO total
    FROM orders
    WHERE customer_id = p_customer_id;
    RETURN total;
END //

-- ============================================================
-- Procedures
-- ============================================================

-- Simple procedure
CREATE PROCEDURE deactivate_customer(IN p_customer_id INT)
BEGIN
    UPDATE customers
    SET is_active = 0
    WHERE customer_id = p_customer_id;
END //

-- Procedure with multiple parameters
CREATE PROCEDURE place_order(
    IN p_customer_id INT,
    IN p_product_id INT,
    IN p_quantity INT,
    IN p_status VARCHAR(20)
)
BEGIN
    DECLARE v_order_id INT;
    DECLARE v_price DECIMAL(10,2);

    -- Look up product price
    SELECT unit_price INTO v_price
    FROM products
    WHERE product_id = p_product_id;

    IF v_price IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Product not found';
    END IF;

    -- Create order
    INSERT INTO orders (customer_id, order_date, total_amount, status)
    VALUES (p_customer_id, CURRENT_DATE, v_price * p_quantity, p_status);

    SET v_order_id = LAST_INSERT_ID();

    -- Create order item
    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
    VALUES (v_order_id, p_product_id, p_quantity, v_price);

    -- Update stock
    UPDATE stock_levels
    SET quantity = quantity - p_quantity,
        last_updated = CURRENT_TIMESTAMP
    WHERE product_id = p_product_id;
END //

-- Procedure that rebuilds analytics
CREATE PROCEDURE rebuild_daily_sales(
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DELETE FROM daily_sales
    WHERE report_date BETWEEN p_start_date AND p_end_date;

    INSERT INTO daily_sales (report_date, total_orders, total_revenue, avg_order)
    SELECT
        o.order_date,
        COUNT(*),
        SUM(o.total_amount),
        AVG(o.total_amount)
    FROM orders o
    WHERE o.order_date BETWEEN p_start_date AND p_end_date
    GROUP BY o.order_date;
END //

-- Procedure with OUT parameter
CREATE PROCEDURE get_customer_stats(
    IN p_customer_id INT,
    OUT p_order_count INT,
    OUT p_total_spend DECIMAL(14,2)
)
BEGIN
    SELECT COUNT(*), COALESCE(SUM(total_amount), 0)
    INTO p_order_count, p_total_spend
    FROM orders
    WHERE customer_id = p_customer_id;
END //

DELIMITER ;

-- ============================================================
-- Sample data
-- ============================================================

INSERT INTO customers (first_name, last_name, email) VALUES
    ('Alice',   'Johnson',  'alice@example.com'),
    ('Bob',     'Smith',    'bob@example.com'),
    ('Carol',   'Williams', 'carol@example.com'),
    ('David',   'Brown',    'david@example.com'),
    ('Eve',     'Davis',    NULL);

INSERT INTO products (sku, name, description, category, unit_price) VALUES
    ('WDG-001', 'Widget A',       'Standard widget',         'Widgets',     9.99),
    ('WDG-002', 'Widget B',       'Premium widget',          'Widgets',    19.99),
    ('GDG-001', 'Gadget X',       'Entry-level gadget',      'Gadgets',    49.99),
    ('GDG-002', 'Gadget Y Pro',   'Professional gadget',     'Gadgets',    99.99),
    ('ACC-001', 'Accessory Pack', 'Assorted accessories',    'Accessories', 14.99);

INSERT INTO stock_levels (product_id, warehouse, quantity) VALUES
    (1, 'Warehouse A', 150),
    (2, 'Warehouse A', 75),
    (3, 'Warehouse A', 30),
    (4, 'Warehouse B', 5),
    (5, 'Warehouse B', 200);

INSERT INTO suppliers (name, contact_email, phone, country) VALUES
    ('Acme Corp',     'sales@acme.example.com',     '555-0100', 'US'),
    ('Global Parts',  'info@globalparts.example',    '555-0200', 'UK'),
    ('QuickSupply',   'orders@quicksupply.example',  '555-0300', 'DE');

INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
    (1, DATE_SUB(CURRENT_DATE, INTERVAL 10 DAY), 29.97, 'completed'),
    (1, DATE_SUB(CURRENT_DATE, INTERVAL 3 DAY),  99.99, 'completed'),
    (2, DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY),  49.99, 'shipped'),
    (3, DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),  64.96, 'pending'),
    (4, CURRENT_DATE,                              9.99, 'pending');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 3,  9.99),
    (2, 4, 1, 99.99),
    (3, 3, 1, 49.99),
    (4, 2, 2, 19.99),
    (4, 5, 1, 14.99),
    (5, 1, 1,  9.99);

INSERT INTO customer_segments (segment_name, min_spend, max_spend, description) VALUES
    ('Bronze',   0,       50,    'New or low-activity customers'),
    ('Silver',   50.01,   200,   'Regular customers'),
    ('Gold',     200.01,  1000,  'High-value customers'),
    ('Platinum', 1000.01, NULL,  'Top-tier customers');
