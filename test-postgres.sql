-- test-postgres.sql
-- Creates a test database with schemas, tables, views, functions & procedures
-- for exercising datum's PostgreSQL introspection features.
--
-- Usage:
--   1. Connect as a superuser/admin:  psql -U postgres
--   2. Run:  \i test-postgres.sql
--   3. Connect to the new db:  psql -U postgres -d datum_test
--      (or use datum with the datum_test database)

-- ============================================================
-- Database
-- ============================================================
DROP DATABASE IF EXISTS datum_test;
CREATE DATABASE datum_test;
\connect datum_test

-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA inventory;
CREATE SCHEMA analytics;
CREATE SCHEMA util;

-- ============================================================
-- Tables
-- ============================================================

-- public schema
CREATE TABLE public.customers (
    customer_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) UNIQUE,
    created_at    TIMESTAMP DEFAULT NOW(),
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE public.orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES public.customers(customer_id),
    order_date    DATE NOT NULL DEFAULT CURRENT_DATE,
    total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0,
    status        VARCHAR(20) DEFAULT 'pending'
);

CREATE TABLE public.order_items (
    item_id       SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES public.orders(order_id),
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2) NOT NULL
);

-- inventory schema
CREATE TABLE inventory.products (
    product_id    SERIAL PRIMARY KEY,
    sku           VARCHAR(50) UNIQUE NOT NULL,
    name          VARCHAR(200) NOT NULL,
    description   TEXT,
    category      VARCHAR(100),
    unit_price    NUMERIC(10,2) NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE inventory.stock_levels (
    stock_id      SERIAL PRIMARY KEY,
    product_id    INT NOT NULL REFERENCES inventory.products(product_id),
    warehouse     VARCHAR(100) NOT NULL,
    quantity      INT NOT NULL DEFAULT 0,
    last_updated  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE inventory.suppliers (
    supplier_id   SERIAL PRIMARY KEY,
    name          VARCHAR(200) NOT NULL,
    contact_email VARCHAR(255),
    phone         VARCHAR(50),
    country       VARCHAR(100)
);

-- analytics schema
CREATE TABLE analytics.daily_sales (
    report_date   DATE PRIMARY KEY,
    total_orders  INT NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    avg_order     NUMERIC(10,2)
);

CREATE TABLE analytics.customer_segments (
    segment_id    SERIAL PRIMARY KEY,
    segment_name  VARCHAR(100) NOT NULL,
    min_spend     NUMERIC(12,2),
    max_spend     NUMERIC(12,2),
    description   TEXT
);

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW public.customer_order_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    COUNT(o.order_id) AS order_count,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_spend,
    MAX(o.order_date) AS last_order_date
FROM public.customers c
LEFT JOIN public.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

CREATE VIEW inventory.low_stock AS
SELECT
    p.product_id,
    p.sku,
    p.name,
    sl.warehouse,
    sl.quantity,
    sl.last_updated
FROM inventory.products p
JOIN inventory.stock_levels sl ON p.product_id = sl.product_id
WHERE sl.quantity < 10;

CREATE VIEW analytics.revenue_by_category AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM public.order_items oi
JOIN public.orders o ON oi.order_id = o.order_id
JOIN inventory.products p ON oi.product_id = p.product_id
GROUP BY p.category;

-- ============================================================
-- Functions (various signatures and return types)
-- ============================================================

-- Simple scalar function
CREATE OR REPLACE FUNCTION util.format_currency(amount NUMERIC, symbol VARCHAR DEFAULT '$')
RETURNS VARCHAR
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    RETURN symbol || TO_CHAR(amount, 'FM999,999,990.00');
END;
$$;

-- Function returning a set of rows (TABLE return)
CREATE OR REPLACE FUNCTION inventory.search_products(
    search_term VARCHAR,
    min_price NUMERIC DEFAULT 0,
    max_price NUMERIC DEFAULT 999999
)
RETURNS TABLE(product_id INT, sku VARCHAR, name VARCHAR, unit_price NUMERIC)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT p.product_id, p.sku, p.name, p.unit_price
    FROM inventory.products p
    WHERE (p.name ILIKE '%' || search_term || '%'
           OR p.sku ILIKE '%' || search_term || '%')
      AND p.unit_price BETWEEN min_price AND max_price
    ORDER BY p.name;
END;
$$;

-- Function with nested query and multiple parameters
CREATE OR REPLACE FUNCTION analytics.customer_rank(
    p_customer_id INT,
    p_since DATE DEFAULT '2000-01-01'
)
RETURNS TABLE(rank BIGINT, total_spend NUMERIC, order_count BIGINT)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    RETURN QUERY
    WITH customer_totals AS (
        SELECT
            o.customer_id,
            SUM(o.total_amount) AS spend,
            COUNT(*) AS cnt
        FROM public.orders o
        WHERE o.order_date >= p_since
        GROUP BY o.customer_id
    ),
    ranked AS (
        SELECT
            ct.customer_id,
            ct.spend,
            ct.cnt,
            RANK() OVER (ORDER BY ct.spend DESC) AS rnk
        FROM customer_totals ct
    )
    SELECT r.rnk, r.spend, r.cnt
    FROM ranked r
    WHERE r.customer_id = p_customer_id;
END;
$$;

-- Function with composite type / record output
CREATE OR REPLACE FUNCTION util.parse_name(full_name VARCHAR)
RETURNS TABLE(first_name VARCHAR, last_name VARCHAR)
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    parts TEXT[];
BEGIN
    parts := STRING_TO_ARRAY(full_name, ' ');
    first_name := parts[1];
    last_name  := ARRAY_TO_STRING(parts[2:], ' ');
    RETURN NEXT;
END;
$$;

-- Function with INOUT parameters
CREATE OR REPLACE FUNCTION util.clamp(
    INOUT val NUMERIC,
    low NUMERIC DEFAULT 0,
    high NUMERIC DEFAULT 100
)
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF val < low THEN val := low;
    ELSIF val > high THEN val := high;
    END IF;
END;
$$;

-- Pure SQL function (no plpgsql)
CREATE OR REPLACE FUNCTION util.is_valid_email(addr VARCHAR)
RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE
AS $$
    SELECT addr ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z]{2,}$';
$$;

-- ============================================================
-- Procedures (PostgreSQL 11+)
-- ============================================================

-- Simple procedure
CREATE OR REPLACE PROCEDURE public.deactivate_customer(p_customer_id INT)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.customers
    SET is_active = FALSE
    WHERE customer_id = p_customer_id;
END;
$$;

-- Procedure with multiple parameters and nested logic
CREATE OR REPLACE PROCEDURE public.place_order(
    p_customer_id INT,
    p_product_ids INT[],
    p_quantities INT[],
    p_status VARCHAR DEFAULT 'pending'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id INT;
    v_total    NUMERIC(12,2) := 0;
    v_price    NUMERIC(10,2);
    i          INT;
BEGIN
    -- Create the order header
    INSERT INTO public.orders (customer_id, order_date, total_amount, status)
    VALUES (p_customer_id, CURRENT_DATE, 0, p_status)
    RETURNING order_id INTO v_order_id;

    -- Insert each item
    FOR i IN 1..ARRAY_LENGTH(p_product_ids, 1) LOOP
        SELECT unit_price INTO v_price
        FROM inventory.products
        WHERE product_id = p_product_ids[i];

        IF v_price IS NULL THEN
            RAISE EXCEPTION 'Product % not found', p_product_ids[i];
        END IF;

        INSERT INTO public.order_items (order_id, product_id, quantity, unit_price)
        VALUES (v_order_id, p_product_ids[i], p_quantities[i], v_price);

        v_total := v_total + (v_price * p_quantities[i]);

        -- Update stock
        UPDATE inventory.stock_levels
        SET quantity = quantity - p_quantities[i],
            last_updated = NOW()
        WHERE product_id = p_product_ids[i];
    END LOOP;

    -- Update order total
    UPDATE public.orders
    SET total_amount = v_total
    WHERE order_id = v_order_id;
END;
$$;

-- Procedure that rebuilds analytics
CREATE OR REPLACE PROCEDURE analytics.rebuild_daily_sales(
    p_start_date DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
    p_end_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM analytics.daily_sales
    WHERE report_date BETWEEN p_start_date AND p_end_date;

    INSERT INTO analytics.daily_sales (report_date, total_orders, total_revenue, avg_order)
    SELECT
        o.order_date,
        COUNT(*),
        SUM(o.total_amount),
        AVG(o.total_amount)
    FROM public.orders o
    WHERE o.order_date BETWEEN p_start_date AND p_end_date
    GROUP BY o.order_date;
END;
$$;

-- Procedure with inventory restock logic
CREATE OR REPLACE PROCEDURE inventory.restock(
    p_product_id INT,
    p_warehouse VARCHAR,
    p_quantity INT,
    p_supplier_id INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Upsert stock level
    INSERT INTO inventory.stock_levels (product_id, warehouse, quantity, last_updated)
    VALUES (p_product_id, p_warehouse, p_quantity, NOW())
    ON CONFLICT (stock_id) DO UPDATE
    SET quantity = inventory.stock_levels.quantity + p_quantity,
        last_updated = NOW();
END;
$$;

-- ============================================================
-- Sample data
-- ============================================================

INSERT INTO public.customers (first_name, last_name, email) VALUES
    ('Alice',   'Johnson',  'alice@example.com'),
    ('Bob',     'Smith',    'bob@example.com'),
    ('Carol',   'Williams', 'carol@example.com'),
    ('David',   'Brown',    'david@example.com'),
    ('Eve',     'Davis',    NULL);

INSERT INTO inventory.products (sku, name, description, category, unit_price) VALUES
    ('WDG-001', 'Widget A',       'Standard widget',         'Widgets',    9.99),
    ('WDG-002', 'Widget B',       'Premium widget',          'Widgets',   19.99),
    ('GDG-001', 'Gadget X',       'Entry-level gadget',      'Gadgets',   49.99),
    ('GDG-002', 'Gadget Y Pro',   'Professional gadget',     'Gadgets',   99.99),
    ('ACC-001', 'Accessory Pack',  'Assorted accessories',   'Accessories', 14.99);

INSERT INTO inventory.stock_levels (product_id, warehouse, quantity) VALUES
    (1, 'Warehouse A', 150),
    (2, 'Warehouse A', 75),
    (3, 'Warehouse A', 30),
    (4, 'Warehouse B', 5),
    (5, 'Warehouse B', 200);

INSERT INTO inventory.suppliers (name, contact_email, phone, country) VALUES
    ('Acme Corp',     'sales@acme.example.com',    '555-0100', 'US'),
    ('Global Parts',  'info@globalparts.example',   '555-0200', 'UK'),
    ('QuickSupply',   'orders@quicksupply.example', '555-0300', 'DE');

INSERT INTO public.orders (customer_id, order_date, total_amount, status) VALUES
    (1, CURRENT_DATE - 10, 29.97, 'completed'),
    (1, CURRENT_DATE - 3,  99.99, 'completed'),
    (2, CURRENT_DATE - 5,  49.99, 'shipped'),
    (3, CURRENT_DATE - 1,  64.96, 'pending'),
    (4, CURRENT_DATE,       9.99, 'pending');

INSERT INTO public.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 3,  9.99),
    (2, 4, 1, 99.99),
    (3, 3, 1, 49.99),
    (4, 2, 2, 19.99),
    (4, 5, 1, 14.99),
    (5, 1, 1,  9.99);

INSERT INTO analytics.customer_segments (segment_name, min_spend, max_spend, description) VALUES
    ('Bronze',   0,      50,    'New or low-activity customers'),
    ('Silver',   50.01,  200,   'Regular customers'),
    ('Gold',     200.01, 1000,  'High-value customers'),
    ('Platinum', 1000.01, NULL, 'Top-tier customers');
