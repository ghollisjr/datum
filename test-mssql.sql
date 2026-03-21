-- test-mssql.sql
-- Creates a test database with tables, views, functions & procedures
-- for exercising datum's SQL Server introspection features.
--
-- Usage (via sqlcmd):
--   sqlcmd -S localhost -U sa -P 'DatumTest1!' -C -i test-mssql.sql

-- ============================================================
-- Database
-- ============================================================
IF DB_ID('datum_test') IS NOT NULL
BEGIN
    ALTER DATABASE datum_test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE datum_test;
END
GO

CREATE DATABASE datum_test;
GO

USE datum_test;
GO

-- ============================================================
-- Tables
-- ============================================================

CREATE TABLE dbo.customers (
    customer_id   INT IDENTITY(1,1) PRIMARY KEY,
    first_name    NVARCHAR(100) NOT NULL,
    last_name     NVARCHAR(100) NOT NULL,
    email         NVARCHAR(255) NULL,
    created_at    DATETIME2 DEFAULT GETDATE(),
    is_active     BIT DEFAULT 1,
    CONSTRAINT uq_customers_email UNIQUE (email)
);
GO

CREATE TABLE dbo.orders (
    order_id      INT IDENTITY(1,1) PRIMARY KEY,
    customer_id   INT NOT NULL,
    order_date    DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    total_amount  DECIMAL(12,2) NOT NULL DEFAULT 0,
    status        NVARCHAR(20) DEFAULT 'pending',
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
);
GO

CREATE TABLE dbo.order_items (
    item_id       INT IDENTITY(1,1) PRIMARY KEY,
    order_id      INT NOT NULL,
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id) REFERENCES dbo.orders(order_id)
);
GO

CREATE TABLE dbo.products (
    product_id    INT IDENTITY(1,1) PRIMARY KEY,
    sku           NVARCHAR(50) NOT NULL,
    name          NVARCHAR(200) NOT NULL,
    description   NVARCHAR(MAX) NULL,
    category      NVARCHAR(100) NULL,
    unit_price    DECIMAL(10,2) NOT NULL,
    created_at    DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT uq_products_sku UNIQUE (sku)
);
GO

CREATE TABLE dbo.stock_levels (
    stock_id      INT IDENTITY(1,1) PRIMARY KEY,
    product_id    INT NOT NULL,
    warehouse     NVARCHAR(100) NOT NULL,
    quantity      INT NOT NULL DEFAULT 0,
    last_updated  DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_stock_product
        FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
);
GO

CREATE TABLE dbo.suppliers (
    supplier_id   INT IDENTITY(1,1) PRIMARY KEY,
    name          NVARCHAR(200) NOT NULL,
    contact_email NVARCHAR(255) NULL,
    phone         NVARCHAR(50) NULL,
    country       NVARCHAR(100) NULL
);
GO

CREATE TABLE dbo.daily_sales (
    report_date   DATE PRIMARY KEY,
    total_orders  INT NOT NULL DEFAULT 0,
    total_revenue DECIMAL(14,2) NOT NULL DEFAULT 0,
    avg_order     DECIMAL(10,2) NULL
);
GO

CREATE TABLE dbo.customer_segments (
    segment_id    INT IDENTITY(1,1) PRIMARY KEY,
    segment_name  NVARCHAR(100) NOT NULL,
    min_spend     DECIMAL(12,2) NULL,
    max_spend     DECIMAL(12,2) NULL,
    description   NVARCHAR(MAX) NULL
);
GO

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW dbo.customer_order_summary AS
SELECT
    c.customer_id,
    c.first_name + ' ' + c.last_name AS full_name,
    c.email,
    COUNT(o.order_id) AS order_count,
    ISNULL(SUM(o.total_amount), 0) AS lifetime_spend,
    MAX(o.order_date) AS last_order_date
FROM dbo.customers c
LEFT JOIN dbo.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;
GO

CREATE VIEW dbo.low_stock AS
SELECT
    p.product_id,
    p.sku,
    p.name,
    sl.warehouse,
    sl.quantity,
    sl.last_updated
FROM dbo.products p
JOIN dbo.stock_levels sl ON p.product_id = sl.product_id
WHERE sl.quantity < 10;
GO

CREATE VIEW dbo.revenue_by_category AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM dbo.order_items oi
JOIN dbo.orders o ON oi.order_id = o.order_id
JOIN dbo.products p ON oi.product_id = p.product_id
GROUP BY p.category;
GO

-- ============================================================
-- Functions
-- ============================================================

-- Simple scalar function
CREATE FUNCTION dbo.format_currency(@amount DECIMAL(12,2), @symbol NVARCHAR(5))
RETURNS NVARCHAR(50)
AS
BEGIN
    RETURN @symbol + FORMAT(@amount, 'N2');
END;
GO

-- Email validation function
CREATE FUNCTION dbo.is_valid_email(@addr NVARCHAR(255))
RETURNS BIT
AS
BEGIN
    IF @addr LIKE '%_@_%.__%'
        RETURN 1;
    RETURN 0;
END;
GO

-- Customer lifetime value function
CREATE FUNCTION dbo.customer_lifetime_value(@p_customer_id INT)
RETURNS DECIMAL(14,2)
AS
BEGIN
    DECLARE @total DECIMAL(14,2);
    SELECT @total = ISNULL(SUM(total_amount), 0)
    FROM dbo.orders
    WHERE customer_id = @p_customer_id;
    RETURN @total;
END;
GO

-- ============================================================
-- Procedures
-- ============================================================

-- Simple procedure
CREATE PROCEDURE dbo.deactivate_customer
    @p_customer_id INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.customers
    SET is_active = 0
    WHERE customer_id = @p_customer_id;
END;
GO

-- Procedure with multiple parameters
CREATE PROCEDURE dbo.place_order
    @p_customer_id INT,
    @p_product_id INT,
    @p_quantity INT,
    @p_status NVARCHAR(20) = 'pending'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @v_order_id INT;
    DECLARE @v_price DECIMAL(10,2);

    -- Look up product price
    SELECT @v_price = unit_price
    FROM dbo.products
    WHERE product_id = @p_product_id;

    IF @v_price IS NULL
    BEGIN
        RAISERROR('Product not found', 16, 1);
        RETURN;
    END;

    -- Create order
    INSERT INTO dbo.orders (customer_id, order_date, total_amount, status)
    VALUES (@p_customer_id, CAST(GETDATE() AS DATE), @v_price * @p_quantity, @p_status);

    SET @v_order_id = SCOPE_IDENTITY();

    -- Create order item
    INSERT INTO dbo.order_items (order_id, product_id, quantity, unit_price)
    VALUES (@v_order_id, @p_product_id, @p_quantity, @v_price);

    -- Update stock
    UPDATE dbo.stock_levels
    SET quantity = quantity - @p_quantity,
        last_updated = GETDATE()
    WHERE product_id = @p_product_id;
END;
GO

-- Procedure that rebuilds analytics
CREATE PROCEDURE dbo.rebuild_daily_sales
    @p_start_date DATE,
    @p_end_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM dbo.daily_sales
    WHERE report_date BETWEEN @p_start_date AND @p_end_date;

    INSERT INTO dbo.daily_sales (report_date, total_orders, total_revenue, avg_order)
    SELECT
        o.order_date,
        COUNT(*),
        SUM(o.total_amount),
        AVG(o.total_amount)
    FROM dbo.orders o
    WHERE o.order_date BETWEEN @p_start_date AND @p_end_date
    GROUP BY o.order_date;
END;
GO

-- Procedure with output parameters
CREATE PROCEDURE dbo.get_customer_stats
    @p_customer_id INT,
    @p_order_count INT OUTPUT,
    @p_total_spend DECIMAL(14,2) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        @p_order_count = COUNT(*),
        @p_total_spend = ISNULL(SUM(total_amount), 0)
    FROM dbo.orders
    WHERE customer_id = @p_customer_id;
END;
GO

-- ============================================================
-- Sample data
-- ============================================================

SET IDENTITY_INSERT dbo.customers ON;
INSERT INTO dbo.customers (customer_id, first_name, last_name, email) VALUES
    (1, 'Alice',   'Johnson',  'alice@example.com'),
    (2, 'Bob',     'Smith',    'bob@example.com'),
    (3, 'Carol',   'Williams', 'carol@example.com'),
    (4, 'David',   'Brown',    'david@example.com'),
    (5, 'Eve',     'Davis',    NULL);
SET IDENTITY_INSERT dbo.customers OFF;
GO

SET IDENTITY_INSERT dbo.products ON;
INSERT INTO dbo.products (product_id, sku, name, description, category, unit_price) VALUES
    (1, 'WDG-001', 'Widget A',       'Standard widget',       'Widgets',     9.99),
    (2, 'WDG-002', 'Widget B',       'Premium widget',        'Widgets',    19.99),
    (3, 'GDG-001', 'Gadget X',       'Entry-level gadget',    'Gadgets',    49.99),
    (4, 'GDG-002', 'Gadget Y Pro',   'Professional gadget',   'Gadgets',    99.99),
    (5, 'ACC-001', 'Accessory Pack', 'Assorted accessories',  'Accessories', 14.99);
SET IDENTITY_INSERT dbo.products OFF;
GO

INSERT INTO dbo.stock_levels (product_id, warehouse, quantity) VALUES
    (1, 'Warehouse A', 150),
    (2, 'Warehouse A', 75),
    (3, 'Warehouse A', 30),
    (4, 'Warehouse B', 5),
    (5, 'Warehouse B', 200);
GO

INSERT INTO dbo.suppliers (name, contact_email, phone, country) VALUES
    ('Acme Corp',     'sales@acme.example.com',     '555-0100', 'US'),
    ('Global Parts',  'info@globalparts.example',    '555-0200', 'UK'),
    ('QuickSupply',   'orders@quicksupply.example',  '555-0300', 'DE');
GO

INSERT INTO dbo.orders (customer_id, order_date, total_amount, status) VALUES
    (1, DATEADD(DAY, -10, CAST(GETDATE() AS DATE)), 29.97, 'completed'),
    (1, DATEADD(DAY, -3,  CAST(GETDATE() AS DATE)), 99.99, 'completed'),
    (2, DATEADD(DAY, -5,  CAST(GETDATE() AS DATE)), 49.99, 'shipped'),
    (3, DATEADD(DAY, -1,  CAST(GETDATE() AS DATE)), 64.96, 'pending'),
    (4, CAST(GETDATE() AS DATE),                      9.99, 'pending');
GO

INSERT INTO dbo.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 3,  9.99),
    (2, 4, 1, 99.99),
    (3, 3, 1, 49.99),
    (4, 2, 2, 19.99),
    (4, 5, 1, 14.99),
    (5, 1, 1,  9.99);
GO

INSERT INTO dbo.customer_segments (segment_name, min_spend, max_spend, description) VALUES
    ('Bronze',   0,       50,    'New or low-activity customers'),
    ('Silver',   50.01,   200,   'Regular customers'),
    ('Gold',     200.01,  1000,  'High-value customers'),
    ('Platinum', 1000.01, NULL,  'Top-tier customers');
GO
