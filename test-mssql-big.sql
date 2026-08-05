-- test-mssql-big.sql
-- Creates a database with 60,000 empty tables for stress-testing
-- background introspection.
--
-- Usage (via sqlcmd):
--   sqlcmd -S localhost -U sa -P 'DatumTest1!' -C -i test-mssql-big.sql

IF DB_ID('datum_big') IS NOT NULL
BEGIN
    ALTER DATABASE datum_big SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE datum_big;
END
GO

CREATE DATABASE datum_big;
GO

USE datum_big;
GO

-- Generate 60,000 empty tables across a handful of schemas.
-- Each table has a single INT column to keep creation fast.

CREATE SCHEMA sales;
GO
CREATE SCHEMA inventory;
GO
CREATE SCHEMA reporting;
GO
CREATE SCHEMA staging;
GO
CREATE SCHEMA archive;
GO

DECLARE @i INT = 1;
DECLARE @sql NVARCHAR(MAX);
DECLARE @schema NVARCHAR(20);
WHILE @i <= 60000
BEGIN
    SET @schema = CASE (@i % 6)
        WHEN 0 THEN 'dbo'
        WHEN 1 THEN 'sales'
        WHEN 2 THEN 'inventory'
        WHEN 3 THEN 'reporting'
        WHEN 4 THEN 'staging'
        WHEN 5 THEN 'archive'
    END;
    SET @sql = 'CREATE TABLE ' + @schema + '.t_'
               + RIGHT('00000' + CAST(@i AS VARCHAR(5)), 5)
               + ' (id INT)';
    EXEC sp_executesql @sql;
    SET @i = @i + 1;
END
GO

PRINT 'Created 60,000 tables in datum_big.';
GO
