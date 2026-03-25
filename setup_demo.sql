-- Step 1: Ensure Query Store is enabled and configured
ALTER DATABASE CURRENT SET QUERY_STORE = ON;
ALTER DATABASE CURRENT SET QUERY_STORE (
    OPERATION_MODE = READ_WRITE,
    DATA_FLUSH_INTERVAL_SECONDS = 60,
    INTERVAL_LENGTH_MINUTES = 1,
    MAX_STORAGE_SIZE_MB = 100,
    QUERY_CAPTURE_MODE = ALL
);
GO

-- Step 2: Create test table
DROP TABLE IF EXISTS dbo.OrderHistory;
GO

CREATE TABLE dbo.OrderHistory (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME NOT NULL,
    OrderAmount DECIMAL(10,2) NOT NULL,
    ProductCategory NVARCHAR(50) NOT NULL,
    Region NVARCHAR(50) NOT NULL
);
GO

-- Step 3: Insert test data (creating skewed distribution)
-- Small dataset for CustomerID 1 (will benefit from index seek)
INSERT INTO dbo.OrderHistory (CustomerID, OrderDate, OrderAmount, ProductCategory, Region)
SELECT 
    1,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 365), GETDATE()),
    ABS(CHECKSUM(NEWID()) % 1000) + 10.00,
    CASE ABS(CHECKSUM(NEWID()) % 3) 
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        ELSE 'Books'
    END,
    CASE ABS(CHECKSUM(NEWID()) % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END
FROM sys.all_columns c1
CROSS JOIN (SELECT TOP 10 1 AS n FROM sys.all_columns) c2;  -- ~100 rows for CustomerID 1

-- Large dataset for other customers (will benefit from scan)
INSERT INTO dbo.OrderHistory (CustomerID, OrderDate, OrderAmount, ProductCategory, Region)
SELECT 
    ABS(CHECKSUM(NEWID()) % 1000) + 2,  -- CustomerID 2-1001
    DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 730), GETDATE()),
    ABS(CHECKSUM(NEWID()) % 5000) + 10.00,
    CASE ABS(CHECKSUM(NEWID()) % 3) 
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        ELSE 'Books'
    END,
    CASE ABS(CHECKSUM(NEWID()) % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END
FROM sys.all_columns c1
CROSS JOIN sys.all_columns c2;  -- Many thousands of rows
GO

-- Step 4: Create index on CustomerID
CREATE NONCLUSTERED INDEX IX_CustomerID ON dbo.OrderHistory(CustomerID) 
INCLUDE (OrderDate, OrderAmount);
GO

-- Step 5: Update statistics to ensure accurate cardinality estimates
UPDATE STATISTICS dbo.OrderHistory WITH FULLSCAN;
GO

-- Step 6: Create stored procedure with parameter
DROP PROCEDURE IF EXISTS dbo.GetCustomerOrders;
GO

CREATE PROCEDURE dbo.GetCustomerOrders
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        CustomerID,
        OrderDate,
        OrderAmount,
        ProductCategory,
        Region
    FROM dbo.OrderHistory
    WHERE CustomerID = @CustomerID
    ORDER BY OrderDate DESC;
END
GO

-- Step 7: Execute the procedure multiple times to capture baseline plan
-- This will create an optimized plan for CustomerID = 1 (small result set, uses index seek)
EXEC dbo.GetCustomerOrders @CustomerID = 1;
GO 10

-- Wait a moment for Query Store to capture the data
WAITFOR DELAY '00:00:05';
GO

-- Step 8: Force a plan change by executing with a different parameter
-- This demonstrates parameter sniffing - if we execute with a value that returns many rows,
-- we might see a different plan or suboptimal performance
EXEC dbo.GetCustomerOrders @CustomerID = 500;
GO 5

-- Step 9: Force statistics to become outdated by adding more skewed data
INSERT INTO dbo.OrderHistory (CustomerID, OrderDate, OrderAmount, ProductCategory, Region)
SELECT 
    1,  -- Add lots more rows for CustomerID 1
    DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 365), GETDATE()),
    ABS(CHECKSUM(NEWID()) % 1000) + 10.00,
    'Electronics',
    'North'
FROM sys.all_columns c1
CROSS JOIN sys.all_columns c2;  -- Now CustomerID 1 has many rows too
GO

-- Step 10: Update statistics to change the plan
UPDATE STATISTICS dbo.OrderHistory WITH FULLSCAN;
GO

-- Step 11: Execute again to potentially get a different plan
EXEC dbo.GetCustomerOrders @CustomerID = 1;
GO 10

WAITFOR DELAY '00:00:05';
GO

-- Step 12: Query the Query Store to see plan changes
SELECT 
    q.query_id,
    qt.query_sql_text,
    p.plan_id,
    p.query_plan_hash,
    rs.count_executions,
    rs.avg_duration,
    rs.last_execution_time,
    CAST(p.query_plan AS XML) AS query_plan_xml
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
WHERE qt.query_sql_text LIKE '%OrderHistory%'
    AND qt.query_sql_text NOT LIKE '%sys.query_store%'
ORDER BY q.query_id, p.plan_id, rs.last_execution_time;
GO

-- Step 13: See plan changes for a specific query
SELECT 
    q.query_id,
    COUNT(DISTINCT p.query_plan_hash) AS number_of_different_plans,
    STRING_AGG(CAST(p.plan_id AS NVARCHAR(10)), ', ') AS plan_ids
FROM sys.query_store_query q
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
WHERE qt.query_sql_text LIKE '%FROM dbo.OrderHistory%'
    AND qt.query_sql_text NOT LIKE '%sys.query_store%'
GROUP BY q.query_id
HAVING COUNT(DISTINCT p.query_plan_hash) > 1;  -- Only show queries with multiple plans
GO