-- =============================================
-- Script kiểm tra và tăng database size
-- Fix lỗi: insufficient disk space
-- =============================================

USE master
GO

PRINT '========================================'
PRINT 'KIỂM TRA DATABASE SIZE'
PRINT '========================================'
PRINT ''

-- Kiểm tra database size và free space
PRINT '1. Kiểm tra database size:'
SELECT 
    DB_NAME(database_id) AS DatabaseName,
    (SELECT SUM(size) * 8.0 / 1024 FROM sys.master_files WHERE database_id = mf.database_id) AS TotalSize_MB,
    (SELECT SUM(size) * 8.0 / 1024 FROM sys.master_files WHERE database_id = mf.database_id AND type = 0) AS DataSize_MB,
    (SELECT SUM(size) * 8.0 / 1024 FROM sys.master_files WHERE database_id = mf.database_id AND type = 1) AS LogSize_MB
FROM sys.master_files mf
WHERE DB_NAME(database_id) IN ('DW_MediaAnalytics', 'DM_MediaAnalytics')
GROUP BY database_id
GO

-- Kiểm tra file properties
PRINT ''
PRINT '2. Kiểm tra file properties:'
SELECT 
    DB_NAME(database_id) AS DatabaseName,
    name AS FileName,
    type_desc AS FileType,
    size * 8.0 / 1024 AS Size_MB,
    max_size AS MaxSize,
    growth AS Growth,
    is_percent_growth AS IsPercentGrowth,
    physical_name AS PhysicalPath
FROM sys.master_files
WHERE DB_NAME(database_id) IN ('DW_MediaAnalytics', 'DM_MediaAnalytics')
ORDER BY database_id, type;
GO

-- Kiểm tra Data Mart
PRINT ''
PRINT '3. Kiểm tra dữ liệu trong Data Mart:'
USE DM_MediaAnalytics
GO

SELECT 
    'DM_ContractAnalytics' AS TableName,
    COUNT(*) AS RowCount,
    SUM(CAST(DATALENGTH(CAST(AnalyticsKey AS VARCHAR)) + 
             DATALENGTH(DateValue) + 
             DATALENGTH(ContractID) AS BIGINT)) / 1024.0 / 1024.0 AS EstimatedSize_MB
FROM DM_ContractAnalytics
UNION ALL
SELECT 
    'DM_DailySummary',
    COUNT(*),
    SUM(CAST(DATALENGTH(CAST(SummaryKey AS VARCHAR)) + DATALENGTH(DateValue) AS BIGINT)) / 1024.0 / 1024.0
FROM DM_DailySummary
UNION ALL
SELECT 
    'DM_ContentTypeTrend',
    COUNT(*),
    SUM(CAST(DATALENGTH(CAST(TrendKey AS VARCHAR)) + DATALENGTH(DateValue) + DATALENGTH(ContentType) AS BIGINT)) / 1024.0 / 1024.0
FROM DM_ContentTypeTrend
GO

-- =============================================
-- TĂNG DATABASE SIZE
-- =============================================
PRINT ''
PRINT '========================================'
PRINT 'TĂNG DATABASE SIZE'
PRINT '========================================'
PRINT ''

USE master
GO

-- Tăng size cho DM_MediaAnalytics
PRINT 'Đang tăng size cho DM_MediaAnalytics...'
ALTER DATABASE DM_MediaAnalytics
MODIFY FILE (
    NAME = 'DM_MediaAnalytics',
    SIZE = 500MB,  -- Tăng từ 100MB lên 500MB
    MAXSIZE = UNLIMITED,  -- Không giới hạn
    FILEGROWTH = 50MB  -- Tăng 50MB mỗi lần
)
GO

PRINT '✓ Đã tăng size cho DM_MediaAnalytics data file'

-- Tăng size cho log file
ALTER DATABASE DM_MediaAnalytics
MODIFY FILE (
    NAME = 'DM_MediaAnalytics_Log',
    SIZE = 100MB,  -- Tăng từ 10MB lên 100MB
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
GO

PRINT '✓ Đã tăng size cho DM_MediaAnalytics log file'

-- Tương tự cho DW_MediaAnalytics (nếu cần)
PRINT ''
PRINT 'Đang tăng size cho DW_MediaAnalytics...'
ALTER DATABASE DW_MediaAnalytics
MODIFY FILE (
    NAME = 'DW_MediaAnalytics',
    SIZE = 500MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB
)
GO

PRINT '✓ Đã tăng size cho DW_MediaAnalytics data file'

ALTER DATABASE DW_MediaAnalytics
MODIFY FILE (
    NAME = 'DW_MediaAnalytics_Log',
    SIZE = 100MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 10MB
)
GO

PRINT '✓ Đã tăng size cho DW_MediaAnalytics log file'

PRINT ''
PRINT '========================================'
PRINT 'GIẢI PHÁP KHÁC'
PRINT '========================================'
PRINT ''
PRINT 'Nếu database vẫn đầy, có thể:'
PRINT '  1. Truncate các bảng trong DM (xem script Truncate_DM.sql)'
PRINT '  2. Xóa dữ liệu cũ:'
PRINT '     TRUNCATE TABLE DM_ContractAnalytics'
PRINT '     TRUNCATE TABLE DM_DailySummary'
PRINT '     TRUNCATE TABLE DM_ContentTypeTrend'
PRINT ''
PRINT '========================================'
PRINT 'HOÀN THÀNH'
PRINT '========================================'
PRINT ''
PRINT 'Database size đã được tăng. Kiểm tra lại:'
PRINT '  SELECT name, size * 8.0 / 1024 AS Size_MB, max_size, growth'
PRINT '  FROM sys.database_files'
PRINT '========================================'
GO
