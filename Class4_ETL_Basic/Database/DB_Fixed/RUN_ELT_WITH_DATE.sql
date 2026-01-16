-- =============================================
-- Script chạy ELT với ProcessDate cụ thể
-- Dữ liệu có từ 2022-04-01 đến 2022-04-30
-- =============================================

USE DW_MediaAnalytics
GO

-- Kiểm tra dữ liệu có trong Staging_RawData
PRINT '=== KIỂM TRA DỮ LIỆU TRƯỚC KHI CHẠY ELT ==='
SELECT 
    COUNT(*) AS TotalRows,
    MIN(EventDate) AS MinDate,
    MAX(EventDate) AS MaxDate,
    COUNT(DISTINCT EventDate) AS DistinctDates
FROM Staging_RawData
GO

PRINT ''
PRINT '=== CHẠY ELT VỚI ProcessDate = NULL (Xử lý TẤT CẢ dữ liệu) ==='
EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = NULL
GO

-- Hoặc chạy cho từng ngày cụ thể:
-- EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = '2022-04-01'
-- EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = '2022-04-30'

PRINT ''
PRINT '=== KIỂM TRA KẾT QUẢ SAU KHI CHẠY ELT ==='
SELECT 
    (SELECT COUNT(*) FROM DimContract) AS TotalContracts,
    (SELECT COUNT(*) FROM DimDevice) AS TotalDevices,
    (SELECT COUNT(*) FROM FactViewingSession) AS TotalSessions,
    (SELECT COUNT(*) FROM FactContractSummary) AS TotalSummaries
GO
