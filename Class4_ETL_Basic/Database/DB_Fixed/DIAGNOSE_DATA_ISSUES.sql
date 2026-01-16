-- =============================================
-- Script chẩn đoán vấn đề dữ liệu - Tổng hợp
-- Chẩn đoán tại sao số dòng xử lý = 0 và kiểm tra trạng thái dữ liệu
-- =============================================

USE DW_MediaAnalytics
GO

PRINT '========================================'
PRINT 'KIỂM TRA TRẠNG THÁI DỮ LIỆU'
PRINT '========================================'
PRINT ''

-- 1. Kiểm tra dữ liệu trong Staging_RawData
PRINT '1. Kiểm tra Staging_RawData:'
SELECT 
    COUNT(*) AS TotalRows,
    MIN(EventDate) AS MinDate,
    MAX(EventDate) AS MaxDate,
    COUNT(DISTINCT EventDate) AS DistinctDates
FROM Staging_RawData
GO

-- 2. Kiểm tra dữ liệu mẫu trong Staging_RawData
PRINT ''
PRINT '2. Dữ liệu mẫu từ Staging_RawData (5 dòng đầu):'
SELECT TOP 5
    EventDate,
    Contract,
    Mac,
    AppName,
    AppCategory,
    TotalDuration
FROM Staging_RawData
GO

-- 3. Kiểm tra các bảng Dimension có dữ liệu không
PRINT ''
PRINT '3. Kiểm tra các bảng Dimension:'
SELECT 
    (SELECT COUNT(*) FROM DimDate) AS DimDate_Count,
    (SELECT COUNT(*) FROM DimApp) AS DimApp_Count,
    (SELECT COUNT(*) FROM DimContentType) AS DimContentType_Count,
    (SELECT COUNT(*) FROM DimContract) AS DimContract_Count,
    (SELECT COUNT(*) FROM DimDevice) AS DimDevice_Count
GO

-- 4. Kiểm tra dữ liệu cho ngày cụ thể (ví dụ: 2026-01-16)
PRINT ''
PRINT '4. Kiểm tra dữ liệu cho ngày 2026-01-16:'
SELECT 
    COUNT(*) AS RowsFor2026_01_16,
    COUNT(DISTINCT Contract) AS DistinctContracts,
    COUNT(DISTINCT Mac) AS DistinctMacAddresses
FROM Staging_RawData
WHERE EventDate = '2026-01-16'
GO

-- 5. Kiểm tra xem dữ liệu trong Staging có khớp với DimApp không
PRINT ''
PRINT '5. Kiểm tra AppName trong Staging có khớp với DimApp:'
SELECT 
    COUNT(DISTINCT s.AppName) AS DistinctAppNames_InStaging,
    COUNT(DISTINCT a.AppName) AS DistinctAppNames_InDimApp,
    COUNT(DISTINCT CASE WHEN a.AppName IS NOT NULL THEN s.AppName END) AS MatchedAppNames
FROM Staging_RawData s
LEFT JOIN DimApp a ON s.AppName = a.AppName
GO

-- 6. Kiểm tra xem dữ liệu trong Staging có khớp với DimContentType không
PRINT ''
PRINT '6. Kiểm tra AppCategory trong Staging có khớp với DimContentType:'
SELECT 
    COUNT(DISTINCT s.AppCategory) AS DistinctAppCategories_InStaging,
    COUNT(DISTINCT ct.TypeName) AS DistinctTypeNames_InDimContentType,
    COUNT(DISTINCT CASE WHEN ct.TypeName IS NOT NULL THEN s.AppCategory END) AS MatchedCategories
FROM Staging_RawData s
LEFT JOIN DimContentType ct ON s.AppCategory = ct.TypeName
GO

-- 7. Kiểm tra xem EventDate có khớp với DimDate không
PRINT ''
PRINT '7. Kiểm tra EventDate có khớp với DimDate:'
SELECT 
    COUNT(DISTINCT s.EventDate) AS DistinctEventDates_InStaging,
    COUNT(DISTINCT d.DateValue) AS DistinctDateValues_InDimDate,
    COUNT(DISTINCT CASE WHEN d.DateKey IS NOT NULL THEN s.EventDate END) AS MatchedDates
FROM Staging_RawData s
LEFT JOIN DimDate d ON CAST(FORMAT(s.EventDate, 'yyyyMMdd') AS INT) = d.DateKey
GO

-- 8. Kiểm tra điều kiện JOIN đầy đủ cho FactViewingSession
PRINT ''
PRINT '8. Kiểm tra điều kiện JOIN đầy đủ (có thể JOIN được bao nhiêu dòng):'
SELECT 
    COUNT(*) AS RowsThatCanJoin
FROM Staging_RawData s
INNER JOIN DimDate d ON CAST(FORMAT(s.EventDate, 'yyyyMMdd') AS INT) = d.DateKey
INNER JOIN DimContract c ON s.Contract = c.ContractID
INNER JOIN DimDevice dev ON s.Mac = dev.MacAddress
INNER JOIN DimApp a ON s.AppName = a.AppName
INNER JOIN DimContentType ct ON a.AppCategory = ct.TypeName
GO

-- 9. Kiểm tra xem có dữ liệu nào không thể JOIN được không
PRINT ''
PRINT '9. Kiểm tra dữ liệu KHÔNG thể JOIN (vấn đề):'
SELECT 
    COUNT(*) AS RowsCannotJoin,
    COUNT(DISTINCT CASE WHEN d.DateKey IS NULL THEN s.EventDate END) AS MissingDates,
    COUNT(DISTINCT CASE WHEN c.ContractKey IS NULL THEN s.Contract END) AS MissingContracts,
    COUNT(DISTINCT CASE WHEN dev.DeviceKey IS NULL THEN s.Mac END) AS MissingDevices,
    COUNT(DISTINCT CASE WHEN a.AppKey IS NULL THEN s.AppName END) AS MissingApps,
    COUNT(DISTINCT CASE WHEN ct.ContentTypeKey IS NULL THEN s.AppCategory END) AS MissingContentTypes
FROM Staging_RawData s
LEFT JOIN DimDate d ON CAST(FORMAT(s.EventDate, 'yyyyMMdd') AS INT) = d.DateKey
LEFT JOIN DimContract c ON s.Contract = c.ContractID
LEFT JOIN DimDevice dev ON s.Mac = dev.MacAddress
LEFT JOIN DimApp a ON s.AppName = a.AppName
LEFT JOIN DimContentType ct ON a.AppCategory = ct.TypeName
WHERE d.DateKey IS NULL 
   OR c.ContractKey IS NULL 
   OR dev.DeviceKey IS NULL 
   OR a.AppKey IS NULL 
   OR ct.ContentTypeKey IS NULL
GO

-- 10. Kiểm tra các giá trị AppName và AppCategory trong Staging
PRINT ''
PRINT '10. Các giá trị AppName và AppCategory trong Staging:'
SELECT TOP 10
    AppName,
    AppCategory,
    COUNT(*) AS Count
FROM Staging_RawData
GROUP BY AppName, AppCategory
ORDER BY Count DESC
GO

-- 11. Kiểm tra dữ liệu đã được load vào FactViewingSession
PRINT ''
PRINT '11. Kiểm tra FactViewingSession:'
SELECT 
    COUNT(*) AS TotalSessions,
    MIN(DateKey) AS MinDateKey,
    MAX(DateKey) AS MaxDateKey
FROM FactViewingSession
GO

-- 12. Kiểm tra dữ liệu đã được load vào FactContractSummary cho ngày 2026-01-16
PRINT ''
PRINT '12. Kiểm tra FactContractSummary cho ngày 2026-01-16:'
SELECT 
    fcs.*,
    d.DateValue
FROM FactContractSummary fcs
INNER JOIN DimDate d ON fcs.DateKey = d.DateKey
WHERE d.DateValue = '2026-01-16'
GO

-- 13. Kiểm tra xem dữ liệu trong Staging có khớp với điều kiện không
PRINT ''
PRINT '13. Kiểm tra điều kiện lọc:'
SELECT 
    COUNT(*) AS RowsWithContract,
    COUNT(*) AS RowsWithMac,
    COUNT(*) AS RowsWithAppName
FROM Staging_RawData
WHERE EventDate = '2026-01-16'
  AND Contract IS NOT NULL
  AND Mac IS NOT NULL
  AND AppName IS NOT NULL
GO

-- 14. Kiểm tra xem dữ liệu đã tồn tại trong các bảng dimension chưa
PRINT ''
PRINT '14. Kiểm tra dữ liệu mới cần insert:'
SELECT 
    (SELECT COUNT(DISTINCT Contract) 
     FROM Staging_RawData 
     WHERE EventDate = '2026-01-16' 
       AND Contract IS NOT NULL 
       AND Contract NOT IN (SELECT ContractID FROM DimContract)) AS NewContracts,
    (SELECT COUNT(DISTINCT s.Mac) 
     FROM Staging_RawData s
     WHERE s.EventDate = '2026-01-16' 
       AND s.Mac IS NOT NULL 
       AND s.Mac NOT IN (SELECT MacAddress FROM DimDevice)) AS NewDevices
GO

PRINT ''
PRINT '========================================'
PRINT 'KẾT LUẬN'
PRINT '========================================'
PRINT ''
PRINT 'Nếu TotalRows = 0: Không có dữ liệu trong Staging_RawData'
PRINT 'Nếu RowsFor2026_01_16 = 0: Không có dữ liệu cho ngày 2026-01-16'
PRINT 'Nếu MissingDates > 0: Cần populate DimDate cho các ngày này'
PRINT 'Nếu MissingApps > 0: Cần populate DimApp cho các AppName này'
PRINT 'Nếu MissingContentTypes > 0: Cần populate DimContentType cho các AppCategory này'
PRINT 'Nếu MissingDevices > 0: Cần chạy lại bước 2 (Load DimDevice)'
PRINT 'Nếu NewContracts = 0 và NewDevices = 0: Dữ liệu đã được xử lý trước đó'
PRINT '========================================'
GO
