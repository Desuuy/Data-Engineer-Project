-- =============================================
-- Script TRUNCATE các bảng trong Data Mart
-- Sử dụng khi database hết dung lượng
-- TRUNCATE giải phóng space nhanh hơn DELETE
-- =============================================

USE DM_MediaAnalytics
GO

PRINT '=== TRUNCATE CÁC BẢNG TRONG DATA MART ==='
PRINT 'Cảnh báo: Tất cả dữ liệu sẽ bị xóa!'
PRINT ''

-- Kiểm tra số dòng trước khi truncate
PRINT 'Số dòng trước khi truncate:'
SELECT 
    (SELECT COUNT(*) FROM DM_ContractAnalytics) AS DM_ContractAnalytics,
    (SELECT COUNT(*) FROM DM_DailySummary) AS DM_DailySummary,
    (SELECT COUNT(*) FROM DM_ContentTypeTrend) AS DM_ContentTypeTrend
GO

-- Truncate các bảng
PRINT ''
PRINT 'Đang truncate các bảng...'

-- Disable foreign key constraints nếu có
-- (Trong trường hợp này không có FK, nhưng để an toàn)

TRUNCATE TABLE DM_ContractAnalytics
PRINT '✓ Đã truncate DM_ContractAnalytics'

TRUNCATE TABLE DM_DailySummary
PRINT '✓ Đã truncate DM_DailySummary'

TRUNCATE TABLE DM_ContentTypeTrend
PRINT '✓ Đã truncate DM_ContentTypeTrend'

GO

-- Kiểm tra sau khi truncate
PRINT ''
PRINT 'Số dòng sau khi truncate:'
SELECT 
    (SELECT COUNT(*) FROM DM_ContractAnalytics) AS DM_ContractAnalytics,
    (SELECT COUNT(*) FROM DM_DailySummary) AS DM_DailySummary,
    (SELECT COUNT(*) FROM DM_ContentTypeTrend) AS DM_ContentTypeTrend
GO

PRINT ''
PRINT '=== HOÀN THÀNH ==='
PRINT 'Các bảng đã được truncate. Bây giờ có thể chạy lại ETL pipeline.'
PRINT 'Chạy: python run_full_etl.py'
GO
