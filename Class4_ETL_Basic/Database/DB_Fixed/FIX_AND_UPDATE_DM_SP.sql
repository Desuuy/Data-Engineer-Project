-- =============================================
-- Script kiểm tra và cập nhật stored procedure cho Data Mart
-- Fix lỗi: Cannot find the object "Fact_MediaReport"
-- =============================================

USE DM_MediaAnalytics
GO

-- Kiểm tra stored procedure hiện tại
PRINT '=== KIỂM TRA STORED PROCEDURE HIỆN TẠI ==='
IF OBJECT_ID('sp_ELT_LoadToDataMart', 'P') IS NOT NULL
BEGIN
    PRINT 'Stored Procedure sp_ELT_LoadToDataMart ĐÃ TỒN TẠI'
    PRINT 'Định nghĩa hiện tại:'
    SELECT OBJECT_DEFINITION(OBJECT_ID('sp_ELT_LoadToDataMart'))
END
ELSE
BEGIN
    PRINT 'Stored Procedure sp_ELT_LoadToDataMart CHƯA TỒN TẠI'
END
GO

-- Kiểm tra các bảng trong Data Mart
PRINT ''
PRINT '=== KIỂM TRA CÁC BẢNG TRONG DATA MART ==='
SELECT 
    CASE WHEN OBJECT_ID('DM_ContractAnalytics', 'U') IS NOT NULL THEN '✓' ELSE '✗' END AS DM_ContractAnalytics,
    CASE WHEN OBJECT_ID('DM_DailySummary', 'U') IS NOT NULL THEN '✓' ELSE '✗' END AS DM_DailySummary,
    CASE WHEN OBJECT_ID('DM_ContentTypeTrend', 'U') IS NOT NULL THEN '✓' ELSE '✗' END AS DM_ContentTypeTrend,
    CASE WHEN OBJECT_ID('Fact_MediaReport', 'U') IS NOT NULL THEN '✓' ELSE '✗' END AS Fact_MediaReport
GO

-- Drop stored procedure cũ (nếu có)
IF OBJECT_ID('sp_ELT_LoadToDataMart', 'P') IS NOT NULL
BEGIN
    PRINT ''
    PRINT 'Đang drop stored procedure cũ...'
    DROP PROCEDURE sp_ELT_LoadToDataMart
    PRINT '✓ Đã drop stored procedure cũ'
END
GO

PRINT ''
PRINT '=== HƯỚNG DẪN ==='
PRINT 'Để tạo/cập nhật stored procedure, vui lòng chạy file:'
PRINT '  Database/06_ELT_Load_DM.sql'
PRINT ''
PRINT 'File này chứa stored procedure đầy đủ với:'
PRINT '  - Logic xử lý @ProcessDate = NULL để xử lý TẤT CẢ dữ liệu'
PRINT '  - Chỉ sử dụng các bảng: DM_ContractAnalytics, DM_DailySummary, DM_ContentTypeTrend'
PRINT '  - Không tham chiếu đến Fact_MediaReport (đã được loại bỏ)'
PRINT ''
PRINT 'Sau khi chạy file 06_ELT_Load_DM.sql, bạn có thể test:'
PRINT '  EXEC sp_ELT_LoadToDataMart @ProcessDate = NULL'
GO
