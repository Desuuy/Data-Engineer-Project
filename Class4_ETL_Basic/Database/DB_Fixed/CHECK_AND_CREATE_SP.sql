-- =============================================
-- Script kiểm tra và tạo stored procedure
-- Script này sẽ kiểm tra và gọi file 05_ELT_Load_DW.sql để tạo stored procedure
-- =============================================

USE DW_MediaAnalytics
GO

-- Kiểm tra database hiện tại
SELECT DB_NAME() AS CurrentDatabase
GO

-- Kiểm tra stored procedure có tồn tại không
PRINT '=== KIỂM TRA STORED PROCEDURE ==='
IF OBJECT_ID('sp_ELT_LoadToDataWarehouse', 'P') IS NOT NULL
BEGIN
    PRINT '✓ Stored Procedure sp_ELT_LoadToDataWarehouse ĐÃ TỒN TẠI'
    PRINT 'Định nghĩa:'
    SELECT OBJECT_DEFINITION(OBJECT_ID('sp_ELT_LoadToDataWarehouse'))
END
ELSE
BEGIN
    PRINT '✗ Stored Procedure sp_ELT_LoadToDataWarehouse CHƯA TỒN TẠI'
END
GO

PRINT ''
PRINT '=== HƯỚNG DẪN ==='
PRINT 'Để tạo/cập nhật stored procedure, vui lòng chạy file:'
PRINT '  Database/05_ELT_Load_DW.sql'
PRINT ''
PRINT 'File này chứa stored procedure đầy đủ với:'
PRINT '  - MERGE statement để xử lý duplicate MacAddress'
PRINT '  - Logic xử lý @ProcessDate = NULL để xử lý TẤT CẢ dữ liệu'
PRINT ''
PRINT 'Sau khi chạy file 05_ELT_Load_DW.sql, bạn có thể test:'
PRINT '  EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = NULL'
GO
