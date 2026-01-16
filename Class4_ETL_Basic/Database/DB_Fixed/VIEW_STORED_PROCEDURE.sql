-- =============================================
-- Script để xem toàn bộ code của stored procedure
-- =============================================

USE DW_MediaAnalytics
GO

-- Cách 1: Dùng OBJECT_DEFINITION (hiển thị trong Results tab)
SELECT OBJECT_DEFINITION(OBJECT_ID('sp_ELT_LoadToDataWarehouse')) AS StoredProcedureCode
GO

-- Cách 2: Dùng sp_helptext (hiển thị từng dòng trong Messages tab)
EXEC sp_helptext 'sp_ELT_LoadToDataWarehouse'
GO

-- Cách 3: Kiểm tra stored procedure có tồn tại không
IF OBJECT_ID('sp_ELT_LoadToDataWarehouse', 'P') IS NOT NULL
    PRINT '✓ Stored Procedure sp_ELT_LoadToDataWarehouse ĐÃ TỒN TẠI'
ELSE
    PRINT '✗ Stored Procedure sp_ELT_LoadToDataWarehouse CHƯA TỒN TẠI'
GO
