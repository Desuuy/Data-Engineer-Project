-- =============================================
-- Script cấp quyền - Tổng hợp
-- Cấp quyền cho user hiện tại và grafana_user
-- CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)
-- =============================================

USE master;
GO

PRINT '========================================'
PRINT 'CẤP QUYỀN CHO USER HIỆN TẠI'
PRINT '========================================'
PRINT ''

-- Cấp quyền cho user hiện tại (nếu chạy với quyền admin)
DECLARE @CurrentUser NVARCHAR(128) = SYSTEM_USER;
PRINT 'Đang cấp quyền cho: ' + @CurrentUser;
PRINT ''

-- Thêm vào role dbcreator (có quyền tạo database)
BEGIN TRY
    ALTER SERVER ROLE dbcreator ADD MEMBER @CurrentUser;
    PRINT '✓ Đã thêm ' + @CurrentUser + ' vào role dbcreator';
END TRY
BEGIN CATCH
    PRINT '⚠ Lỗi: ' + ERROR_MESSAGE();
    PRINT '  Có thể user đã có quyền hoặc không tồn tại.';
END CATCH
GO

-- Kiểm tra quyền
PRINT ''
PRINT 'Kiểm tra lại quyền:'
SELECT 
    SYSTEM_USER AS CurrentUser,
    CASE 
        WHEN IS_SRVROLEMEMBER('dbcreator') = 1 THEN '✓ Có quyền dbcreator'
        ELSE '✗ Không có quyền dbcreator'
    END AS Permission;
GO

-- =============================================
-- CẤP QUYỀN CREATE TABLE CHO USER HIỆN TẠI
-- =============================================
PRINT ''
PRINT '========================================'
PRINT 'CẤP QUYỀN CREATE TABLE'
PRINT '========================================'
PRINT ''

USE DM_MediaAnalytics;
GO

-- Cấp quyền db_ddladmin (có quyền CREATE TABLE, ALTER TABLE, DROP TABLE)
BEGIN TRY
    ALTER ROLE db_ddladmin ADD MEMBER @CurrentUser;
    PRINT '✓ Đã thêm ' + @CurrentUser + ' vào role db_ddladmin';
END TRY
BEGIN CATCH
    PRINT '⚠ Lỗi: ' + ERROR_MESSAGE();
    PRINT '  Có thể user đã có quyền hoặc không tồn tại trong database này.';
END CATCH
GO

-- Kiểm tra quyền
PRINT ''
PRINT 'Kiểm tra quyền:'
SELECT 
    SYSTEM_USER AS CurrentUser,
    CASE 
        WHEN IS_MEMBER('db_ddladmin') = 1 THEN '✓ Có quyền db_ddladmin'
        WHEN IS_MEMBER('db_owner') = 1 THEN '✓ Có quyền db_owner'
        WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN '✓ Có quyền sysadmin'
        ELSE '✗ Không có quyền tạo bảng'
    END AS Permission;
GO

-- =============================================
-- CẤP QUYỀN ĐẦY ĐỦ CHO GRAFANA_USER
-- =============================================
PRINT ''
PRINT '========================================'
PRINT 'CẤP QUYỀN ĐẦY ĐỦ CHO GRAFANA_USER'
PRINT '========================================'
PRINT ''

USE master;
GO

-- Tạo login nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'grafana_user')
BEGIN
    CREATE LOGIN grafana_user WITH PASSWORD = 'Grafana@123';
    PRINT '✓ Đã tạo login: grafana_user';
END
ELSE
BEGIN
    PRINT '✓ Login grafana_user đã tồn tại';
END
GO

-- =============================================
-- CẤP QUYỀN CHO DW_MediaAnalytics
-- =============================================
PRINT ''
PRINT '--- Cấp quyền cho DW_MediaAnalytics ---'
USE DW_MediaAnalytics;
GO

-- Tạo user nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'grafana_user')
BEGIN
    CREATE USER grafana_user FOR LOGIN grafana_user;
    PRINT '✓ Đã tạo user grafana_user trong DW_MediaAnalytics';
END
ELSE
BEGIN
    PRINT '✓ User grafana_user đã tồn tại trong DW_MediaAnalytics';
END
GO

-- Cấp quyền đọc dữ liệu
ALTER ROLE db_datareader ADD MEMBER grafana_user;
PRINT '✓ Đã cấp quyền db_datareader cho DW_MediaAnalytics';

-- Cấp quyền ghi dữ liệu (cần cho INSERT vào Staging_RawData)
ALTER ROLE db_datawriter ADD MEMBER grafana_user;
PRINT '✓ Đã cấp quyền db_datawriter cho DW_MediaAnalytics';

-- Cấp quyền EXECUTE stored procedure
GRANT EXECUTE ON SCHEMA::dbo TO grafana_user;
PRINT '✓ Đã cấp quyền EXECUTE stored procedure cho DW_MediaAnalytics';

-- Cấp quyền xem schema
GRANT VIEW DEFINITION TO grafana_user;
PRINT '✓ Đã cấp quyền VIEW DEFINITION cho DW_MediaAnalytics';
GO

-- =============================================
-- CẤP QUYỀN CHO DM_MediaAnalytics
-- =============================================
PRINT ''
PRINT '--- Cấp quyền cho DM_MediaAnalytics ---'
USE DM_MediaAnalytics;
GO

-- Tạo user nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'grafana_user')
BEGIN
    CREATE USER grafana_user FOR LOGIN grafana_user;
    PRINT '✓ Đã tạo user grafana_user trong DM_MediaAnalytics';
END
ELSE
BEGIN
    PRINT '✓ User grafana_user đã tồn tại trong DM_MediaAnalytics';
END
GO

-- Cấp quyền đọc dữ liệu
ALTER ROLE db_datareader ADD MEMBER grafana_user;
PRINT '✓ Đã cấp quyền db_datareader cho DM_MediaAnalytics';

-- Cấp quyền ghi dữ liệu (cần cho stored procedure INSERT/UPDATE/DELETE)
ALTER ROLE db_datawriter ADD MEMBER grafana_user;
PRINT '✓ Đã cấp quyền db_datawriter cho DM_MediaAnalytics';

-- Cấp quyền EXECUTE stored procedure
GRANT EXECUTE ON SCHEMA::dbo TO grafana_user;
PRINT '✓ Đã cấp quyền EXECUTE stored procedure cho DM_MediaAnalytics';

-- Cấp quyền xem schema
GRANT VIEW DEFINITION TO grafana_user;
PRINT '✓ Đã cấp quyền VIEW DEFINITION cho DM_MediaAnalytics';
GO

-- =============================================
-- KIỂM TRA QUYỀN
-- =============================================
PRINT ''
PRINT '========================================'
PRINT 'KIỂM TRA QUYỀN'
PRINT '========================================'

-- Kiểm tra quyền trong DW_MediaAnalytics
PRINT ''
PRINT 'Quyền trong DW_MediaAnalytics:'
USE DW_MediaAnalytics;
GO

SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'grafana_user';
GO

-- Kiểm tra quyền trong DM_MediaAnalytics
PRINT ''
PRINT 'Quyền trong DM_MediaAnalytics:'
USE DM_MediaAnalytics;
GO

SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'grafana_user';
GO

-- Kiểm tra quyền EXECUTE
PRINT ''
PRINT 'Kiểm tra quyền EXECUTE stored procedures:'
USE DW_MediaAnalytics;
GO

SELECT 
    'DW_MediaAnalytics' AS DatabaseName,
    p.name AS ProcedureName,
    dp.name AS UserName,
    CASE 
        WHEN HAS_PERMS_BY_NAME(p.name, 'OBJECT', 'EXECUTE') = 1 THEN '✓ Có quyền'
        ELSE '✗ Không có quyền'
    END AS ExecutePermission
FROM sys.procedures p
CROSS JOIN sys.database_principals dp
WHERE dp.name = 'grafana_user'
  AND p.name IN ('sp_ELT_LoadToDataWarehouse');
GO

USE DM_MediaAnalytics;
GO

SELECT 
    'DM_MediaAnalytics' AS DatabaseName,
    p.name AS ProcedureName,
    dp.name AS UserName,
    CASE 
        WHEN HAS_PERMS_BY_NAME(p.name, 'OBJECT', 'EXECUTE') = 1 THEN '✓ Có quyền'
        ELSE '✗ Không có quyền'
    END AS ExecutePermission
FROM sys.procedures p
CROSS JOIN sys.database_principals dp
WHERE dp.name = 'grafana_user'
  AND p.name IN ('sp_ELT_LoadToDataMart');
GO

PRINT ''
PRINT '========================================'
PRINT 'HOÀN THÀNH!'
PRINT '========================================'
PRINT ''
PRINT 'User grafana_user đã có đầy đủ quyền:'
PRINT '  ✓ db_datareader (đọc dữ liệu)'
PRINT '  ✓ db_datawriter (ghi dữ liệu)'
PRINT '  ✓ EXECUTE (chạy stored procedure)'
PRINT '  ✓ VIEW DEFINITION (xem schema)'
PRINT ''
PRINT 'Bây giờ có thể chạy lại: python run_full_etl.py'
PRINT '========================================'
GO
