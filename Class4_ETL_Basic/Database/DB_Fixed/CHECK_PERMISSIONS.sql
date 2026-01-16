-- =============================================
-- Script kiểm tra quyền - Tổng hợp
-- Kiểm tra quyền của user hiện tại và grafana_user
-- =============================================

PRINT '========================================'
PRINT 'KIỂM TRA QUYỀN USER HIỆN TẠI'
PRINT '========================================'
PRINT ''

-- User hiện tại
PRINT '1. User hiện tại:'
SELECT SYSTEM_USER AS CurrentUser;
PRINT ''

-- Kiểm tra roles
PRINT '2. Roles của user:'
SELECT 
    CASE 
        WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN '✓ Có quyền sysadmin'
        ELSE '✗ Không có quyền sysadmin'
    END AS SysAdmin,
    CASE 
        WHEN IS_SRVROLEMEMBER('dbcreator') = 1 THEN '✓ Có quyền dbcreator'
        ELSE '✗ Không có quyền dbcreator'
    END AS DbCreator,
    CASE 
        WHEN IS_SRVROLEMEMBER('serveradmin') = 1 THEN '✓ Có quyền serveradmin'
        ELSE '✗ Không có quyền serveradmin'
    END AS ServerAdmin;
PRINT ''

-- Chi tiết roles
PRINT '3. Chi tiết roles:'
SELECT 
    p.name AS PrincipalName,
    r.name AS RoleName
FROM sys.server_role_members rm
INNER JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.server_principals p ON rm.member_principal_id = p.principal_id
WHERE p.name = SYSTEM_USER;
PRINT ''

-- Kết luận
PRINT '4. Kết luận:'
IF IS_SRVROLEMEMBER('sysadmin') = 1 OR IS_SRVROLEMEMBER('dbcreator') = 1
BEGIN
    PRINT '✓ User có quyền tạo database!'
    PRINT '  Bạn có thể chạy script tạo database.'
END
ELSE
BEGIN
    PRINT '✗ User KHÔNG có quyền tạo database!'
    PRINT ''
    PRINT 'Giải pháp:'
    PRINT '1. Kết nối với sa account (SQL Server Authentication)'
    PRINT '2. Hoặc chạy SSMS với quyền Administrator'
    PRINT '3. Hoặc cấp quyền dbcreator cho user này'
    PRINT ''
    PRINT 'Để cấp quyền (chạy với quyền sysadmin):'
    PRINT '  ALTER SERVER ROLE dbcreator ADD MEMBER ''' + SYSTEM_USER + ''';'
END
PRINT ''
PRINT '========================================'
GO

-- =============================================
-- KIỂM TRA QUYỀN GRAFANA_USER
-- =============================================
PRINT ''
PRINT '========================================'
PRINT 'KIỂM TRA QUYỀN GRAFANA_USER'
PRINT '========================================'
PRINT ''

USE DM_MediaAnalytics;
GO

-- Kiểm tra quyền EXECUTE trên stored procedure
PRINT '1. Kiểm tra quyền EXECUTE trên stored procedure:'
SELECT 
    'DM_MediaAnalytics' AS DatabaseName,
    p.name AS ProcedureName,
    dp.name AS UserName,
    CASE 
        WHEN HAS_PERMS_BY_NAME(p.name, 'OBJECT', 'EXECUTE') = 1 THEN '✓ Có quyền EXECUTE'
        ELSE '✗ Không có quyền EXECUTE'
    END AS ExecutePermission
FROM sys.procedures p
CROSS JOIN sys.database_principals dp
WHERE dp.name = 'grafana_user'
  AND p.name = 'sp_ELT_LoadToDataMart';
GO

-- Kiểm tra quyền EXECUTE trên schema
PRINT ''
PRINT '2. Kiểm tra quyền EXECUTE trên schema:'
SELECT 
    'DM_MediaAnalytics' AS DatabaseName,
    'dbo' AS SchemaName,
    dp.name AS UserName,
    CASE 
        WHEN HAS_PERMS_BY_NAME('dbo', 'SCHEMA', 'EXECUTE') = 1 THEN '✓ Có quyền EXECUTE trên schema'
        ELSE '✗ Không có quyền EXECUTE trên schema'
    END AS ExecutePermission
FROM sys.database_principals dp
WHERE dp.name = 'grafana_user';
GO

-- Kiểm tra tất cả quyền của grafana_user
PRINT ''
PRINT '3. Tất cả quyền của grafana_user:'
SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'grafana_user';
GO

-- Kiểm tra quyền trực tiếp (GRANT)
PRINT ''
PRINT '4. Quyền trực tiếp (GRANT):'
SELECT 
    dp.name AS UserName,
    permission_name AS Permission,
    state_desc AS State
FROM sys.database_permissions p
INNER JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
WHERE dp.name = 'grafana_user'
  AND permission_name = 'EXECUTE';
GO

PRINT ''
PRINT '========================================'
PRINT 'KẾT LUẬN'
PRINT '========================================'
PRINT ''
PRINT 'Nếu không có quyền EXECUTE, chạy:'
PRINT '  Database/DB_Fixed/GRANT_PERMISSIONS.sql'
PRINT '========================================'
GO
