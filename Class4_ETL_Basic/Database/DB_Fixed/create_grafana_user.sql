-- =============================================
-- Script tạo user grafana_user cho Grafana
-- CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)
-- =============================================

USE master;
GO

PRINT '========================================'
PRINT 'TAO USER GRAFANA_USER'
PRINT '========================================'
PRINT ''

-- Tạo login nếu chưa có
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'grafana_user')
BEGIN
    CREATE LOGIN grafana_user WITH PASSWORD = 'Grafana@123';
    PRINT '✓ Đã tạo login: grafana_user';
END
ELSE
BEGIN
    PRINT '⚠ Login grafana_user đã tồn tại';
    -- Reset password nếu cần
    ALTER LOGIN grafana_user WITH PASSWORD = 'Grafana@123';
    PRINT '✓ Đã reset password cho grafana_user';
END
GO

-- Tạo user trong DM_MediaAnalytics
USE DM_MediaAnalytics;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'grafana_user')
BEGIN
    CREATE USER grafana_user FOR LOGIN grafana_user;
    PRINT '✓ Đã tạo user grafana_user trong DM_MediaAnalytics';
END
ELSE
BEGIN
    PRINT '⚠ User grafana_user đã tồn tại trong DM_MediaAnalytics';
END
GO

-- Cấp quyền đọc dữ liệu
ALTER ROLE db_datareader ADD MEMBER grafana_user;
PRINT '✓ Đã cấp quyền db_datareader';

-- Cấp quyền xem schema
GRANT VIEW DEFINITION TO grafana_user;
PRINT '✓ Đã cấp quyền VIEW DEFINITION';
GO

-- Kiểm tra quyền
PRINT ''
PRINT 'Kiểm tra quyền của grafana_user:'
SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'grafana_user';
GO

PRINT ''
PRINT '========================================'
PRINT 'HOAN THANH!'
PRINT '========================================'
PRINT ''
PRINT 'Thong tin ket noi cho Grafana:'
PRINT '  Host: localhost:1433'
PRINT '  Database: DM_MediaAnalytics'
PRINT '  Authentication: SQL Server Authentication'
PRINT '  User: grafana_user'
PRINT '  Password: Grafana@123'
PRINT ''
PRINT 'Luu y: Format server la localhost:1433 (dau hai cham, khong phai dau phay)'
PRINT '========================================'
GO
