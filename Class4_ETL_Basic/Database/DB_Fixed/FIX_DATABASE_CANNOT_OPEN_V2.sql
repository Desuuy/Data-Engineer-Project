-- =============================================
-- Script fix lỗi: Database cannot be opened (Version 2)
-- Tự động tìm đường dẫn database files và fix
-- =============================================

USE master;
GO

PRINT '========================================'
PRINT 'KIỂM TRA TRẠNG THÁI VÀ FILE DATABASE'
PRINT '========================================'
PRINT ''

-- Kiểm tra trạng thái database
SELECT 
    name AS DatabaseName,
    state_desc AS State
FROM sys.databases
WHERE name = 'DM_MediaAnalytics';
GO

-- Lấy đường dẫn thực tế của database files
DECLARE @DataPath NVARCHAR(260);
DECLARE @LogPath NVARCHAR(260);
DECLARE @DataFileName NVARCHAR(260);
DECLARE @LogFileName NVARCHAR(260);

-- Lấy đường dẫn default data path
EXEC master.dbo.xp_instance_regread 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer', 
    N'DefaultData', 
    @DataPath OUTPUT;

-- Nếu không tìm thấy, thử lấy từ master database
IF @DataPath IS NULL
BEGIN
    SELECT @DataPath = SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1)
    FROM master.sys.master_files
    WHERE database_id = 1 AND file_id = 1;
END

-- Lấy tên file từ sys.master_files (nếu database đã tồn tại)
SELECT 
    @DataFileName = name,
    @DataPath = SUBSTRING(physical_name, 1, LEN(physical_name) - LEN(name) - 4)
FROM sys.master_files
WHERE DB_NAME(database_id) = 'DM_MediaAnalytics' AND type = 0;

SELECT 
    @LogFileName = name,
    @LogPath = SUBSTRING(physical_name, 1, LEN(physical_name) - LEN(name) - 4)
FROM sys.master_files
WHERE DB_NAME(database_id) = 'DM_MediaAnalytics' AND type = 1;

-- Nếu không tìm thấy, dùng tên mặc định
IF @DataFileName IS NULL
    SET @DataFileName = 'DM_MediaAnalytics';
IF @LogFileName IS NULL
    SET @LogFileName = 'DM_MediaAnalytics_Log';

PRINT 'Đường dẫn Data: ' + ISNULL(@DataPath, 'Không tìm thấy');
PRINT 'Đường dẫn Log: ' + ISNULL(@LogPath, 'Không tìm thấy');
PRINT 'Tên file Data: ' + @DataFileName;
PRINT 'Tên file Log: ' + @LogFileName;
PRINT '';

-- Kiểm tra trạng thái database
DECLARE @DBState NVARCHAR(60);
SELECT @DBState = state_desc 
FROM sys.databases 
WHERE name = 'DM_MediaAnalytics';

PRINT 'Trạng thái database: ' + @DBState;
PRINT '';

-- =============================================
-- GIẢI PHÁP
-- =============================================

IF @DBState = 'SUSPECT' OR @DBState = 'RECOVERY_PENDING'
BEGIN
    PRINT '⚠ Database đang ở trạng thái: ' + @DBState;
    PRINT '';
    PRINT 'CÁCH FIX 1: Thử đặt về EMERGENCY và rebuild log';
    PRINT '';
    
    BEGIN TRY
        -- Bước 1: EMERGENCY mode
        ALTER DATABASE DM_MediaAnalytics SET EMERGENCY;
        PRINT '✓ Đã đặt database về EMERGENCY mode';
        
        -- Bước 2: Rebuild log với đường dẫn đúng
        IF @LogPath IS NOT NULL AND @LogFileName IS NOT NULL
        BEGIN
            DECLARE @RebuildSQL NVARCHAR(MAX);
            SET @RebuildSQL = N'ALTER DATABASE DM_MediaAnalytics REBUILD LOG ON (
                NAME = ''' + @LogFileName + ''',
                FILENAME = ''' + @LogPath + @LogFileName + '.ldf''
            );';
            
            PRINT 'Đang rebuild log với SQL:';
            PRINT @RebuildSQL;
            EXEC sp_executesql @RebuildSQL;
            PRINT '✓ Đã rebuild log file';
        END
        ELSE
        BEGIN
            PRINT '⚠ Không tìm thấy đường dẫn log file. Thử cách khác...';
        END
        
        -- Bước 3: ONLINE
        ALTER DATABASE DM_MediaAnalytics SET ONLINE;
        PRINT '✓ Đã đặt database về ONLINE';
    END TRY
    BEGIN CATCH
        PRINT '✗ Lỗi khi rebuild log: ' + ERROR_MESSAGE();
        PRINT '';
        PRINT 'CÁCH FIX 2: Drop và tạo lại database (nếu không quan trọng dữ liệu)';
        PRINT '  Chạy script: DROP_AND_RECREATE_DM_DATABASE.sql';
    END CATCH
END
ELSE IF @DBState = 'OFFLINE'
BEGIN
    PRINT '⚠ Database đang OFFLINE';
    PRINT '';
    
    BEGIN TRY
        ALTER DATABASE DM_MediaAnalytics SET ONLINE;
        PRINT '✓ Đã đặt database về ONLINE';
    END TRY
    BEGIN CATCH
        PRINT '✗ Không thể đặt database về ONLINE: ' + ERROR_MESSAGE();
        PRINT '  Cần xử lý thủ công hoặc drop và tạo lại';
    END CATCH
END
ELSE
BEGIN
    PRINT '✓ Database đang ở trạng thái: ' + @DBState;
    PRINT '  Nếu vẫn không mở được, thử restart SQL Server service';
END
GO

PRINT ''
PRINT '========================================'
PRINT 'KIỂM TRA LẠI TRẠNG THÁI'
PRINT '========================================'
PRINT ''

SELECT 
    name AS DatabaseName,
    state_desc AS State,
    CASE 
        WHEN state_desc = 'ONLINE' THEN '✓ Database có thể sử dụng'
        ELSE '✗ Database không thể sử dụng - Cần fix'
    END AS Status
FROM sys.databases
WHERE name = 'DM_MediaAnalytics';
GO

PRINT ''
PRINT '========================================'
PRINT 'HOÀN THÀNH'
PRINT '========================================'
PRINT ''
PRINT 'Nếu database vẫn không mở được:'
PRINT '  1. Chạy script: DROP_AND_RECREATE_DM_DATABASE.sql (nếu không quan trọng dữ liệu)'
PRINT '  2. Hoặc kiểm tra SQL Server error log'
PRINT '  3. Hoặc restart SQL Server service'
PRINT '========================================'
GO
