-- =============================================
-- Script DROP và tạo lại database DM_MediaAnalytics
-- CHỈ CHẠY NẾU KHÔNG QUAN TRỌNG DỮ LIỆU
-- CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)
-- =============================================

USE master;
GO

PRINT '========================================'
PRINT 'DROP VÀ TẠO LẠI DM_MediaAnalytics'
PRINT '========================================'
PRINT ''
PRINT '⚠ CẢNH BÁO: Tất cả dữ liệu trong DM_MediaAnalytics sẽ bị xóa!'
PRINT ''

-- Kiểm tra database có tồn tại không
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DM_MediaAnalytics')
BEGIN
    PRINT 'Đang drop database DM_MediaAnalytics...';
    
    -- Đóng tất cả connections
    ALTER DATABASE DM_MediaAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    PRINT '✓ Đã đóng tất cả connections';
    
    -- Drop database
    DROP DATABASE DM_MediaAnalytics;
    PRINT '✓ Đã drop database DM_MediaAnalytics';
END
ELSE
BEGIN
    PRINT '⚠ Database DM_MediaAnalytics không tồn tại';
END
GO

PRINT ''
PRINT '========================================'
PRINT 'TẠO LẠI DATABASE'
PRINT '========================================'
PRINT ''

-- Tự động tìm đường dẫn default data path
DECLARE @DataPath NVARCHAR(260);
DECLARE @LogPath NVARCHAR(260);

-- Lấy đường dẫn default data path từ registry
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

-- Lấy đường dẫn log path
EXEC master.dbo.xp_instance_regread 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer', 
    N'DefaultLog', 
    @LogPath OUTPUT;

-- Nếu không tìm thấy, dùng cùng đường dẫn với data
IF @LogPath IS NULL
    SET @LogPath = @DataPath;

-- Nếu vẫn không tìm thấy, dùng đường dẫn từ master log
IF @LogPath IS NULL
BEGIN
    SELECT @LogPath = SUBSTRING(physical_name, 1, CHARINDEX(N'mastlog.ldf', LOWER(physical_name)) - 1)
    FROM master.sys.master_files
    WHERE database_id = 1 AND file_id = 2;
END

-- Nếu vẫn không tìm thấy, lấy từ một database đã tồn tại (ví dụ: DW_MediaAnalytics hoặc master)
IF @DataPath IS NULL
BEGIN
    -- Thử lấy từ DW_MediaAnalytics
    SELECT @DataPath = SUBSTRING(physical_name, 1, LEN(physical_name) - LEN(name) - 4)
    FROM sys.master_files
    WHERE DB_NAME(database_id) = 'DW_MediaAnalytics' AND type = 0;
    
    -- Nếu vẫn không có, lấy từ master
    IF @DataPath IS NULL
    BEGIN
        SELECT @DataPath = SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1)
        FROM master.sys.master_files
        WHERE database_id = 1 AND file_id = 1;
    END
END

IF @LogPath IS NULL
    SET @LogPath = @DataPath;

PRINT 'Đường dẫn Data: ' + ISNULL(@DataPath, 'Không tìm thấy - Sẽ dùng đường dẫn mặc định');
PRINT 'Đường dẫn Log: ' + ISNULL(@LogPath, 'Không tìm thấy - Sẽ dùng đường dẫn mặc định');
PRINT '';

-- Tạo lại database với đường dẫn đúng
IF @DataPath IS NOT NULL AND @LogPath IS NOT NULL
BEGIN
    DECLARE @CreateDBSQL NVARCHAR(MAX);
    SET @CreateDBSQL = N'CREATE DATABASE DM_MediaAnalytics
    ON (
        NAME = ''DM_MediaAnalytics'',
        FILENAME = ''' + @DataPath + 'DM_MediaAnalytics.mdf'',
        SIZE = 100MB,
        MAXSIZE = UNLIMITED,
        FILEGROWTH = 50MB
    )
    LOG ON (
        NAME = ''DM_MediaAnalytics_Log'',
        FILENAME = ''' + @LogPath + 'DM_MediaAnalytics_Log.ldf'',
        SIZE = 10MB,
        MAXSIZE = UNLIMITED,
        FILEGROWTH = 10MB
    );';
    
    PRINT 'Đang tạo database với SQL:';
    PRINT @CreateDBSQL;
    PRINT '';
    
    EXEC sp_executesql @CreateDBSQL;
    PRINT '✓ Đã tạo lại database DM_MediaAnalytics';
END
ELSE
BEGIN
    -- Nếu không tìm thấy đường dẫn, tạo database với đường dẫn mặc định (SQL Server sẽ tự chọn)
    PRINT '⚠ Không tìm thấy đường dẫn cụ thể. Tạo database với đường dẫn mặc định...';
    PRINT '  SQL Server sẽ tự động chọn đường dẫn phù hợp';
    CREATE DATABASE DM_MediaAnalytics;
    PRINT '✓ Đã tạo lại database DM_MediaAnalytics (với đường dẫn mặc định)';
    PRINT '';
    PRINT 'Kiểm tra đường dẫn thực tế:';
    SELECT 
        name AS LogicalFileName,
        physical_name AS PhysicalPath
    FROM sys.master_files
    WHERE DB_NAME(database_id) = 'DM_MediaAnalytics';
END
GO

PRINT ''
PRINT '========================================'
PRINT 'BƯỚC TIẾP THEO'
PRINT '========================================'
PRINT ''
PRINT 'Bây giờ cần chạy các script sau:'
PRINT '  1. 03_Create_DM_Schema.sql - Tạo schema và các bảng'
PRINT '  2. 06_ELT_Load_DM.sql - Tạo stored procedure'
PRINT '  3. GRANT_FULL_PERMISSION_FOR_ETL.sql - Cấp quyền (nếu dùng SQL Server Auth)'
PRINT ''
PRINT 'Sau đó chạy lại: python run_full_etl.py'
PRINT '========================================'
GO
