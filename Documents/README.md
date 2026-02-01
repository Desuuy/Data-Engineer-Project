# Hệ thống ETL Media Analytics - Hướng dẫn Step-by-Step

> **🎯 Người mới bắt đầu?** 
Xem file **[pipeline_setup/SETUP_GUIDE.md](./pipeline_setup/SETUP_GUIDE.md)** để setup từ đầu KHÔNG LỖI!

## 📋 Tổng quan hệ thống

Hệ thống ETL hoàn chỉnh bao gồm:
1. **ETL Application** - Phần mềm ETL với giao diện interactive
2. **Data Warehouse (DW)** - Database normalized để lưu trữ dữ liệu chi tiết
3. **Data Mart (DM)** - Database denormalized để tối ưu cho reporting
4. **Real-time Dashboards** - Streamlit và Grafana dashboard hiển thị dữ liệu real-time

### Quick Start - Chạy tất cả trong một lệnh

```bash
# Chạy full ETL pipeline tự động (load data + stored procedures + dashboard)
python C:\Users\anhhu\Downloads\Study_DE\Project\Class4_ETL_Basic\Scripts\run_full_etl.py
```

Script này sẽ tự động:
- ✅ Load data vào Staging
- ✅ Chạy stored procedures (DW → DM)
- ✅ Khởi động Streamlit dashboard với **real-time auto-refresh**

---

## Environment Setup

### Cài đặt Python packages

```bash
pip install streamlit pandas pyodbc plotly pyspark findspark
```

### Cài đặt SQL Server và JDBC Driver

1. **Cài đặt SQL Server** (SQL Server 2019 hoặc mới hơn)
2. **Download và cài đặt** [Microsoft JDBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server)
3. **Copy file `sqljdbc_auth.dll`** vào thư mục Spark jars:
   - Windows: `C:\spark\spark-4.0.0-bin-hadoop3\jars\`

### Cấu hình SQL Server

- Đảm bảo SQL Server đang chạy
- Cho phép SQL Server Authentication hoặc sử dụng Windows Authentication
- Tạo thư mục `C:\Data\` để lưu database files (hoặc thay đổi path trong script)

---

## Hướng dẫn sử dụng

### Bước 1: Tạo Database và Schema

Mở **SQL Server Management Studio (SSMS)** và chạy các script theo thứ tự:

```sql
-- 1. Tạo databases
-- Chạy: Database/01_Create_Database.sql

-- 2. Tạo Data Warehouse schema (normalized)
-- Chạy: Database/02_Create_DW_Schema.sql

-- 3. Tạo Data Mart schema (denormalized)
-- Chạy: Database/03_Create_DM_Schema.sql

-- 4. Populate dimension tables
-- Chạy: Database/04_Populate_DimDate.sql

-- 5. Tạo ELT stored procedures (ĐÃ ĐƯỢC FIX - KHÔNG LỖI)
-- Chạy: Database/05_ELT_Load_DW.sql ⭐
--   ✅ Đã fix: MERGE để xử lý duplicate MacAddress
--   ✅ Đã fix: ProcessDate = NULL xử lý TẤT CẢ dữ liệu
-- Chạy: Database/06_ELT_Load_DM.sql ⭐
--   ✅ Đã fix: ProcessDate = NULL xử lý TẤT CẢ dữ liệu
--   ✅ Đã fix: Không tham chiếu đến Fact_MediaReport

-- 6. Tạo SQL Server Agent Jobs (optional)
-- Chạy: Database/07_Create_Scheduler.sql
```

**Lưu ý:** Nếu bạn không có quyền tạo SQL Server Agent Jobs, có thể bỏ qua bước 6 và chạy stored procedures thủ công.

**Xem hướng dẫn chi tiết:** [pipeline_setup/SETUP_GUIDE.md](./pipeline_setup/SETUP_GUIDE.md)

---

### Bước 2: Chạy ETL Application

#### Option 1: Chạy Full ETL Pipeline (Khuyến nghị)

**Script master tự động chạy tất cả các bước:**
- Load data vào Staging
- Chạy stored procedures (DW và DM)
- Khởi động Streamlit dashboard với real-time

```bash
# Nếu đang ở thư mục Project:
python Class4_ETL_Basic/run_full_etl.py

# Hoặc nếu đang ở trong thư mục Class4_ETL_Basic:
python run_full_etl.py
```

**Các bước trong ứng dụng:**
1. Nhập thông tin SQL Server và authentication
2. Nhập đường dẫn thư mục chứa file JSON
3. Chọn chế độ:
   - **Option 1:** Chọn khoảng ngày (nhập YYYYMMDD)
   - **Option 2:** Chọn tất cả file trong thư mục
4. Chọn process date (Enter để process tất cả)
5. Script sẽ tự động:
   - Load vào Staging_RawData
   - Chạy `sp_ELT_LoadToDataWarehouse`
   - Chạy `sp_ELT_LoadToDataMart`
6. Chọn có khởi động Streamlit Dashboard ngay không

**Streamlit Dashboard sẽ tự động refresh real-time** (mặc định 10 giây)

#### Option 2: Chạy từng bước riêng lẻ

##### 2.1. Chạy ETL Application (Xuất CSV)

```bash
python Class4_ETL_Basic/ETL_Application.py
```

**Các bước trong ứng dụng:**
1. Nhập đường dẫn thư mục chứa file JSON
2. Chọn chế độ:
   - **Option 1:** Chọn khoảng ngày (nhập YYYYMMDD)
   - **Option 2:** Chọn tất cả file trong thư mục
3. Chọn output:
   - **Option 1:** CSV File
   - **Option 2:** SQL Server Database

##### 2.2. Chạy ETL Load vào Database

```bash
python Class4_ETL_Basic/ETL_LoadToDatabase.py
```

**Các bước:**
1. Nhập thông tin SQL Server
2. Chọn authentication (Windows hoặc SQL Server)
3. Nhập đường dẫn file JSON
4. Chọn chế độ lấy file (khoảng ngày hoặc tất cả)

**Sau khi load xong, chạy stored procedures:**

```sql
-- Load từ Staging vào Data Warehouse
USE DW_MediaAnalytics
GO
EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = NULL  -- NULL = process tất cả

-- Load từ Data Warehouse xuống Data Mart
USE DM_MediaAnalytics
GO
EXEC sp_ELT_LoadToDataMart @ProcessDate = NULL
```

#### Option 3: Chỉ chạy Dashboard

```bash
cd Class4_ETL_Basic/Dashboard
streamlit run app.py
```

---

### Bước 3: Cấu hình Grafana Dashboard

#### 3.1. Cài đặt Grafana

**Windows:**
1. Download: https://grafana.com/grafana/download?platform=windows
2. Cài đặt và chạy
3. Mở: http://localhost:3000
4. Login: admin/admin

**Docker:**
```bash
docker run -d -p 3000:3000 --name=grafana grafana/grafana
```

#### 3.2. Cài đặt SQL Server Plugin

1. Vào **Configuration** → **Plugins**
2. Tìm "Microsoft SQL Server"
3. Click **Install**

#### 3.3. Tạo Datasource

1. Vào **Configuration** → **Data Sources** → **Add data source**
2. Chọn **Microsoft SQL Server**
3. Điền thông tin:
   - Name: `SQL Server - Media Analytics`
   - Host: `localhost:1433` (hoặc `host.docker.internal:1433` nếu dùng Docker)
   - Database: `DM_MediaAnalytics`
   - Authentication: Windows Authentication hoặc SQL Server Authentication
   - Nếu dùng SQL Server Authentication:
     - Username: `grafana_user`
     - Password: `Grafana@123`
4. Click **Save & Test**

#### 3.4. Import Dashboards

1. Vào **Dashboards** → **Import**
2. Upload các file JSON từ thư mục `Grafana/`:
   - `dashboard_overview.json` - Overview dashboard
   - `dashboard_contract_analytics.json` - Contract analytics
   - `dashboard_content_trends.json` - Content type trends
3. Chọn datasource: **SQL Server - Media Analytics**
4. Click **Import**

**Các tính năng Dashboard:**
- **Overview Dashboard:** KPI cards, daily trends, content type distribution
- **Contract Analytics:** Top contracts, contract details, filters
- **Content Type Trends:** Trends over time, comparisons, statistics

**Cấu hình Auto Refresh (Real-time):**
- Mở dashboard → Click **⚙️** (Settings) → Set Refresh: `10s` hoặc `30s`
- Hoặc click vào dropdown refresh ở góc trên bên phải → Chọn `10s`, `30s`, `1m`, `5m`, etc.
- Dashboard sẽ tự động refresh và hiển thị dữ liệu real-time

**Lưu ý:** Để Grafana hiển thị real-time tốt nhất:
1. Đảm bảo SQL Server đang chạy và có dữ liệu mới
2. Set refresh interval phù hợp (khuyến nghị: 10s-30s cho real-time, 1m-5m cho monitoring thông thường)
3. Các panel sẽ tự động cập nhật khi có dữ liệu mới

**Xem hướng dẫn chi tiết:** [pipeline_setup/HUONG_DAN_MO_GRAFANA.md](./pipeline_setup/HUONG_DAN_MO_GRAFANA.md)

---

## 📊 Kiến trúc Database

### Data Warehouse (Normalized)

**Dimension Tables:**
- `DimDate` - Thông tin ngày tháng
- `DimContract` - Thông tin Contract
- `DimDevice` - Thông tin Device
- `DimApp` - Thông tin Application
- `DimContentType` - Loại nội dung

**Fact Tables:**
- `FactViewingSession` - Chi tiết session (normalized)
- `FactContractSummary` - Tổng hợp theo Contract (một phần denormalized)

**Staging Table:**
- `Staging_RawData` - Table tạm để load data từ Spark

### Data Mart (Denormalized)

**Tables:**
- `DM_ContractAnalytics` - Phân tích Contract (denormalized)
- `DM_DailySummary` - Tóm tắt theo ngày
- `DM_ContentTypeTrend` - Xu hướng Content Type

**Xem chi tiết:** [Database/README_SQL.md](./Database/README_SQL.md)

---

## 🔄 Quy trình ETL/ELT

### ETL Process (Extract → Transform → Load)

1. **Extract:** Đọc file JSON từ thư mục
2. **Transform:** 
   - Select fields từ `_source`
   - Transform AppName thành ContentType
   - Tính toán statistics
3. **Load:** 
   - **Option 1:** Lưu vào CSV
   - **Option 2:** Load vào SQL Server Staging table

### ELT Process (Extract → Load → Transform)

1. **Extract:** Đọc từ Staging_RawData
2. **Load:** Load vào Dimension và Fact tables
3. **Transform:** 
   - Transform và aggregate trong SQL
   - Load vào Data Mart (denormalized)

---

## ⏰ Tự động hóa với SQL Server Agent

Nếu đã tạo SQL Server Agent Jobs (bước 1), hệ thống sẽ tự động chạy:

- **Job 1:** `ETL_LoadToDataWarehouse` - Chạy hàng ngày lúc 2:00 AM
- **Job 2:** `ETL_LoadToDataMart` - Chạy hàng ngày lúc 3:00 AM

**Chạy thủ công nếu cần:**

```sql
-- Chạy Job thủ công
USE msdb
GO
EXEC sp_start_job @job_name = 'ETL_LoadToDataWarehouse'
EXEC sp_start_job @job_name = 'ETL_LoadToDataMart'
```

---

## 📝 Ví dụ sử dụng

### Ví dụ 1: ETL một khoảng ngày và xuất CSV

```bash
python Class4_ETL_Basic/ETL_Application.py
# Chọn: 1 (khoảng ngày)
# Nhập: 20220401 (start), 20220405 (end)
# Chọn: 1 (CSV)
# Nhập: C:\Output\ETL_Results
```

### Ví dụ 2: ETL tất cả file và load vào Database

```bash
python Class4_ETL_Basic/ETL_LoadToDatabase.py
# Nhập: localhost
# Database: DW_MediaAnalytics
# Auth: 1 (Windows) hoặc 2 (SQL Server)
# Path: C:\Users\anhhu\Downloads\Study_DE\Project\Data\log_content
# Mode: 2 (tất cả)
```

Sau đó chạy stored procedures trong SSMS.

### Ví dụ 3: Chạy Full ETL Pipeline và Dashboard

```bash
# Chạy full pipeline (tự động hóa tất cả)
python C:\Users\anhhu\Downloads\Study_DE\Project\Class4_ETL_Basic\Scripts\run_full_etl.py

# Input mẫu:
# SQL Server: localhost
# Data Warehouse: [Enter] (mặc định: DW_MediaAnalytics)
# Data Mart: [Enter] (mặc định: DM_MediaAnalytics)
# Authentication: 1-Windows/2-SQL Server
# Nếu chọn 2:
#   Username: grafana_user
#   Password: Grafana@123
# Encrypt: [Enter] (mặc định: Enable)
# Trust Server Certificate: [Enter] (mặc định: Yes)
# Đường dẫn thư mục: C:\Users\anhhu\Downloads\Study_DE\Project\Data\log_content
# Chế độ: 
#   1: Chọn khoảng ngày
#      20220401
#      20220403
#   2: All
# Process date: [Enter] (NULL = process tất cả)
# Khởi động Dashboard: yes
# Port: [Enter] (mặc định: 8501)
```

**Dashboard Real-time Features:**
- ✅ Auto-refresh tự động (mặc định 10 giây)
- ✅ Hiển thị thời gian cập nhật cuối cùng
- ✅ Countdown timer trước khi refresh
- ✅ Real-time indicator trong sidebar
- ✅ Có thể tắt/bật auto-refresh và điều chỉnh interval

Mở browser và cấu hình database connection trong sidebar. Dashboard sẽ tự động refresh để hiển thị dữ liệu mới nhất.

---

## 🐛 Troubleshooting

### Lỗi: "No module named 'pyspark'"
```bash
pip install pyspark findspark
```

### Lỗi: "JDBC Driver not found"
- Download và cài đặt Microsoft JDBC Driver
- Copy `sqljdbc_auth.dll` vào thư mục Spark jars

### Lỗi: "Cannot connect to SQL Server"
- Kiểm tra SQL Server đang chạy
- Kiểm tra firewall settings
- Kiểm tra authentication method
- Xem: [pipeline_setup/sql_connect_trouble.md](./pipeline_setup/sql_connect_trouble.md)

### Lỗi: "Table does not exist"
- Chạy lại các script SQL tạo schema (bước 1)
- Xem: [pipeline_setup/SETUP_GUIDE.md](./pipeline_setup/SETUP_GUIDE.md)

### Lỗi: "Login failed" hoặc "Cannot open database"
- Chạy `Database/DB_Fixed/GRANT_PERMISSIONS.sql` để cấp quyền
- Xem: [pipeline_setup/sql_fix_permission.md](./pipeline_setup/sql_fix_permission.md)

### Lỗi: "Insufficient disk space"
- Chạy `Database/DB_Fixed/Truncate_DM.sql` để giải phóng space
- Hoặc chạy `Database/DB_Fixed/FIX_DATABASE_SIZE.sql` để tăng database size

### Lỗi: "Số dòng đã xử lý: 0"
- Chạy `Database/DB_Fixed/DIAGNOSE_DATA_ISSUES.sql` để chẩn đoán
- Kiểm tra dữ liệu trong Staging_RawData

**Xem tất cả lỗi và giải pháp:** [pipeline_setup/Solution.md](./pipeline_setup/Solution.md)

---

## 📚 Tài liệu tham khảo

### Hướng dẫn chính
- **Setup Guide:** [pipeline_setup/SETUP_GUIDE.md](./pipeline_setup/SETUP_GUIDE.md) ⭐ - Hướng dẫn setup từ đầu KHÔNG LỖI
- **Quick Start:** [pipeline_setup/quick_start.md](./pipeline_setup/quick_start.md) - Chạy nhanh trong 5 phút
- **Hướng dẫn chạy script:** [pipeline_setup/HUONG_DAN_CHAY_SCRIPT.md](./pipeline_setup/HUONG_DAN_CHAY_SCRIPT.md) - Hướng dẫn chạy `run_full_etl.py`
- **Debug Guide:** [pipeline_setup/DEBUG_GUIDE.md](./pipeline_setup/DEBUG_GUIDE.md) - Hướng dẫn debug chung

### Grafana
- **Hướng dẫn Grafana:** [pipeline_setup/HUONG_DAN_MO_GRAFANA.md](./pipeline_setup/HUONG_DAN_MO_GRAFANA.md) - Cài đặt, cấu hình, sử dụng
- **Setup Grafana:** [pipeline_setup/setup_grafana.md](./pipeline_setup/setup_grafana.md) - Hướng dẫn setup Grafana

### Database & Troubleshooting
- **SQL Files Guide:** [Database/README_SQL.md](./Database/README_SQL.md) - Hướng dẫn sử dụng các file SQL
- **Sửa lỗi permission:** [pipeline_setup/sql_fix_permission.md](./pipeline_setup/sql_fix_permission.md) - Sửa lỗi CREATE DATABASE permission denied
- **SQL Connection Troubleshooting:** [pipeline_setup/sql_connect_trouble.md](./pipeline_setup/sql_connect_trouble.md) - Sửa lỗi kết nối SQL Server
- **Solution:** [pipeline_setup/Solution.md](./pipeline_setup/Solution.md) ⭐ - Tổng hợp TẤT CẢ lỗi và giải pháp

### Files SQL chính (ĐÃ ĐƯỢC FIX):
- **`Database/05_ELT_Load_DW.sql`** ⭐ - Stored procedure DW (đã fix MERGE và ProcessDate)
- **`Database/06_ELT_Load_DM.sql`** ⭐ - Stored procedure DM (đã fix ProcessDate và Fact_MediaReport)

**Tài liệu kỹ thuật:**
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [SQL Server Documentation](https://docs.microsoft.com/en-us/sql/)
- [Grafana Documentation](https://grafana.com/docs/grafana/latest/)

---

## 🔐 Grafana Account (Nếu sử dụng SQL Server Authentication)

```
Username: grafana_user
Password: Grafana@123
Database: DM_MediaAnalytics
Auth: SQL Server Authentication
Role: db_datareader (read-only)
```

**Lưu ý:** Account này chỉ có quyền đọc dữ liệu. Để chạy ETL pipeline, cần account có quyền EXECUTE stored procedures. Xem: [Database/DB_Fixed/GRANT_PERMISSIONS.sql](./Database/DB_Fixed/GRANT_PERMISSIONS.sql)

---

## ✅ Đảm bảo không lỗi

Tất cả các file SQL chính đã được cập nhật và fix:
- ✅ MERGE statement để xử lý duplicate
- ✅ ProcessDate = NULL xử lý TẤT CẢ dữ liệu
- ✅ Không còn reference đến Fact_MediaReport
- ✅ Error handling đầy đủ
- ✅ Comments rõ ràng

**Người mới chỉ cần chạy các file SQL theo thứ tự trong [pipeline_setup/SETUP_GUIDE.md](./pipeline_setup/SETUP_GUIDE.md) là sẽ không gặp lỗi!**

---

## 📁 Cấu trúc thư mục

```
Class4_ETL_Basic/
├── Database/                    # SQL scripts
│   ├── 01_Create_Database.sql
│   ├── 02_Create_DW_Schema.sql
│   ├── 03_Create_DM_Schema.sql
│   ├── 04_Populate_DimDate.sql
│   ├── 05_ELT_Load_DW.sql      # ⭐ Stored procedure DW
│   ├── 06_ELT_Load_DM.sql      # ⭐ Stored procedure DM
│   ├── 07_Create_Scheduler.sql
│   ├── README_SQL.md
│   └── DB_Fixed/               # Scripts hỗ trợ và fix lỗi
├── Dashboard/                   # Streamlit dashboard
│   ├── app.py
│   └── requirements.txt
├── Grafana/                     # Grafana dashboards
│   ├── dashboard_overview.json
│   ├── dashboard_contract_analytics.json
│   ├── dashboard_content_trends.json
│   └── datasource_sqlserver.json
├── pipeline_setup/              # Tài liệu hướng dẫn
│   ├── SETUP_GUIDE.md          # ⭐ Hướng dẫn setup
│   ├── Solution.md             # ⭐ Tổng hợp lỗi và giải pháp
│   └── ...
├── ETL_Application.py           # ETL app xuất CSV
├── ETL_LoadToDatabase.py       # ETL load vào database
├── run_full_etl.py             # ⭐ Script master tự động
└── README.md                   # File này
```

---

**Cập nhật lần cuối:** 2026-01-16  
**Phiên bản:** 2.0
