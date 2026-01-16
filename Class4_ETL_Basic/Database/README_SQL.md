# 📚 Hướng dẫn sử dụng các file SQL

## 🎯 File chính (Main Files)

### Data Warehouse (DW)
- **`05_ELT_Load_DW.sql`** ⭐ **FILE CHÍNH**
  - Stored procedure: `sp_ELT_LoadToDataWarehouse`
  - Chứa logic đầy đủ với MERGE và ProcessDate handling
  - **Luôn sử dụng file này để tạo/cập nhật stored procedure**

### Data Mart (DM)
- **`06_ELT_Load_DM.sql`** ⭐ **FILE CHÍNH**
  - Stored procedure: `sp_ELT_LoadToDataMart`
  - Chứa logic đầy đủ với ProcessDate handling
  - **Luôn sử dụng file này để tạo/cập nhật stored procedure**

---

## 📁 Thư mục DB_Fixed - Các file hỗ trợ và fix lỗi

Thư mục `DB_Fixed` chứa các script hỗ trợ, chẩn đoán và fix lỗi đã được tối ưu và gộp lại.

### 🔍 Kiểm tra và chẩn đoán

#### **`CHECK_PERMISSIONS.sql`** ⭐
- **Script kiểm tra quyền tổng hợp**
- Kiểm tra quyền của user hiện tại (sysadmin, dbcreator, serveradmin)
- Kiểm tra quyền của `grafana_user` (EXECUTE, roles, permissions)
- **Chạy khi cần kiểm tra quyền trước khi chạy ETL**

#### **`DIAGNOSE_DATA_ISSUES.sql`** ⭐
- **Script chẩn đoán vấn đề dữ liệu tổng hợp**
- Kiểm tra dữ liệu trong Staging_RawData
- Kiểm tra các bảng Dimension và Fact
- Phát hiện dữ liệu không thể JOIN (MissingDates, MissingApps, MissingContentTypes, etc.)
- **Chạy khi số dòng xử lý = 0 hoặc gặp lỗi dữ liệu**

#### **`VIEW_STORED_PROCEDURE.sql`**
- Script xem định nghĩa stored procedure
- Sử dụng `OBJECT_DEFINITION` và `sp_helptext`
- **Chạy khi cần xem code của stored procedure**

#### **`CHECK_AND_CREATE_SP.sql`**
- Script kiểm tra stored procedure có tồn tại không
- **Chỉ reference đến file chính `05_ELT_Load_DW.sql`**
- Không chứa code duplicate

#### **`FIX_AND_UPDATE_DM_SP.sql`**
- Script kiểm tra stored procedure Data Mart
- **Chỉ reference đến file chính `06_ELT_Load_DM.sql`**
- Không chứa code duplicate

### 🔐 Cấp quyền

#### **`GRANT_PERMISSIONS.sql`** ⭐
- **Script cấp quyền tổng hợp**
- Cấp quyền cho user hiện tại:
  - `dbcreator` role (tạo database)
  - `db_ddladmin` role (CREATE TABLE)
- Cấp đầy đủ quyền cho `grafana_user`:
  - `db_datareader` (đọc dữ liệu)
  - `db_datawriter` (ghi dữ liệu)
  - `EXECUTE` (chạy stored procedure)
  - `VIEW DEFINITION` (xem schema)
- Cấp quyền trên cả `DW_MediaAnalytics` và `DM_MediaAnalytics`
- **CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)**
- **Chạy khi gặp lỗi "Login failed" hoặc "Cannot open database"**

#### **`create_grafana_user.sql`**
- Script tạo user `grafana_user` (chỉ cho Grafana, không đủ quyền cho ETL)
- Chỉ cấp quyền đọc, không có quyền EXECUTE
- **Chỉ dùng cho Grafana dashboard, không dùng cho ETL**

### 🔧 Fix database

#### **`FIX_DATABASE_SIZE.sql`** ⭐
- **Script kiểm tra và tăng database size tổng hợp**
- Kiểm tra database size và free space
- Kiểm tra file properties (size, max_size, growth)
- Kiểm tra dữ liệu trong Data Mart
- Tự động tăng size cho `DW_MediaAnalytics` và `DM_MediaAnalytics`
- **Chạy khi gặp lỗi "Insufficient disk space"**

#### **`FIX_DATABASE_CANNOT_OPEN_V2.sql`** ⭐
- Script fix lỗi "Database cannot be opened" (nâng cao)
- Tự động tìm đường dẫn database files
- Tự động thử rebuild log với đường dẫn đúng
- Xử lý các trạng thái: SUSPECT, RECOVERY_PENDING, OFFLINE
- **Chạy khi gặp lỗi "Database cannot be opened due to inaccessible files"**

#### **`DROP_AND_RECREATE_DM_DATABASE.sql`** ⭐
- Script drop và tạo lại database DM_MediaAnalytics
- Tự động tìm đường dẫn database files
- **CHỈ CHẠY NẾU KHÔNG QUAN TRỌNG DỮ LIỆU**
- **CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)**
- Sau khi chạy, cần chạy lại: `03_Create_DM_Schema.sql` và `06_ELT_Load_DM.sql`

#### **`Truncate_DM.sql`**
- Script truncate các bảng trong Data Mart (giải phóng space)
- Truncate: `DM_ContractAnalytics`, `DM_DailySummary`, `DM_ContentTypeTrend`
- **TRUNCATE giải phóng space nhanh hơn DELETE**
- **Chạy khi database hết dung lượng**

### 🚀 Chạy ETL

#### **`RUN_ELT_WITH_DATE.sql`**
- Script chạy ELT với ProcessDate cụ thể
- Kiểm tra dữ liệu trước và sau khi chạy
- Hỗ trợ `@ProcessDate = NULL` để xử lý TẤT CẢ dữ liệu

### 📋 Setup và hướng dẫn

#### **`SETUP_COMPLETE.sql`**
- Script hướng dẫn setup hoàn chỉnh cho người mới
- Liệt kê thứ tự chạy các file SQL
- **Chạy để xem hướng dẫn setup**

---

## 📝 Thứ tự chạy các file SQL

### Lần đầu setup:
1. `01_Create_Database.sql` - Tạo databases
2. `02_Create_DW_Schema.sql` - Tạo schema Data Warehouse
3. `03_Create_DM_Schema.sql` - Tạo schema Data Mart
4. `04_Populate_DimDate.sql` - Populate DimDate
5. `05_ELT_Load_DW.sql` ⭐ - Tạo stored procedure DW
6. `06_ELT_Load_DM.sql` ⭐ - Tạo stored procedure DM
7. `DB_Fixed/GRANT_PERMISSIONS.sql` ⭐ - Cấp quyền cho ETL (nếu dùng SQL Server Authentication)

### Khi cần cập nhật stored procedure:
1. Chạy file chính: `05_ELT_Load_DW.sql` hoặc `06_ELT_Load_DM.sql`
2. Hoặc chạy file helper: `DB_Fixed/CHECK_AND_CREATE_SP.sql` hoặc `DB_Fixed/FIX_AND_UPDATE_DM_SP.sql` (sẽ hướng dẫn chạy file chính)

### Khi gặp lỗi:

#### 1. **Lỗi "Login failed" hoặc "Cannot open database":**
   - Chạy `DB_Fixed/GRANT_PERMISSIONS.sql` để cấp quyền cho `grafana_user`
   - Hoặc chạy `DB_Fixed/CHECK_PERMISSIONS.sql` để kiểm tra quyền hiện tại

#### 2. **Lỗi "Insufficient disk space":**
   - **Cách 1:** Chạy `DB_Fixed/Truncate_DM.sql` để giải phóng space
   - **Cách 2:** Chạy `DB_Fixed/FIX_DATABASE_SIZE.sql` để tăng database size

#### 3. **Lỗi "Database cannot be opened":**
   - **Cách 1:** Chạy `DB_Fixed/FIX_DATABASE_CANNOT_OPEN_V2.sql` để tự động fix
   - **Cách 2:** Nếu không quan trọng dữ liệu, chạy `DB_Fixed/DROP_AND_RECREATE_DM_DATABASE.sql`
   - Database có thể đang OFFLINE, SUSPECT, hoặc disk space hết

#### 4. **Lỗi "Số dòng đã xử lý: 0":**
   - Chạy `DB_Fixed/DIAGNOSE_DATA_ISSUES.sql` để chẩn đoán
   - Script sẽ phát hiện: MissingDates, MissingApps, MissingContentTypes, MissingDevices, etc.

#### 5. **Xem file `Solution.md`** để tìm giải pháp cho các lỗi khác

---

## ⚠️ Lưu ý quan trọng

1. **Luôn sử dụng file chính:**
   - ✅ `05_ELT_Load_DW.sql` cho Data Warehouse
   - ✅ `06_ELT_Load_DM.sql` cho Data Mart

2. **File helper chỉ để kiểm tra:**
   - Các file helper (`CHECK_AND_CREATE_SP.sql`, `FIX_AND_UPDATE_DM_SP.sql`) chỉ kiểm tra và hướng dẫn
   - Chúng không chứa code duplicate, chỉ reference đến file chính

3. **Phân biệt file cấp quyền:**
   - `create_grafana_user.sql` - Chỉ cho Grafana (đọc dữ liệu)
   - `GRANT_PERMISSIONS.sql` - Cho ETL pipeline (đầy đủ quyền)

4. **Các file đã được gộp và tối ưu:**
   - ✅ `CHECK_PERMISSIONS.sql` - Gộp từ `CHECK_EXECUTE_PERMISSION.sql` và `check_permission.sql`
   - ✅ `GRANT_PERMISSIONS.sql` - Gộp từ `grant_permission.sql`, `GRANT_FULL_PERMISSION_FOR_ETL.sql`, `GRANT_CREATE_TABLE_PERMISSION.sql`
   - ✅ `DIAGNOSE_DATA_ISSUES.sql` - Gộp từ `CHECK_DATA_STATUS.sql` và `DIAGNOSE_WHY_ZERO_ROWS.sql`
   - ✅ `FIX_DATABASE_SIZE.sql` - Gộp từ `FIX_DATABASE_SIZE.sql` (cũ) và `Increse_DBsize.sql`
   - ❌ `FIX_DATABASE_CANNOT_OPEN.sql` - Đã xóa, chỉ giữ lại V2

5. **File trong DB_Fixed đã được tối ưu:**
   - Không còn file duplicate
   - Mỗi file có mục đích rõ ràng
   - Dễ dàng tìm và sử dụng

---

## 📊 Tổng quan cấu trúc thư mục

```
Database/
├── 01_Create_Database.sql          # Tạo databases
├── 02_Create_DW_Schema.sql         # Tạo schema Data Warehouse
├── 03_Create_DM_Schema.sql         # Tạo schema Data Mart
├── 04_Populate_DimDate.sql          # Populate DimDate
├── 05_ELT_Load_DW.sql              # ⭐ Stored procedure DW
├── 06_ELT_Load_DM.sql              # ⭐ Stored procedure DM
├── 07_Create_Scheduler.sql         # Tạo scheduler (tùy chọn)
├── README_SQL.md                   # File này
└── DB_Fixed/                       # Thư mục các file hỗ trợ
    ├── CHECK_PERMISSIONS.sql       # ⭐ Kiểm tra quyền
    ├── GRANT_PERMISSIONS.sql       # ⭐ Cấp quyền
    ├── DIAGNOSE_DATA_ISSUES.sql    # ⭐ Chẩn đoán dữ liệu
    ├── FIX_DATABASE_SIZE.sql       # ⭐ Fix database size
    ├── FIX_DATABASE_CANNOT_OPEN_V2.sql  # ⭐ Fix database cannot open
    ├── DROP_AND_RECREATE_DM_DATABASE.sql  # ⭐ Drop và tạo lại DB
    ├── CHECK_AND_CREATE_SP.sql     # Kiểm tra SP
    ├── FIX_AND_UPDATE_DM_SP.sql    # Fix SP Data Mart
    ├── VIEW_STORED_PROCEDURE.sql   # Xem SP code
    ├── RUN_ELT_WITH_DATE.sql       # Chạy ELT với date
    ├── Truncate_DM.sql             # Truncate Data Mart
    ├── create_grafana_user.sql     # Tạo user Grafana
    └── SETUP_COMPLETE.sql          # Hướng dẫn setup
```

---

## 🔄 Cập nhật

**Ngày cập nhật:** 2026-01-16  
**Phiên bản:** 3.0  
**Thay đổi:** 
- ✅ Đã gộp và tối ưu các file trong thư mục `DB_Fixed`
- ✅ Gộp các file permission: `CHECK_PERMISSIONS.sql`, `GRANT_PERMISSIONS.sql`
- ✅ Gộp các file diagnostic: `DIAGNOSE_DATA_ISSUES.sql`
- ✅ Gộp các file database size: `FIX_DATABASE_SIZE.sql`
- ✅ Xóa file cũ: `FIX_DATABASE_CANNOT_OPEN.sql` (chỉ giữ V2)
- ✅ Cấu trúc thư mục rõ ràng, dễ tìm và sử dụng
- ✅ Mỗi file có mục đích rõ ràng, không còn duplicate
