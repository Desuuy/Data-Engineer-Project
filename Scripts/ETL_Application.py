"""
ETL Application - Phần mềm ETL hoàn chỉnh
Hỗ trợ:
- Nhập đường dẫn input/output
- Chọn khoảng thời gian hoặc folder
- Xuất CSV hoặc import vào SQL Server
"""

import sys
import glob
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import findspark
findspark.init()


# Database connection
try:
    from pyspark.sql import DataFrameWriter
    import pyodbc
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    print("Warning: pyodbc not available. Database export will be disabled.")


class ETLApplication:
    def __init__(self):
        self.spark = SparkSession.builder.config(
            "spark.driver.memory", "8g").getOrCreate()
        self.file_type = "json"

    def get_date_range_files(self, base_path, start_date_str, end_date_str):
        """
        Lấy danh sách các file JSON trong khoảng thời gian
        Format: YYYYMMDD (ví dụ: 20220401)
        """
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.strptime(end_date_str, "%Y%m%d")

        file_list = []
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            file_path = os.path.join(base_path, f"{date_str}.json")
            if os.path.exists(file_path):
                file_list.append(file_path)
            current_date += timedelta(days=1)

        return file_list

    def get_files_from_folder(self, folder_path):
        """
        Lấy tất cả các file JSON trong thư mục
        """
        if not os.path.exists(folder_path):
            raise ValueError(f"Thư mục không tồn tại: {folder_path}")

        pattern = os.path.join(folder_path, "*.json")
        file_list = glob.glob(pattern)
        return sorted(file_list)

    def read_data_from_paths(self, file_paths):
        """
        Đọc và union tất cả các file trong danh sách
        """
        if not file_paths:
            raise ValueError("Không có file nào để đọc!")

        print(f"Đang đọc {len(file_paths)} file(s)...")

        # Đọc file đầu tiên
        df = self.spark.read.format(self.file_type).load(file_paths[0])

        # Union các file còn lại
        for i, file_path in enumerate(file_paths[1:], 1):
            print(
                f"Đang đọc file {i+1}/{len(file_paths)}: {os.path.basename(file_path)}")
            try:
                df_temp = self.spark.read.format(
                    self.file_type).load(file_path)
                df = df.unionByName(df_temp, allowMissingColumns=True)
            except Exception as e:
                print(f"Lỗi khi đọc file {file_path}: {e}")
                continue

        print(f"Đã đọc xong {len(file_paths)} file(s)")
        print(f"Tổng số dòng: {df.count()}")
        df.show(10)
        return df

    def select_fields(self, df):
        """
        Lấy tất cả các cột từ _source
        """
        df = df.select("_source.*")
        return df

    def calculate_devices(self, df):
        """
        Tính tổng số thiết bị theo Contract
        """
        total_devices = df.select(
            "Contract", "Mac").groupBy("Contract").count()
        total_devices = total_devices.withColumnRenamed(
            'count', 'TotalDevices')
        return total_devices

    def transform_category(self, df):
        """
        Transform AppName thành Type category
        """
        df = df.withColumn("Type",
                           when((col("AppName") == 'CHANNEL') | (col("AppName") == 'DSHD') |
                                (col("AppName") == 'KPLUS') | (col("AppName") == 'KPlus'), "Truyền Hình")
                           .when((col("AppName") == 'VOD') | (col("AppName") == 'FIMS_RES') |
                                 (col("AppName") == 'BHD_RES') | (col("AppName") == 'VOD_RES') |
                                 (col("AppName") == 'FIMS') | (col("AppName") == 'BHD') |
                                 (col("AppName") == 'DANET'), "Phim Truyện")
                           .when((col("AppName") == 'RELAX'), "Giải Trí")
                           .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
                           .when((col("AppName") == 'SPORT'), "Thể Thao")
                           .otherwise("Error"))
        return df

    def calculate_statistics(self, df):
        """
        Tính toán thống kê theo Contract và Type
        """
        statistics = df.select('Contract', 'TotalDuration', 'Type').groupBy(
            'Contract', 'Type').sum()
        statistics = statistics.withColumnRenamed(
            'sum(TotalDuration)', 'TotalDuration')
        statistics = statistics.groupBy('Contract').pivot(
            'Type').sum('TotalDuration').na.fill(0)
        return statistics

    def calculate_most_watch(self, statistics):
        """
        Tính MostWatchContentType và MostWatchDuration theo Contract
        dựa trên tổng thời lượng theo từng loại nội dung (Type).

        Logic tương đương phần MostWatch trong stored procedure DM_ContractAnalytics:
        - Chọn content type có TotalDuration lớn nhất > 0
        - Nếu tất cả đều <= 0 thì để MostWatchContentType = NULL, MostWatchDuration = 0
        - Thứ tự ưu tiên khi bằng nhau: Truyền Hình → Phim Truyện → Giải Trí → Thiếu Nhi → Thể Thao
        """
        # Các cột duration theo từng content type (phải trùng với giá trị cột Type sau pivot)
        content_cols = ["Truyền Hình", "Phim Truyện",
                        "Giải Trí", "Thiếu Nhi", "Thể Thao"]

        # Giá trị duration lớn nhất trong 5 content type
        max_duration = greatest(*[col(c) for c in content_cols])

        # Xác định content type tương ứng với max_duration, chỉ lấy khi > 0
        most_type = (
            when((col("Truyền Hình") == max_duration) & (
                col("Truyền Hình") > 0), lit("Truyền Hình"))
            .when((col("Phim Truyện") == max_duration) & (col("Phim Truyện") > 0), lit("Phim Truyện"))
            .when((col("Giải Trí") == max_duration) & (col("Giải Trí") > 0), lit("Giải Trí"))
            .when((col("Thiếu Nhi") == max_duration) & (col("Thiếu Nhi") > 0), lit("Thiếu Nhi"))
            .when((col("Thể Thao") == max_duration) & (col("Thể Thao") > 0), lit("Thể Thao"))
            .otherwise(lit(None))
        )

        statistics = statistics.withColumn("MostWatchContentType", most_type)
        statistics = statistics.withColumn(
            "MostWatchDuration",
            when(max_duration > 0, max_duration).otherwise(lit(0))
        )

        return statistics

    def finalize_result(self, statistics, total_devices):
        """
        Kết hợp statistics và total_devices
        """
        result = statistics.join(total_devices, "Contract", 'inner')
        return result

    def save_to_csv(self, result, save_path):
        """
        Lưu kết quả ra CSV
        """
        print(f"Đang lưu vào CSV: {save_path}")
        result.repartition(1).write.mode("overwrite").option(
            "header", "true").csv(save_path)
        print("Data Saved Successfully to CSV")

    def save_to_database(self, result, server, database, table, username=None, password=None):
        """
        Lưu kết quả vào SQL Server
        """
        if not DB_AVAILABLE:
            raise ImportError(
                "pyodbc không được cài đặt. Không thể lưu vào database.")

        print(f"Đang lưu vào SQL Server: {server}/{database}.{table}")

        # Tạo connection string
        if username and password:
            connection_string = f"jdbc:sqlserver://{server};databaseName={database};user={username};password={password}"
        else:
            # Windows Authentication
            connection_string = f"jdbc:sqlserver://{server};databaseName={database};integratedSecurity=true"

        # Write to SQL Server
        result.write \
            .format("jdbc") \
            .option("url", connection_string) \
            .option("dbtable", table) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("overwrite") \
            .save()

        print(f"Data Saved Successfully to {table}")

    def process_etl(self, file_paths, output_type="csv", save_path=None,
                    db_server=None, db_name=None, db_table=None, db_user=None, db_password=None):
        """
        Thực hiện ETL pipeline
        """
        print("=" * 60)
        print("Bắt đầu ETL Process...")
        print("=" * 60)

        # Extract
        print("\n[1/6] Extract - Đọc dữ liệu từ file(s)...")
        df = self.read_data_from_paths(file_paths)

        # Transform
        print("\n[2/6] Transform - Chọn fields...")
        df = self.select_fields(df)

        print("\n[3/6] Transform - Tính toán Devices...")
        total_devices = self.calculate_devices(df)

        print("\n[4/6] Transform - Transform Category...")
        df = self.transform_category(df)

        print("\n[5/6] Transform - Tính toán Statistics...")
        statistics = self.calculate_statistics(df)

        print("\n[5.1/6] Transform - Tính toán MostWatch (Customer Taste)...")
        statistics = self.calculate_most_watch(statistics)

        print("\n[6/6] Transform - Finalize Result...")
        result = self.finalize_result(statistics, total_devices)

        total_rows = result.count()
        print(f"\nTổng số dòng kết quả: {total_rows}")

        # Load
        print("\n[Load] Đang lưu kết quả...")
        if output_type.lower() == "csv":
            if not save_path:
                raise ValueError("save_path không được để trống khi chọn CSV")
            self.save_to_csv(result, save_path)
        elif output_type.lower() == "db" or output_type.lower() == "database":
            if not all([db_server, db_name, db_table]):
                raise ValueError(
                    "Thiếu thông tin database (server, database, table)")
            self.save_to_database(result, db_server, db_name,
                                  db_table, db_user, db_password)
        else:
            raise ValueError(
                f"Output type không hợp lệ: {output_type}. Chọn 'csv' hoặc 'db'")

        print("\n" + "=" * 60)
        print("ETL Process hoàn thành!")
        print("=" * 60)

        return result

    def run_interactive(self):
        """
        Chạy ứng dụng ở chế độ interactive
        """
        print("=" * 70)
        print("ETL APPLICATION - Phần mềm ETL hoàn chỉnh")
        print("=" * 70)

        # Nhập đường dẫn input
        print("\n[1] Nhập đường dẫn chứa data:")
        input_path = input("Đường dẫn thư mục chứa file JSON: ").strip()
        if not os.path.exists(input_path):
            print(f"Lỗi: Thư mục không tồn tại: {input_path}")
            return

        # Chọn chế độ lấy file
        print("\n[2] Chọn chế độ lấy file:")
        print("  1. Chọn khoảng ngày (từ ngày đến ngày)")
        print("  2. Chọn tất cả file trong thư mục")
        mode_choice = input("Nhập lựa chọn (1 hoặc 2): ").strip()

        file_paths = []
        if mode_choice == "1":
            print("\nChế độ: Chọn khoảng ngày")
            print("Format ngày: YYYYMMDD (ví dụ: 20220401)")
            start_date = input("Nhập ngày bắt đầu (YYYYMMDD): ").strip()
            end_date = input("Nhập ngày kết thúc (YYYYMMDD): ").strip()
            try:
                file_paths = self.get_date_range_files(
                    input_path, start_date, end_date)
            except ValueError as e:
                print(f"Lỗi: {e}")
                return
        elif mode_choice == "2":
            print("\nChế độ: Lấy tất cả file trong thư mục")
            file_paths = self.get_files_from_folder(input_path)
        else:
            print("Lựa chọn không hợp lệ!")
            return

        if not file_paths:
            print("Không tìm thấy file nào!")
            return

        print(f"\nTìm thấy {len(file_paths)} file(s)")
        for i, fp in enumerate(file_paths[:5], 1):
            print(f"  {i}. {os.path.basename(fp)}")
        if len(file_paths) > 5:
            print(f"  ... và {len(file_paths) - 5} file(s) khác")

        # Chọn output type
        print("\n[3] Chọn định dạng output:")
        print("  1. CSV File")
        print("  2. SQL Server Database")
        output_choice = input("Nhập lựa chọn (1 hoặc 2): ").strip()

        if output_choice == "1":
            output_path = input("\nNhập đường dẫn lưu file CSV: ").strip()
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(output_path, exist_ok=True)

            # Xác nhận
            confirm = input(
                f"\nXác nhận chạy ETL cho {len(file_paths)} file(s)? (yes/no): ").strip().lower()
            if confirm in ['yes', 'y']:
                self.process_etl(file_paths, output_type="csv",
                                 save_path=output_path)
            else:
                print("Đã hủy!")

        elif output_choice == "2":
            if not DB_AVAILABLE:
                print("Lỗi: pyodbc chưa được cài đặt. Không thể kết nối database.")
                print("Cài đặt: pip install pyodbc")
                return

            print("\nNhập thông tin SQL Server:")
            db_server = input(
                "Server (ví dụ: localhost hoặc localhost\\SQLEXPRESS): ").strip()
            db_name = input("Database name: ").strip()
            db_table = input("Table name: ").strip()

            auth_choice = input(
                "Authentication (1-Windows, 2-SQL Server): ").strip()
            db_user = None
            db_password = None
            if auth_choice == "2":
                db_user = input("Username: ").strip()
                db_password = input("Password: ").strip()

            # Xác nhận
            confirm = input(
                f"\nXác nhận chạy ETL cho {len(file_paths)} file(s) và lưu vào {db_table}? (yes/no): ").strip().lower()
            if confirm in ['yes', 'y']:
                self.process_etl(file_paths, output_type="db",
                                 db_server=db_server, db_name=db_name, db_table=db_table,
                                 db_user=db_user, db_password=db_password)
            else:
                print("Đã hủy!")
        else:
            print("Lựa chọn không hợp lệ!")


def main():
    app = ETLApplication()
    app.run_interactive()


if __name__ == "__main__":
    main()
