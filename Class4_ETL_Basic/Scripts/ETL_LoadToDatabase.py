"""
ETL Script để load data từ Spark vào SQL Server Database
Script này sẽ load data vào Staging_RawData table trong DW
"""

import os
import sys
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import findspark
findspark.init()


class ETLDatabaseLoader:
    def __init__(self, server, database, username=None, password=None, port=1433, encrypt=True, trust_server_certificate=True):
        self.spark = SparkSession.builder.config(
            "spark.driver.memory", "8g").getOrCreate()
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate

        # Tạo server string với port nếu cần
        if ':' in server:
            # Server đã có port trong string (ví dụ: host.docker.internal:1433)
            server_with_port = server
        else:
            # Thêm port vào server
            server_with_port = f"{server}:{port}"

        # Tạo JDBC connection string
        if username and password:
            # SQL Server Authentication
            encrypt_str = "true" if encrypt else "false"
            trust_cert_str = "true" if trust_server_certificate else "false"
            self.jdbc_url = f"jdbc:sqlserver://{server_with_port};databaseName={database};user={username};password={password};encrypt={encrypt_str};trustServerCertificate={trust_cert_str}"
        else:
            # Windows Authentication
            encrypt_str = "true" if encrypt else "false"
            trust_cert_str = "true" if trust_server_certificate else "false"
            self.jdbc_url = f"jdbc:sqlserver://{server_with_port};databaseName={database};integratedSecurity=true;encrypt={encrypt_str};trustServerCertificate={trust_cert_str}"

    def transform_to_staging_format(self, df):
        """
        Transform DataFrame để match với Staging_RawData schema
        """
        # Extract fields từ _source
        df = df.select("_source.*")

        # Thêm EventDate từ file name hoặc current date
        # Giả sử có field Date trong data, nếu không thì dùng current date
        if "Date" in df.columns:
            df = df.withColumn("EventDate", to_date(col("Date"), "yyyyMMdd"))
        else:
            df = df.withColumn("EventDate", current_date())

        # Select và rename columns để match staging table
        staging_df = df.select(
            col("Contract").alias("Contract"),
            col("Mac").alias("Mac"),
            col("AppName").alias("AppName"),
            col("TotalDuration").alias("TotalDuration"),
            col("EventDate").alias("EventDate")
        )

        return staging_df

    def load_to_staging(self, df, table_name="Staging_RawData"):
        """
        Load DataFrame vào Staging table trong SQL Server
        """
        print(f"Đang load data vào {self.database}.{table_name}...")
        print(f"Connection: {self.server}/{self.database}")

        try:
            # Kiểm tra số dòng trước khi load
            row_count = df.count()
            print(f"Số dòng sẽ load: {row_count:,}")

            if row_count == 0:
                print("⚠ Cảnh báo: Không có dữ liệu để load!")
                return False

            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("append") \
                .save()

            print(f"✓ Đã load thành công {row_count:,} dòng vào {table_name}")
            return True
        except Exception as e:
            print(f"✗ Lỗi khi load vào database: {e}")
            print(f"   Database: {self.database}")
            print(f"   Table: {table_name}")
            print(f"   Server: {self.server}")
            import traceback
            print("\nChi tiết lỗi:")
            traceback.print_exc()
            return False

    def process_files_to_database(self, file_paths, file_type="json"):
        """
        Process files và load vào database
        """
        print("=" * 60)
        print("ETL Process - Load vào Database")
        print("=" * 60)

        try:
            # Read files
            print(f"\n[1/3] Đọc {len(file_paths)} file(s)...")
            if not file_paths:
                print("✗ Không có file nào để đọc!")
                return False

            # Kiểm tra file đầu tiên có tồn tại không
            if not os.path.exists(file_paths[0]):
                print(f"✗ File không tồn tại: {file_paths[0]}")
                return False

            df = self.spark.read.format(file_type).load(file_paths[0])
            print(f"✓ Đã đọc file: {os.path.basename(file_paths[0])}")

            for file_path in file_paths[1:]:
                if not os.path.exists(file_path):
                    print(f"⚠ Cảnh báo: File không tồn tại: {file_path}")
                    continue
                try:
                    df_temp = self.spark.read.format(file_type).load(file_path)
                    df = df.unionByName(df_temp, allowMissingColumns=True)
                    print(f"✓ Đã đọc file: {os.path.basename(file_path)}")
                except Exception as e:
                    print(f"⚠ Lỗi khi đọc file {file_path}: {e}")
                    continue

            row_count = df.count()
            print(f"✓ Tổng số dòng: {row_count:,}")

            if row_count == 0:
                print("⚠ Cảnh báo: Không có dữ liệu để load!")
                return False

            # Transform
            print("\n[2/3] Transform data...")
            staging_df = self.transform_to_staging_format(df)
            print("✓ Transform thành công")
            staging_df.show(10)

            # Load to database
            print("\n[3/3] Load vào database...")
            success = self.load_to_staging(staging_df)

            if success:
                print("\n" + "=" * 60)
                print("✓ ETL Process hoàn thành!")
                print("=" * 60)
                print("\nBước tiếp theo:")
                print(
                    "1. Chạy stored procedure: EXEC DW_MediaAnalytics.dbo.sp_ELT_LoadToDataWarehouse")
                print(
                    "2. Chạy stored procedure: EXEC DM_MediaAnalytics.dbo.sp_ELT_LoadToDataMart")
                print("=" * 60)

            return success

        except Exception as e:
            print(f"\n✗ Lỗi trong quá trình ETL: {e}")
            import traceback
            print("\nChi tiết lỗi:")
            traceback.print_exc()
            return False


def main():
    """
    Main function để chạy ETL load vào database
    """
    print("=" * 70)
    print("ETL DATABASE LOADER")
    print("=" * 70)

    # Nhập thông tin database
    server = input(
        "\nSQL Server (ví dụ: localhost hoặc localhost\\SQLEXPRESS): ").strip()
    database = input(
        "Database name (DW_MediaAnalytics): ").strip() or "DW_MediaAnalytics"

    auth_choice = input("Authentication (1-Windows, 2-SQL Server): ").strip()
    username = None
    password = None
    if auth_choice == "2":
        username = input("Username: ").strip()
        password = input("Password: ").strip()

    # Nhập đường dẫn file
    input_path = input("\nĐường dẫn thư mục chứa file JSON: ").strip()

    # Chọn mode
    print("\nChọn chế độ:")
    print("1. Chọn khoảng ngày")
    print("2. Tất cả file trong thư mục")
    mode = input("Lựa chọn (1 hoặc 2): ").strip()

    from ETL_Application import ETLApplication
    app = ETLApplication()

    file_paths = []
    if mode == "1":
        start_date = input("Ngày bắt đầu (YYYYMMDD): ").strip()
        end_date = input("Ngày kết thúc (YYYYMMDD): ").strip()
        file_paths = app.get_date_range_files(input_path, start_date, end_date)
    else:
        file_paths = app.get_files_from_folder(input_path)

    if not file_paths:
        print("Không tìm thấy file nào!")
        return

    print(f"\nTìm thấy {len(file_paths)} file(s)")

    # Load vào database
    loader = ETLDatabaseLoader(server, database, username, password)
    loader.process_files_to_database(file_paths)


if __name__ == "__main__":
    main()
