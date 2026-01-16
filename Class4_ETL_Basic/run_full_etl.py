"""
Script Master để chạy toàn bộ quy trình ETL tự động
Bao gồm:
1. Load data vào Staging
2. Chạy stored procedures (DW và DM)
3. Khởi động Streamlit dashboard với real-time
"""

from ETL_Application import ETLApplication
from ETL_LoadToDatabase import ETLDatabaseLoader
from datetime import datetime
import time
import os
import sys
import subprocess
import pyodbc
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import findspark
findspark.init()


# Thêm thư mục hiện tại vào path để import các module
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)


class FullETLPipeline:
    def __init__(self, server, database_dw="DW_MediaAnalytics", database_dm="DM_MediaAnalytics",
                 username=None, password=None, port=1433, encrypt=True, trust_server_certificate=True):
        """
        Khởi tạo pipeline với thông tin database
        """
        self.server = server
        self.database_dw = database_dw
        self.database_dm = database_dm
        self.username = username
        self.password = password
        self.port = port
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate

        # Tạo server string với port
        # Trong __init__
        if ':' in server:
            # Chuyển localhost:1433 thành localhost,1433
            server_with_port = server.replace(':', ',')
        else:
            server_with_port = f"{server},{port}"

        # Tạo connection string cho pyodbc
        if username and password:
            encrypt_str = "yes" if encrypt else "no"
            self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database_dw};UID={username};PWD={password};Encrypt={encrypt_str};TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"
        else:
            encrypt_str = "yes" if encrypt else "no"
            self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database_dw};Trusted_Connection=yes;Encrypt={encrypt_str};TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"

    def execute_stored_procedure(self, database, procedure_name, process_date=None):
        """
        Chạy stored procedure từ Python
        """
        print(f"\n{'='*60}")
        print(f"Chạy stored procedure: {database}.dbo.{procedure_name}")
        print(f"{'='*60}")

        try:
            # Tạo server string với port
            if self.server.lower() == 'localhost':
                # Dùng IP và dấu phẩy cho ODBC
                server_with_port = f"127.0.0.1,{self.port}"
            else:
                # ODBC dùng dấu phẩy để phân tách port
                server_with_port = self.server.replace(':', ',')

            print(f"Kết nối đến: {server_with_port}")
            print(f"Database: {database}")

            # Tạo connection với database cụ thể
            encrypt_str = "yes" if self.encrypt else "no"
            trust_cert_str = "yes" if self.trust_server_certificate else "no"

            if self.username and self.password:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database};UID={self.username};PWD={self.password};Encrypt={encrypt_str};TrustServerCertificate={trust_cert_str}"
                print(f"Authentication: SQL Server (User: {self.username})")
            else:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database};Trusted_Connection=yes;Encrypt={encrypt_str};TrustServerCertificate={trust_cert_str}"
                print(f"Authentication: Windows Authentication")

            print(
                f"Encrypt: {encrypt_str}, TrustServerCertificate: {trust_cert_str}")
            print("Đang kết nối...")

            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()

            print("✓ Kết nối thành công!")

            # Chạy stored procedure
            print(f"Đang chạy stored procedure...")
            if process_date:
                cursor.execute(
                    f"EXEC {procedure_name} @ProcessDate = ?", process_date)
            else:
                cursor.execute(f"EXEC {procedure_name} @ProcessDate = NULL")

            # Commit transaction
            conn.commit()

            print(f"✓ Đã chạy thành công: {procedure_name}")

            cursor.close()
            conn.close()
            return True

        except pyodbc.Error as e:
            print(f"✗ Lỗi kết nối SQL Server khi chạy {procedure_name}:")
            print(f"   Error code: {e.args[0] if e.args else 'Unknown'}")
            print(
                f"   Error message: {e.args[1] if len(e.args) > 1 else str(e)}")
            print(f"\n💡 Kiểm tra:")
            print(f"   1. SQL Server đang chạy?")
            print(
                f"   2. Server name đúng chưa? (Hiện tại: {self.server}:{self.port})")
            print(f"   3. Port {self.port} có mở không?")
            print(f"   4. Firewall có chặn không?")
            print(f"   5. Database '{database}' có tồn tại không?")
            if self.username:
                print(f"   6. User '{self.username}' có quyền EXECUTE không?")
            import traceback
            print("\nChi tiết lỗi:")
            traceback.print_exc()
            return False
        except Exception as e:
            print(f"✗ Lỗi khi chạy {procedure_name}: {e}")
            import traceback
            print("\nChi tiết lỗi:")
            traceback.print_exc()
            return False

    def run_etl_pipeline(self, file_paths, process_date=None):
        """
        Chạy toàn bộ pipeline ETL
        """
        print("\n" + "="*70)
        print("BẮT ĐẦU FULL ETL PIPELINE")
        print("="*70)

        # Bước 1: Load vào Staging
        print("\n[1/3] Load data vào Staging_RawData...")
        print(f"Server: {self.server}:{self.port}")
        print(f"Database: {self.database_dw}")
        loader = ETLDatabaseLoader(self.server, self.database_dw, self.username, self.password,
                                   self.port, encrypt=self.encrypt, trust_server_certificate=self.trust_server_certificate)
        success = loader.process_files_to_database(file_paths)

        if not success:
            print("✗ Lỗi khi load vào Staging. Dừng pipeline.")
            return False

        # Bước 2: Chạy stored procedure để load vào Data Warehouse
        print("\n[2/3] Load từ Staging vào Data Warehouse...")
        success = self.execute_stored_procedure(
            self.database_dw,
            "sp_ELT_LoadToDataWarehouse",
            process_date
        )

        if not success:
            print("✗ Lỗi khi load vào Data Warehouse. Dừng pipeline.")
            return False

        # Bước 3: Chạy stored procedure để load vào Data Mart
        print("\n[3/3] Load từ Data Warehouse xuống Data Mart...")
        success = self.execute_stored_procedure(
            self.database_dm,
            "sp_ELT_LoadToDataMart",
            process_date
        )

        if not success:
            print("✗ Lỗi khi load vào Data Mart. Dừng pipeline.")
            return False

        print("\n" + "="*70)
        print("✓ FULL ETL PIPELINE HOÀN THÀNH!")
        print("="*70)
        return True

    def start_streamlit_dashboard(self, port=8501):
        """
        Khởi động Streamlit dashboard với real-time
        """
        print(f"\n{'='*70}")
        print(f"KHỞI ĐỘNG STREAMLIT DASHBOARD (Port: {port})")
        print(f"{'='*70}")
        print("Dashboard sẽ tự động refresh real-time")
        print(f"Truy cập: http://localhost:{port}")
        print("\nNhấn Ctrl+C để dừng dashboard")
        print("="*70 + "\n")

        # Đường dẫn đến app.py
        dashboard_path = os.path.join(
            os.path.dirname(__file__), "Dashboard", "app.py")

        if not os.path.exists(dashboard_path):
            print(f"✗ Không tìm thấy file: {dashboard_path}")
            return

        try:
            # Chạy streamlit
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", dashboard_path,
                "--server.port", str(port),
                "--server.headless", "true"
            ])
        except KeyboardInterrupt:
            print("\n✓ Đã dừng dashboard")
        except Exception as e:
            print(f"✗ Lỗi khi khởi động dashboard: {e}")


def main():
    """
    Main function với interactive mode
    """
    print("="*70)
    print("FULL ETL PIPELINE - Tự động hóa toàn bộ quy trình")
    print("="*70)

    # Nhập thông tin database
    print("\n[1] Cấu hình Database:")
    print("Ví dụ: host.docker.internal:1433 hoặc localhost")
    server = input(
        "SQL Server (ví dụ: localhost hoặc host.docker.internal:1433): ").strip()

    # Tách port nếu có trong server string
    port = 1433
    if ':' in server:
        parts = server.split(':')
        server = parts[0]
        try:
            port = int(parts[1])
        except:
            port = 1433

    database_dw = input(
        "Data Warehouse (DW_MediaAnalytics): ").strip() or "DW_MediaAnalytics"
    database_dm = input(
        "Data Mart (DM_MediaAnalytics): ").strip() or "DM_MediaAnalytics"

    auth_choice = input("Authentication (1-Windows, 2-SQL Server): ").strip()
    username = None
    password = None
    if auth_choice == "2":
        username = input("Username (ví dụ: grafana_user): ").strip()
        password = input("Password (ví dụ: Grafana@123): ").strip()

    # Cấu hình Encrypt
    encrypt_choice = input(
        "Encrypt (1-Enable, 2-Disable, Enter=Enable): ").strip() or "1"
    encrypt = (encrypt_choice == "1")

    trust_cert_choice = input(
        "Trust Server Certificate (1-Yes, 2-No, Enter=Yes): ").strip() or "1"
    trust_server_certificate = (trust_cert_choice == "1")

    # Nhập đường dẫn file
    print("\n[2] Chọn file data:")
    input_path = input("Đường dẫn thư mục chứa file JSON: ").strip()

    # Chọn mode
    print("\n[3] Chọn chế độ:")
    print("  1. Chọn khoảng ngày")
    print("  2. Tất cả file trong thư mục")
    mode = input("Lựa chọn (1 hoặc 2): ").strip()

    app = ETLApplication()
    file_paths = []

    if mode == "1":
        start_date = input("Ngày bắt đầu (YYYYMMDD): ").strip()
        end_date = input("Ngày kết thúc (YYYYMMDD): ").strip()
        file_paths = app.get_date_range_files(input_path, start_date, end_date)
    else:
        file_paths = app.get_files_from_folder(input_path)

    if not file_paths:
        print("✗ Không tìm thấy file nào!")
        return

    print(f"\n✓ Tìm thấy {len(file_paths)} file(s)")

    # Chọn process date
    process_date_choice = input(
        "\n[4] Process date (Enter để process tất cả, hoặc nhập YYYY-MM-DD): ").strip()
    process_date = None
    if process_date_choice:
        try:
            process_date = datetime.strptime(
                process_date_choice, "%Y-%m-%d").date()
        except:
            print("⚠ Format ngày không đúng, sẽ process tất cả")

    # Khởi tạo pipeline
    pipeline = FullETLPipeline(server, database_dw, database_dm, username, password,
                               port, encrypt, trust_server_certificate)

    # Chạy ETL pipeline
    print("\n[5] Bắt đầu chạy ETL Pipeline...")
    success = pipeline.run_etl_pipeline(file_paths, process_date)

    if not success:
        print("\n✗ Pipeline thất bại. Kiểm tra lỗi ở trên.")
        return

    # Hỏi có muốn chạy dashboard không
    print("\n[6] Khởi động Dashboard:")
    start_dashboard = input(
        "Bạn có muốn khởi động Streamlit Dashboard ngay? (yes/no): ").strip().lower()

    if start_dashboard in ['yes', 'y']:
        port = input("Port (mặc định 8501): ").strip() or "8501"
        try:
            port = int(port)
        except:
            port = 8501

        pipeline.start_streamlit_dashboard(port)
    else:
        print("\n✓ Để khởi động dashboard sau, chạy lệnh:")
        print("  cd Class4_ETL_Basic/Dashboard")
        print("  streamlit run app.py")


if __name__ == "__main__":
    main()
