import findspark
findspark.init()

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import os
import glob

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

# Base paths
base_data_path = "C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Data\\log_content"
save_path = "C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Class4_ETL_Basic\\Output"
file_type = "json"


def get_date_range_files(start_date_str, end_date_str):
    """
    Lấy danh sách các file JSON trong khoảng thời gian từ start_date đến end_date
    Format: YYYYMMDD (ví dụ: 20220401)
    """
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    file_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        file_path = os.path.join(base_data_path, f"{date_str}.json")
        if os.path.exists(file_path):
            file_list.append(file_path)
        current_date += timedelta(days=1)
    
    return file_list


def get_all_files():
    """
    Lấy tất cả các file JSON trong thư mục log_content
    """
    pattern = os.path.join(base_data_path, "*.json")
    file_list = glob.glob(pattern)
    return sorted(file_list)


def read_data_from_paths(file_paths, file_type):
    """
    Đọc và union tất cả các file trong danh sách
    """
    if not file_paths:
        raise ValueError("Không có file nào để đọc!")
    
    print(f"Đang đọc {len(file_paths)} file(s)...")
    
    # Đọc file đầu tiên
    df = spark.read.format(file_type).load(file_paths[0])
    
    # Union các file còn lại
    for file_path in file_paths[1:]:
        print(f"Đang đọc: {os.path.basename(file_path)}")
        df_temp = spark.read.format(file_type).load(file_path)
        df = df.unionByName(df_temp, allowMissingColumns=True)
    
    print(f"Đã đọc xong {len(file_paths)} file(s)")
    df.show(10)
    return df


def read_data_from_path(file_path, file_type):
    """
    Đọc một file duy nhất (giữ lại để tương thích)
    """
    df = spark.read.format(file_type).load(file_path)
    df.show(10)
    return df


def select_fields(df):
    """
    Lấy tất cả các cột từ _source
    """
    df = df.select("_source.*")
    return df


def calculate_devices(df):
    """
    Tính tổng số thiết bị theo Contract
    """
    total_devices = df.select("Contract", "Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count', 'TotalDevices')
    return total_devices


def transform_category(df):
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


def calculate_statistics(df):
    """
    Tính toán thống kê theo Contract và Type
    """
    statistics = df.select('Contract', 'TotalDuration', 'Type').groupBy('Contract', 'Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics


def finalize_result(statistics, total_devices):
    """
    Kết hợp statistics và total_devices
    """
    result = statistics.join(total_devices, "Contract", 'inner')
    return result


def save_data(result, save_path):
    """
    Lưu kết quả ra CSV
    """
    # repartition(1): gôm tất cả dữ liệu về duy nhất 1 phần tử
    result.repartition(1).write.mode("overwrite").option("header", "true").csv(save_path)
    return print("Data Saved Successfully")


def process_etl(file_paths):
    """
    Thực hiện ETL pipeline cho danh sách file
    """
    print("Running Spark job...")
    print('-------------Reading data from paths--------------')
    df = read_data_from_paths(file_paths, file_type)
    
    print('-------------Selecting fields--------------')
    df = select_fields(df)
    
    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)
    
    print('-------------Transforming Category --------------')
    df = transform_category(df)
    
    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)
    
    print('-------------Finalizing result --------------')
    result = finalize_result(statistics, total_devices)
    print(f'Total rows: {result.count()}')
    
    print('-------------Saving Results --------------')
    save_data(result, save_path)
    
    print('Task finished')
    return result


def main():
    """
    Main function với menu chọn chế độ
    """
    print("=" * 60)
    print("ETL Script - Chọn chế độ xử lý")
    print("=" * 60)
    print("1. Chọn khoảng ngày (từ ngày đến ngày)")
    print("2. Xử lý tất cả (all)")
    print("=" * 60)
    
    choice = input("Nhập lựa chọn (1 hoặc 2): ").strip()
    
    if choice == "1":
        print("\nChế độ: Chọn khoảng ngày")
        print("Format ngày: YYYYMMDD (ví dụ: 20220401)")
        start_date = input("Nhập ngày bắt đầu (YYYYMMDD): ").strip()
        end_date = input("Nhập ngày kết thúc (YYYYMMDD): ").strip()
        
        try:
            file_paths = get_date_range_files(start_date, end_date)
            if not file_paths:
                print(f"Không tìm thấy file nào trong khoảng {start_date} đến {end_date}")
                return
            print(f"\nTìm thấy {len(file_paths)} file(s) trong khoảng thời gian này")
            process_etl(file_paths)
        except ValueError as e:
            print(f"Lỗi: {e}")
            print("Vui lòng nhập đúng format ngày (YYYYMMDD)")
    
    elif choice == "2" or choice.lower() == "all":
        print("\nChế độ: Xử lý tất cả")
        file_paths = get_all_files()
        if not file_paths:
            print("Không tìm thấy file nào trong thư mục!")
            return
        print(f"\nTìm thấy {len(file_paths)} file(s)")
        confirm = input(f"Bạn có chắc muốn xử lý {len(file_paths)} file(s)? (yes/no): ").strip().lower()
        if confirm in ['yes', 'y']:
            process_etl(file_paths)
        else:
            print("Đã hủy!")
    
    else:
        print("Lựa chọn không hợp lệ! Vui lòng chọn 1 hoặc 2")


if __name__ == "__main__":
    main()
