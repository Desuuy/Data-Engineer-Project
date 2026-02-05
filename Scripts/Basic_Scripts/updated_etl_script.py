import os
import re
from datetime import datetime, timedelta

import findspark
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    datediff,
    greatest,
    lit,
    max,
    min,
    to_date,
    when,
)

findspark.init()
spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

# Đường dẫn mặc định (có thể không còn tồn tại trên máy hiện tại)
DEFAULT_FILE_PATH = r"C:\Users\anhhu\Downloads\Study_DE\BD_Class_4_Project\Data\log_content"
DEFAULT_SAVE_PATH = r"C:\Users\anhhu\Downloads\Study_DE\BD_Class_4_Project\Output"
file_type = "json"


def get_date_range_files(base_path, start_date_str, end_date_str):
    """
    Lấy danh sách file JSON theo khoảng ngày.
    Format: YYYYMMDD (ví dụ: 20220401)
    """
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")

    file_list = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        fp = os.path.join(base_path, f"{date_str}.json")
        if os.path.exists(fp):
            file_list.append(fp)
        current_date += timedelta(days=1)

    return sorted(file_list)


def get_all_files(base_path):
    """
    Lấy tất cả file JSON trong folder.
    """
    if not os.path.exists(base_path):
        raise ValueError(f"Folder không tồn tại: {base_path}")
    return sorted([os.path.join(base_path, f) for f in os.listdir(base_path) if f.lower().endswith(".json")])


def extract_datevalue_from_filename(file_path):
    """
    Parse YYYYMMDD từ tên file, ví dụ: 20220401.json -> 20220401.
    """
    name = os.path.basename(file_path)
    m = re.search(r"(\d{8})", name)
    return m.group(1) if m else None


def read_data_from_paths(file_paths, file_type):
    """
    Đọc và union nhiều file, đồng thời add cột DateValue (YYYYMMDD) từ tên file.
    """
    if not file_paths:
        raise ValueError("Không có file nào để đọc!")

    print(f"Đang đọc {len(file_paths)} file(s)...")

    dfs = []
    for fp in file_paths:
        date_str = extract_datevalue_from_filename(fp)
        if not date_str:
            print(f"⚠ Không parse được ngày từ file: {fp} (bỏ qua file này)")
            continue

        df_one = spark.read.format(file_type).load(fp)
        df_one = df_one.withColumn("DateValue", lit(date_str))
        dfs.append(df_one)

    if not dfs:
        raise ValueError("Không có file hợp lệ (không parse được YYYYMMDD từ tên file).")

    df = dfs[0]
    for df_one in dfs[1:]:
        df = df.unionByName(df_one, allowMissingColumns=True)

    df.show(10)
    return df


def read_data_from_path(file_path, file_type):
    df = spark.read.format(file_type).load(file_path)
    df.show(10)
    return df


def select_fields(df):
    """
    Lấy tất cả các cột từ _source và giữ lại DateValue (nếu có)
    để phục vụ tính Activation nhiều ngày.
    """
    if "DateValue" in df.columns:
        df = df.select("_source.*", "DateValue")
    else:
        df = df.select("_source.*")
    return df


def calculate_devices(df):
    total_devices = df.select("Contract", "Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count', 'TotalDevices')
    return total_devices


def transform_category(df):
    df = df.withColumn("Type",
                       when((col("AppName") == 'CHANNEL') | (col("AppName") == 'DSHD') | (
                           col("AppName") == 'KPLUS') | (col("AppName") == 'KPlus'), "Truyền Hình")
                       .when((col("AppName") == 'VOD') | (col("AppName") == 'FIMS_RES') | (col("AppName") == 'BHD_RES') |
                             (col("AppName") == 'VOD_RES') | (col("AppName") == 'FIMS') | (col("AppName") == 'BHD') | (col("AppName") == 'DANET'), "Phim Truyện")
                       .when((col("AppName") == 'RELAX'), "Giải Trí")
                       .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
                       .when((col("AppName") == 'SPORT'), "Thể Thao")
                       .otherwise("Error"))
    return df


def calculate_statistics(df):
    statistics = df.select('Contract', 'TotalDuration',
                           'Type').groupBy('Contract', 'Type').sum()
    statistics = statistics.withColumnRenamed(
        'sum(TotalDuration)', 'TotalDuration')
    statistics = statistics.groupBy('Contract').pivot(
        'Type').sum('TotalDuration').na.fill(0)
    return statistics


def calculate_most_watch(statistics):
    """
    MostWatch: chọn content type có duration lớn nhất (> 0), ưu tiên
    Truyền Hình → Phim Truyện → Giải Trí → Thiếu Nhi → Thể Thao.
    """
    content_cols = ["Truyền Hình", "Phim Truyện", "Giải Trí", "Thiếu Nhi", "Thể Thao"]
    max_duration = greatest(*[col(c) for c in content_cols])

    most_type = (
        when((col("Truyền Hình") == max_duration) & (col("Truyền Hình") > 0), lit("Truyền Hình"))
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


def calculate_activation(df):
    """
    Activation: dựa trên ngày đầu tiên và cuối cùng của từng Contract.
    Rule: <10 ngày: Thấp, <20: Trung bình, còn lại: Cao.
    Cần cột DateValue (yyyyMMdd hoặc date). Nếu không có, bỏ qua.
    """
    if "DateValue" not in df.columns:
        print("⚠ Không có cột DateValue, bỏ qua tính Activation.")
        return None

    df_dates = df.withColumn(
        "DateValueParsed",
        coalesce(
            to_date(col("DateValue").cast("string"), "yyyyMMdd"),
            to_date(col("DateValue"))
        )
    )

    if df_dates.filter(col("DateValueParsed").isNotNull()).count() == 0:
        print("⚠ Không parse được DateValue, bỏ qua Activation.")
        return None

    w = Window.partitionBy("Contract")
    df_dates = df_dates.withColumn("FirstDate", min(col("DateValueParsed")).over(w)) \
                       .withColumn("LastDate", max(col("DateValueParsed")).over(w))

    df_dates = df_dates.withColumn(
        "ActivationDays",
        datediff(col("LastDate"), col("FirstDate")) + lit(1)
    )

    df_dates = df_dates.withColumn(
        "ActivationLevel",
        when(col("ActivationDays") < 10, "Thấp")
        .when(col("ActivationDays") < 20, "Trung bình")
        .otherwise("Cao")
    )

    activation_df = df_dates.select("Contract", "ActivationDays", "ActivationLevel").dropDuplicates(["Contract"])
    return activation_df


def finalize_result(statistics, total_devices, activation_df=None):
    result = statistics.join(total_devices, "Contract", 'inner')
    if activation_df is not None:
        result = result.join(activation_df, "Contract", 'left')
    return result


def save_data(result, save_path):
    # repartition(1): gôm tất cả dữ liệu về duy nhất 1 phần tửtử
    result.repartition(1).write.mode("overwrite").option(
        "header", "true").csv(save_path)
    return print("Data Saved Successfully")


def main():
    print("Running Spark job...")

    print("=" * 60)
    print("ETL BASIC (Show result on terminal)")
    print("=" * 60)
    print("1. ETL 1 ngày (1 file)")
    print("2. ETL nhiều ngày (date range)")
    print("3. ETL tất cả file trong folder (all)")
    print("=" * 60)
    mode = input("Chọn mode (1/2/3): ").strip()

    df = None
    if mode == "1":
        file_path = DEFAULT_FILE_PATH
        if not os.path.exists(file_path):
            print(f"Default file không tồn tại:\n  {file_path}")
            file_path = input(
                "\nNhập đường dẫn đầy đủ tới file JSON (ví dụ: C:\\Data\\log_content\\20220401.json): "
            ).strip()

        if not os.path.exists(file_path):
            print(f"Lỗi: File không tồn tại: {file_path}")
            return

        print('-------------Reading data from path--------------')
        print(f"Input file: {file_path}")
        df = read_data_from_path(file_path, file_type)

        # gắn DateValue theo tên file để Activation chạy được (ActivationDays sẽ = 1)
        date_str = extract_datevalue_from_filename(file_path)
        if date_str:
            df = df.withColumn("DateValue", lit(date_str))

    elif mode == "2":
        base_path = input("\nNhập folder chứa các file JSON (ví dụ: C:\\Data\\log_content): ").strip()
        if not os.path.exists(base_path):
            print(f"Lỗi: Folder không tồn tại: {base_path}")
            return

        print("Format ngày: YYYYMMDD (ví dụ: 20220401)")
        start_date = input("Nhập ngày bắt đầu (YYYYMMDD): ").strip()
        end_date = input("Nhập ngày kết thúc (YYYYMMDD): ").strip()

        try:
            file_paths = get_date_range_files(base_path, start_date, end_date)
        except Exception as e:
            print(f"Lỗi parse ngày: {e}")
            return

        if not file_paths:
            print("Không tìm thấy file nào trong khoảng ngày đã nhập.")
            return

        print('-------------Reading data from paths--------------')
        df = read_data_from_paths(file_paths, file_type)

    elif mode == "3":
        base_path = input("\nNhập folder chứa các file JSON (ví dụ: C:\\Data\\log_content): ").strip()
        try:
            file_paths = get_all_files(base_path)
        except Exception as e:
            print(f"Lỗi: {e}")
            return

        if not file_paths:
            print("Không tìm thấy file JSON nào trong folder!")
            return

        confirm = input(f"Tìm thấy {len(file_paths)} file. Chạy hết? (yes/no): ").strip().lower()
        if confirm not in ["yes", "y"]:
            print("Đã hủy!")
            return

        print('-------------Reading data from paths--------------')
        df = read_data_from_paths(file_paths, file_type)

    else:
        print("Mode không hợp lệ.")
        return
    print('-------------Selecting fields--------------')
    df = select_fields(df)
    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)
    print('-------------Transforming Category --------------')
    df = transform_category(df)
    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)
    print('-------------Calculating MostWatch --------------')
    statistics = calculate_most_watch(statistics)
    print('-------------Calculating Activation --------------')
    activation_df = calculate_activation(df)
    print('-------------Finalizing result --------------')
    result = finalize_result(statistics, total_devices, activation_df)
    print(f'Total rows: {result.count()}')
    result.show(50, truncate=False)

    # 2. Chọn output folder (có thể bỏ qua)
    save_path = DEFAULT_SAVE_PATH
    if not os.path.exists(save_path):
        user_save = input(
            "\nNhập thư mục lưu CSV (Enter để bỏ qua lưu file, ví dụ: C:\\Output\\ETL_Result): "
        ).strip()
        if user_save:
            save_path = user_save
        else:
            save_path = None

    if save_path:
        os.makedirs(save_path, exist_ok=True)
        print('-------------Saving Results --------------')
        print(f"Output folder: {save_path}")
        save_data(result, save_path)
    else:
        print("Bỏ qua bước lưu CSV (chỉ show kết quả trên terminal).")

    print('Show 10 rows of result: ', result.show(10, truncate=False))
    return print('Task finished')


if __name__ == "__main__":
    main()
