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

# Setup Spark Session
findspark.init()
spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

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
    Read and union multiple files, add DateValue (YYYYMMDD) column from filename.
    """
    if not file_paths:
        raise ValueError("No file to read!")

    print(f"Reading {len(file_paths)} file(s)...")

    dfs = []
    for fp in file_paths:
        date_str = extract_datevalue_from_filename(fp)
        if not date_str:
            print(f"Cannot parse date from file: {fp} (skip this file)")
            continue

        df_one = spark.read.format(file_type).load(fp)
        df_one = df_one.withColumn("DateValue", lit(date_str))
        dfs.append(df_one)

    if not dfs:
        raise ValueError("No valid file (cannot parse YYYYMMDD from filename).")

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
    Select all columns from _source and keep DateValue (if exists)
    to serve for multiple days Activation calculation.
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
                       when((col("AppName") == 'CHANNEL') | 
                            (col("AppName") == 'DSHD') | 
                            (col("AppName") == 'KPLUS') | 
                            (col("AppName") == 'KPlus'), "TV")
                       .when((col("AppName") == 'VOD') | 
                            (col("AppName") == 'FIMS_RES') | 
                            (col("AppName") == 'BHD_RES') |
                            (col("AppName") == 'VOD_RES') | 
                            (col("AppName") == 'FIMS') | 
                            (col("AppName") == 'BHD') | 
                            (col("AppName") == 'DANET'), "Movie")
                       .when((col("AppName") == 'RELAX'), "Entertainment")
                       .when((col("AppName") == 'CHILD'), "Kids")
                       .when((col("AppName") == 'SPORT'), "Sports")
                       .otherwise("Unknown"))
    return df


def calculate_statistics(df):
    statistics = df.select('Contract', 'TotalDuration', 'Type').groupBy('Contract', 'Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics


def calculate_most_watch(statistics):
    """
    MostWatch: select content type with the highest duration (> 0), prioritize
    TV → Movie → Entertainment → Kids → Sports.
    """
    content_cols = ["TV", "Movie", "Entertainment", "Kids", "Sports"]
    max_duration = greatest(*[col(c) for c in content_cols])

    most_type = (
        when((col("TV") == max_duration) & (col("TV") > 0), lit("TV"))
        .when((col("Movie") == max_duration) & (col("Movie") > 0), lit("Movie"))
        .when((col("Entertainment") == max_duration) & (col("Entertainment") > 0), lit("Entertainment"))
        .when((col("Kids") == max_duration) & (col("Kids") > 0), lit("Kids"))
        .when((col("Sports") == max_duration) & (col("Sports") > 0), lit("Sports"))
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
    Activation: based on the first and last date of each Contract.
    Rule: <10 days: Low, <20: Medium, otherwise: High.
    Need DateValue (yyyyMMdd or date) column. If not, skip.
    """
    if "DateValue" not in df.columns:
        print("Cannot find DateValue column, skip Activation calculation.")
        return None

    df_dates = df.withColumn(
        "DateValueParsed",
        coalesce(
            to_date(col("DateValue").cast("string"), "yyyyMMdd"),
            to_date(col("DateValue"))
        )
    )

    if df_dates.filter(col("DateValueParsed").isNotNull()).count() == 0:
        print("Cannot parse DateValue, skip Activation calculation.")
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
        when(col("ActivationDays") < 10, "Low")
        .when(col("ActivationDays") < 20, "Medium")
        .otherwise("High")
    )

    activation_df = df_dates.select("Contract", "ActivationDays", "ActivationLevel").dropDuplicates(["Contract"])
    return activation_df


def finalize_result(statistics, total_devices, activation_df=None):
    result = statistics.join(total_devices, "Contract", 'inner')
    if activation_df is not None:
        result = result.join(activation_df, "Contract", 'left')
    return result


def save_data(result, save_path):
    # repartition(1): combine all data into a single row
    result.repartition(1).write.mode("overwrite").option(
        "header", "true").csv(save_path)
    return print("Data Saved Successfully")


def main():
    print("Running Spark job...")

    print("=" * 60)
    print("ETL BASIC (Show result on terminal)")
    print("=" * 60)
    print("1. ETL 1 day (1 file)")
    print("2. ETL multiple days (date range)")
    print("3. ETL all files in folder (all)")
    print("=" * 60)
    mode = input("Select mode (1/2/3): ").strip()

    df = None
    if mode == "1":
        file_path = DEFAULT_FILE_PATH
        if not os.path.exists(file_path):
            print(f"Default file does not exist:\n  {file_path}")
            file_path = input(
                "\nEnter the full path to the JSON file (example: C:\\Data\\log_content\\20220401.json): "
            ).strip()

        if not os.path.exists(file_path):
            print(f"Lỗi: File không tồn tại: {file_path}")
            return

        print('-------------Reading data from path--------------')
        print(f"Input file: {file_path}")
        df = read_data_from_path(file_path, file_type)

        # Add DateValue from filename to serve for Activation calculation (ActivationDays will be = 1)
        date_str = extract_datevalue_from_filename(file_path)
        if date_str:
            df = df.withColumn("DateValue", lit(date_str))

    elif mode == "2":
        base_path = input("\nEnter the folder containing the JSON files: ").strip()
        if not os.path.exists(base_path):
            print(f"Error: Folder does not exist: {base_path}")
            return

        print("Date format: YYYYMMDD (example: 20220401)")
        start_date = input("Enter the start date (YYYYMMDD): ").strip()
        end_date = input("Enter the end date (YYYYMMDD): ").strip()

        try:
            file_paths = get_date_range_files(base_path, start_date, end_date)
        except Exception as e:
            print(f"Error parsing date: {e}")
            return

        if not file_paths:
            print("No file found in the date range.")
            return

        print('-------------Reading data from paths--------------')
        df = read_data_from_paths(file_paths, file_type)

    elif mode == "3":
        base_path = input("\nEnter the folder containing the JSON files: ").strip()
        try:
            file_paths = get_all_files(base_path)
        except Exception as e:
            print(f"Error: {e}")
            return

        if not file_paths:
            print("No JSON file found in the folder!")
            return

        confirm = input(f"Found {len(file_paths)} file. Run all? (yes/no): ").strip().lower()
        if confirm not in ["yes", "y"]:
            print("Cancelled!")
            return

        print('-------------Reading data from paths--------------')
        df = read_data_from_paths(file_paths, file_type)

    else:
        print("Invalid mode.")
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
    result.show(10, truncate=False)

    # 2. Select output folder (optional)
    save_path = DEFAULT_SAVE_PATH
    if not os.path.exists(save_path):
        user_save = input(
            "\nEnter the folder to save the CSV file: "
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
        print("Skip saving CSV (only show result on terminal).")

    print('Show 10 rows of result: ', result.show(10, truncate=False))
    return print('Task finished')


if __name__ == "__main__":
    main()
