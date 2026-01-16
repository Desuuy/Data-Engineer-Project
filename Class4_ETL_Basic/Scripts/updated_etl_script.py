import findspark
findspark.init()

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *



spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

file_path = "C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Data\\log_content\\20220401.json"
file_type = "json"
save_path = "C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Class4_ETL_Basic\\Output"


def read_data_from_path(file_path, file_type):
    df = spark.read.format(file_type).load(file_path)
    df.show(10)
    return df


def select_fields(df):
    # * Lấy tất cả các cột từ _source
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


def finalize_result(statistics, total_devices):
    result = statistics.join(total_devices, "Contract", 'inner')
    return result


def save_data(result, save_path):
    # repartition(1): gôm tất cả dữ liệu về duy nhất 1 phần tửtử
    result.repartition(1).write.mode("overwrite").option(
        "header", "true").csv(save_path)
    return print("Data Saved Successfully")


def main():
    print("Running Spark job...")
    print('-------------Reading data from path--------------')
    df = read_data_from_path(file_path, file_type)
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
    print('Show 10 rows of result: ', result.show(10, truncate=False))
    return print('Task finished')


if __name__ == "__main__":
    main()
