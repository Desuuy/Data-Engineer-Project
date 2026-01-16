import findspark
findspark.init()

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import * 

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

path = 'C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Data\\log_content\\20220401.json'
save_path = 'C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Class4_ETL_Basic\\Output'

def read_data_from_path(path):
	df = spark.read.json(path)
	return df 

def select_fields(df):	
	df = df.select("_source.*")
	return df 
	
def calculate_devices(df):
	total_devices = df.select("Contract","Mac").groupBy("Contract").count()
	total_devices = total_devices.withColumnRenamed('count','TotalDevices')
	return total_devices
	
def transform_category(df):
	df = df.withColumn("Type",
		   when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
		  .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
				 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
		  .when((col("AppName") == 'RELAX'), "Giải Trí")
		  .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
		  .when((col("AppName") == 'SPORT'), "Thể Thao")
		  .otherwise("Error"))
	return df 

def calculate_statistics(df): 
	statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
	statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
	statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
	return statistics 
	
def finalize_result(statistics,total_devices):
	result = statistics.join(total_devices,'Contract','inner')
	return result 
	
def save_data(result,save_path):
	result.repartition(1).write.option("header","true").csv(save_path)
	return print("Data Saved Successfully")


def main(path):
	print('-------------Reading data from path--------------')
	df = read_data_from_path(path)
	print('-------------Selecting fields--------------')
	df = select_fields(df)
	print('-------------Calculating Devices --------------')
	total_devices = calculate_devices(df)
	print('-------------Transforming Category --------------')
	df = transform_category(df)
	print('-------------Calculating Statistics --------------')
	statistics = calculate_statistics(df)
	print('-------------Finalizing result --------------')
	result = finalize_result(statistics,total_devices)
	print(result)
	print('-------------Saving Results --------------')
	save_data(result,save_path)
	return print('Task finished')
	
main(path)