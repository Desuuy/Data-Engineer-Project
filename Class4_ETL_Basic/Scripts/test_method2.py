import os 
#import etl_script 
from datetime import datetime ,timedelta 

def input_path():
    url = str(input('Please provide datadata source folder'))
    return url

def output_path():
    url = str(input('Please provide destination folder'))
    return url

def list_files(path):
    list_files = os.listdir(input_path)
    print(list_files)
    print("How many files you want to ETL")
    return list_files

#input_path = input_path()
#output_path = output_path()
input_path = 'C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Data\\log_content'
output_path = 'C:\\Users\\anhhu\\Downloads\\Study_DE\\Project\\Class4_ETL_Basic\\Output'
list_files = list_files(input_path) 


start_date = str(input('Please input start_date format yyyymmdd: '))
start_date = datetime.strptime(start_date,"%Y%m%d").date()
to_date = str(input("Please input to_date format yyyymmdd: "))
to_date = datetime.strptime(to_date,"%Y%m%d").date()

date_list = []
current_date = start_date 
end_date = to_date
while (current_date <= end_date):
    date_list.append(current_date.strftime("%Y%m%d"))
    current_date += timedelta(days=1)
print(date_list) 


start_time = datetime.now()
# df = spark.read.csv(date_list[0])
# df = etl(df)                                              # Khac nhau o buoc nay 
for i in date_list[1:]:
    print("ETL_TASK" + input_path+"\\"+ i + ".json")        # Khac nhau o buoc nay 
    print("Union df with new df")
print("Calculation on final output")                        #Khac nhau o buoc nay 
end_time = datetime.now()
print((end_time - start_time).total_seconds())
