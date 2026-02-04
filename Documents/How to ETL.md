### Làm thế nào để ETL data 1 ngày :

- Input : Contract , AppName , TotalDuration 

- Output : Contract , TVDuration , MovieDuration , ChildDuration , RelaxDuration , SportDuration 

## Để từ input ra được output : 

1. Đọc data từ nguồn 
2. Đưa appname về các category 
3. Summarize data mới ( Group by contract & category & Sum ) 
4. Pivot data 


## Thuật toán xử lý data theo ngày / tháng 

Bạn sẽ có 1 list data cần xử lý 

file_name = [0,1,2,3,4,5] 

=> Cần 1 vòng for để đi từng file data và xử lý 

Bài toán ở đây là đọc hết và xử lý hết 30 file 

Có 2 hướng làm : 

- Hướng 1 : Đọc dữ liệu của 30 file , sau đó mới tính toán 
- Hướng 2 : Đọc và xử lý từng file , sau đó gộp tất cả kết quả lại và group by sum 

Home work : edit code để ETL trong khoảng date range mình muốn 