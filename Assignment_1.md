# PUTTING DATA INTO HDFS
[cloudera@quickstart ~]$ cd data
[cloudera@quickstart data]$ ls
array_data.csv  depart_data.csv  map_data.csv  sales_order_data.csv
[cloudera@quickstart data]$ cd
[cloudera@quickstart ~]$ hadoop fs -mkdir /tmp/hive_data_ass1
[cloudera@quickstart ~]$ hadoop fs -ls /tmp/
Found 5 items
drwxrwxrwt   - mapred   mapred              0 2017-10-23 09:15 /tmp/hadoop-yarn
drwx-wx-wx   - hive     supergroup          0 2022-09-08 10:25 /tmp/hive
drwxr-xr-x   - cloudera supergroup          0 2022-09-11 11:33 /tmp/hive_data_ass1
drwxr-xr-x   - cloudera supergroup          0 2022-09-08 13:07 /tmp/hive_data_class_2
drwxrwxrwt   - mapred   hadoop              0 2017-10-23 09:17 /tmp/logs
[cloudera@quickstart ~]$ hadoop fs -put /home/cloudera/data/sales_order_data.csv /tmp/hive_data_ass1
[cloudera@quickstart ~]$ hadoop fs -ls /tmp/hive_data_ass1
Found 1 items
-rw-r--r--   1 cloudera supergroup     360233 2022-09-11 11:34 /tmp/hive_data_ass1/sales_order_data.csv

# CREATING DATABASE AND TABLE SALES_ORDER_CSV:
[cloudera@quickstart ~]$ hive

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> create database hive_database_ass1;
OK
Time taken: 5.191 seconds
hive> use hive_database_ass1;
OK
Time taken: 0.09 seconds
hive> show tables;
OK
Time taken: 1.17 seconds
hive> create table sales_order_csv
    > (
    > ordernumber int,
    > quantityordered int,
    > priceeach float,
    > orderlinenumber int,
    > sales float,
    > status string,
    > qtr_id int,
    > month_id int,
    > year_id int,
    > productline string,
    > msrp int,
    > productcode string,
    > phone string,
    > city string,
    > state string,
    > postalcode string,
    > country string,
    > territory string,
    > contactlastname string,
    > contactfirstname string,
    > dealsize string
    > )
    > row format delimited
    > fields terminated by ','
    > tblproperties ("skip.header.line.count"="1");
OK
Time taken: 0.834 seconds
hive> show tables;
OK
sales_order_csv
Time taken: 0.082 seconds, Fetched: 1 row(s)
hive> describe sales_order_csv;
OK
ordernumber         	int                 	                    
quantityordered     	int                 	                    
priceeach           	float               	                    
orderlinenumber     	int                 	                    
sales               	float               	                    
status              	string              	                    
qtr_id              	int                 	                    
month_id            	int                 	                    
year_id             	int                 	                    
productline         	string              	                    
msrp                	int                 	                    
productcode         	string              	                    
phone               	string              	                    
city                	string              	                    
state               	string              	                    
postalcode          	string              	                    
country             	string              	                    
territory           	string              	                    
contactlastname     	string              	                    
contactfirstname    	string              	                    
dealsize            	string              	                    
Time taken: 0.534 seconds, Fetched: 21 row(s)

# LOADING DATA IN TABLE SALES_ORDER_CSV:

hive> load data inpath "/tmp/hive_data_ass1/sales_order_data.csv" into table sales_order_csv;
Loading data to table default.sales_order_csv
Table default.sales_order_csv stats: [numFiles=1, totalSize=360233]
OK
Time taken: 1.919 seconds

hive> set hive.cli.print.header=true;
hive> select * from sales_order_csv limit 10;
OK
sales_order_csv.ordernumber	sales_order_csv.quantityordered	sales_order_csv.priceeach	sales_order_csv.orderlinenumber	sales_order_csv.sales	sales_order_csv.status	sales_order_csv.qtr_id	sales_order_csv.month_id	sales_order_csv.year_id	sales_order_csv.productline	sales_order_csv.msrp	sales_order_csv.productcode	sales_order_csv.phonesales_order_csv.city	sales_order_csv.state	sales_order_csv.postalcode	sales_order_csv.country	sales_order_csv.territory	sales_order_csv.contactlastname	sales_order_csv.contactfirstname	sales_order_csv.dealsize
10107	30	95.7	2	2871.0	Shipped	1	2	2003	Motorcycles	95	S10_1678	2125557818	NYC	NY	10022	USA	NA	Yu	Kwai	Small
10121	34	81.35	5	2765.9	Shipped	2	5	2003	Motorcycles	95	S10_1678	26.47.1555	Reims		51100	France	EMEA	Henriot	Paul	Small
10134	41	94.74	2	3884.34	Shipped	3	7	2003	Motorcycles	95	S10_1678	+33 1 46 62 7555	Paris		75508	France	EMEA	Da Cunha	Daniel	Medium
10145	45	83.26	6	3746.7	Shipped	3	8	2003	Motorcycles	95	S10_1678	6265557265	Pasadena	CA	90003	USA	NA	Young	Julie	Medium
10159	49	100.0	14	5205.27	Shipped	4	10	2003	Motorcycles	95	S10_1678	6505551386	San Francisco	CA		USA	NA	Brown	Julie	Medium
10168	36	96.66	1	3479.76	Shipped	4	10	2003	Motorcycles	95	S10_1678	6505556809	Burlingame	CA	94217	USA	NA	Hirano	Juri	Medium
10180	29	86.13	9	2497.77	Shipped	4	11	2003	Motorcycles	95	S10_1678	20.16.1555	Lille		59000	France	EMEA	Rance	Martine	Small
10188	48	100.0	1	5512.32	Shipped	4	11	2003	Motorcycles	95	S10_1678	+47 2267 3215Bergen		N 5804	Norway	EMEA	Oeztan	Veysel	Medium
10201	22	98.57	2	2168.54	Shipped	4	12	2003	Motorcycles	95	S10_1678	6505555787	San Francisco	CA		USA	NA	Murphy	Julie	Small
10211	41	100.0	14	4708.44	Shipped	1	1	2004	Motorcycles	95	S10_1678	(1) 47.55.6555	Paris		75016	France	EMEA	Perrier	Dominique	Medium
Time taken: 0.235 seconds, Fetched: 10 row(s)

# creating backup table for sales_order_csv:

hive> create table sales_order_csv_backup as select * from sales_order_csv; 
Query ID = cloudera_20220911123737_d815b2d7-2f96-4fd0-9333-54aa1a56b1e2
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1662920734665_0001, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662920734665_0001/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662920734665_0001
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-09-11 12:38:06,762 Stage-1 map = 0%,  reduce = 0%
2022-09-11 12:38:23,903 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 9.32 sec
MapReduce Total cumulative CPU time: 9 seconds 320 msec
Ended Job = job_1662920734665_0001
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/.hive-staging_hive_2022-09-11_12-37-32_499_4043087289119357259-1/-ext-10001
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_order_csv_backup
Table default.sales_order_csv_backup stats: [numFiles=1, numRows=2823, totalSize=360291, rawDataSize=357468]
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 9.32 sec   HDFS Read: 366330 HDFS Write: 360383 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 320 msec
OK
sales_order_csv.ordernumber	sales_order_csv.quantityordered	sales_order_csv.priceeach	sales_order_csv.orderlinenumber	sales_order_csv.sales	sales_order_csv.status	sales_order_csv.qtr_id	sales_order_csv.month_id	sales_order_csv.year_id	sales_order_csv.productline	sales_order_csv.msrp	sales_order_csv.productcode	sales_order_csv.phonesales_order_csv.city	sales_order_csv.state	sales_order_csv.postalcode	sales_order_csv.country	sales_order_csv.territory	sales_order_csv.contactlastname	sales_order_csv.contactfirstname	sales_order_csv.dealsize
Time taken: 54.803 seconds
hive> show tables;
OK
tab_name
sales_order_csv
sales_order_csv_backup
Time taken: 0.081 seconds, Fetched: 2 row(s)

# CREATING TABLE SALES_ORDER_ORC:
hive> create table sales_order_orc
    > (
    > ordernumber int,
    > quantityordered int,
    > priceeach float,
    > orderlinenumber int,
    > sales float,
    > status string,
    > qtr_id int,
    > month_id int,
    > year_id int,
    > productline string,
    > msrp int,
    > productcode string,
    > phone string,
    > city string,
    > state string,
    > postalcode string,
    > country string,
    > territory string,
    > contactlastname string,
    > contactfirstname string,
    > dealsize string
    > )
    > stored as orc;
OK
Time taken: 0.344 seconds
hive> describe formatted sales_order_orc;
OK
col_name	data_type	comment
# col_name            	data_type           	comment             
	 	 
ordernumber         	int                 	                    
quantityordered     	int                 	                    
priceeach           	float               	                    
orderlinenumber     	int                 	                    
sales               	float               	                    
status              	string              	                    
qtr_id              	int                 	                    
month_id            	int                 	                    
year_id             	int                 	                    
productline         	string              	                    
msrp                	int                 	                    
productcode         	string              	                    
phone               	string              	                    
city                	string              	                    
state               	string              	                    
postalcode          	string              	                    
country             	string              	                    
territory           	string              	                    
contactlastname     	string              	                    
contactfirstname    	string              	                    
dealsize            	string              	                    
	 	 
# Detailed Table Information	 	 
Database:           	default             	 
Owner:              	cloudera            	 
CreateTime:         	Sun Sep 11 12:44:48 PDT 2022	 
LastAccessTime:     	UNKNOWN             	 
Protect Mode:       	None                	 
Retention:          	0                   	 
Location:           	hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_order_orc	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	transient_lastDdlTime	1662925488          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.ql.io.orc.OrcSerde	 
InputFormat:        	org.apache.hadoop.hive.ql.io.orc.OrcInputFormat	 
OutputFormat:       	org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	serialization.format	1                   
Time taken: 0.378 seconds, Fetched: 46 row(s)
hive> show tables;
OK
tab_name
sales_order_csv
sales_order_csv_backup
sales_order_orc
Time taken: 0.068 seconds, Fetched: 3 row(s)
      	                    




# LOADING DATA FROM CSV TABLE TO ORC TABLE:

hive> from sales_order_csv insert overwrite table sales_order_orc select *;
Query ID = cloudera_20220911130202_8bb3c02c-9ca6-4999-863d-a72e3582e71c
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1662920734665_0002, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1662920734665_0002/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1662920734665_0002
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-09-11 13:02:31,630 Stage-1 map = 0%,  reduce = 0%
2022-09-11 13:02:55,900 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 11.92 sec
MapReduce Total cumulative CPU time: 11 seconds 920 msec
Ended Job = job_1662920734665_0002
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/hive_database_ass1.db/sales_order_orc/.hive-staging_hive_2022-09-11_13-02-03_181_1170364234156859823-1/-ext-10000
Loading data to table hive_database_ass1.sales_order_orc
Table hive_database_ass1.sales_order_orc stats: [numFiles=1, numRows=2823, totalSize=37548, rawDataSize=3153291]
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 11.92 sec   HDFS Read: 367490 HDFS Write: 37645 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 920 msec
OK
sales_order_csv.ordernumber	sales_order_csv.quantityordered	sales_order_csv.priceeach	sales_order_csv.orderlinenumber	sales_order_csv.sales	sales_order_csv.status	sales_order_csv.qtr_id	sales_order_csv.month_id	sales_order_csv.year_id	sales_order_csv.productline	sales_order_csv.msrp	sales_order_csv.productcode	sales_order_csv.phonesales_order_csv.city	sales_order_csv.state	sales_order_csv.postalcode	sales_order_csv.country	sales_order_csv.territory	sales_order_csv.contactlastname	sales_order_csv.contactfirstname	sales_order_csv.dealsize
Time taken: 55.563 seconds

hive> select * from sales_order_orc limit 10;
OK
sales_order_orc.ordernumber	sales_order_orc.quantityordered	sales_order_orc.priceeach	sales_order_orc.orderlinenumber	sales_order_orc.sales	sales_order_orc.status	sales_order_orc.qtr_id	sales_order_orc.month_id	sales_order_orc.year_id	sales_order_orc.productline	sales_order_orc.msrp	sales_order_orc.productcode	sales_order_orc.phonesales_order_orc.city	sales_order_orc.state	sales_order_orc.postalcode	sales_order_orc.country	sales_order_orc.territory	sales_order_orc.contactlastname	sales_order_orc.contactfirstname	sales_order_orc.dealsize
10107	30	95.7	2	2871.0	Shipped	1	2	2003	Motorcycles	95	S10_1678	2125557818	NYC	NY	10022	USA	NA	Yu	Kwai	Small
10121	34	81.35	5	2765.9	Shipped	2	5	2003	Motorcycles	95	S10_1678	26.47.1555	Reims		51100	France	EMEA	Henriot	Paul	Small
10134	41	94.74	2	3884.34	Shipped	3	7	2003	Motorcycles	95	S10_1678	+33 1 46 62 7555	Paris		75508	France	EMEA	Da Cunha	Daniel	Medium
10145	45	83.26	6	3746.7	Shipped	3	8	2003	Motorcycles	95	S10_1678	6265557265	Pasadena	CA	90003	USA	NA	Young	Julie	Medium
10159	49	100.0	14	5205.27	Shipped	4	10	2003	Motorcycles	95	S10_1678	6505551386	San Francisco	CA		USA	NA	Brown	Julie	Medium
10168	36	96.66	1	3479.76	Shipped	4	10	2003	Motorcycles	95	S10_1678	6505556809	Burlingame	CA	94217	USA	NA	Hirano	Juri	Medium
10180	29	86.13	9	2497.77	Shipped	4	11	2003	Motorcycles	95	S10_1678	20.16.1555	Lille		59000	France	EMEA	Rance	Martine	Small
10188	48	100.0	1	5512.32	Shipped	4	11	2003	Motorcycles	95	S10_1678	+47 2267 3215Bergen		N 5804	Norway	EMEA	Oeztan	Veysel	Medium
10201	22	98.57	2	2168.54	Shipped	4	12	2003	Motorcycles	95	S10_1678	6505555787	San Francisco	CA		USA	NA	Murphy	Julie	Small
10211	41	100.0	14	4708.44	Shipped	1	1	2004	Motorcycles	95	S10_1678	(1) 47.55.6555	Paris		75016	France	EMEA	Perrier	Dominique	Medium
Time taken: 0.312 seconds, Fetched: 10 row(s)


# Queries:
# a. Calculatye total sales per year:
    select sum(sales) from sales_order_orc group by year_id;

# b. Find a product for which maximum orders were placed:
   select productline, sum(quantityordered) as quantityordered from sales_order_orc group by productline order by quantityordered desc limit 1;
   product with maximum sales: classic cars  33992

# c. Calculate the total sales for each quarter:
   select qtr_id, sum(sales) as total_sales_per_quarter from sales_order_orc group by qtr_id;

# d. In which quarter sales was Minimum
   select qtr_id, sum(sales) as total_sales from sales_order_orc group by qtr_id order by total_sales limit 1;
    qtr with minimun sales: 3


# e. In which country sales was maximum and in which country sales was minimum
   select country, sum(sales) as sales from sales_order_orc group by country order by sales limit 1;
   minimum: Ireland  57756.43029785156

   select country, sum(sales) as sales from sales_order_orc group by country order by sales desc limit 1;
   maximum: USA  3627982.825744629


# f. Calculate quartelry sales for each city
  select sum(sales), city, qtr_id from sales_order_orc group by city, qtr_id;


# h. Find a month for each year in which maximum number of quantities were sold
      with cte as (
      select row_number() over(
      partition by year_id 
      order by sum(quantityordered) desc
      ) row_num, sum(quantityordered) as total_sales, year_id, month_id 
      from sales_order_orc group by year_id, month_id
    )select year_id, month_id, total_sales from cte where row_num=1;

2003    11      10179
2004    11      10678
2005    5       4357
Time taken: 144.301 seconds, Fetched: 3 row(s)
