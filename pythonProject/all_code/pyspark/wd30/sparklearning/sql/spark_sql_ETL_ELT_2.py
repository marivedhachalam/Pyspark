#Functions library
#/usr/local/spark/python/pyspark/sql
#readwriter.py - EL Functions (eg. read.csv, write.json)
#dataframe.py - Direct DF functions (eg. df.createOrReplaceTempView, alias for renaming dataframe)
#functions.py - Common SQL functions (cast)
#column.py - Column SQL functions (alias for renaming columname)

#spark_sql_ETL_2.py start (Bread & Butter2)
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)
#above and beyond
#6. how to standardize the code or how create generic/reusable frameworks (level 3) - masking engine, reusable transformation, data movement automation engine, quality suite, audit engine, data observability, data modernization...

#1. SDLC life cycle (Agile/Scrum/RAD/V/WF) -> requirement -> dev -> peer code review -> unit testing -> shift code to UAT/SIT-> Deployed
# -> Execution (Integration testing) -> signoff -> Productionise
#2. To Productionise: Dev -> packaged ->shipped (DEV) -> Deployed/Submitted -> Tested -> Package -> shift (github->jenkins) -> Deploy in Prod (cloud) -> Orchestration & Scheduling -> Monitoring and log analysis (PS team)

#lets take our scenario (Roles and Responsibilities of a DE)
#1. We completed creating a spark application to do some ETL/ELT operation in the Pycharm/Intellij/Eclipse IDE running in Windows/Mac - Yes
#2. From Windows PC -> move the code to Dev Cluster using a tool called WinScp
#3. Submit the code using spark-submit in Dev Cluster to test the job submission in local mode,
# but the code got failed because of the dependent modules are not provided
# What are the dependencies this spark_sql_ETL_ELT_2.py has -
# dependency0 - source data dependency - using the arguments using sys.argv
# dependency1 - from sparkapp.pyspark.utils.common_functions import cls_common_udf - using shutil we build the zip file
# dependency2 (already admin will takecare) - connecting with hive - hive-site.xml kept in the /usr/local/spark/conf/ and mysql connector jar kept in /jars path
# dependency3 - connecting with external db (MYSQL/Oracle/TD/Reshift) - needed driver/connector jar file which should be provided using any one of these 3 options
# option1 (priority1): mention in the spark session as a config("spark.jars","/home/hduser/install/mysql-connector-java3.jar")
# option2 (priority2): keep this jar in some other location and pass it as an arg using --jars mysql-connector-java2.jar
# option3 (priority3): keep in /usr/local/spark/jars/mysql-connector-java1.jar
# dependency4 - connection properties driver, connection url, username, password (hardcoded for now)
# hence I am going to provide the dependent programs in the form of archive because it should occupy less space when
# shipped to all executors at the time of job submission
# shutil.make_archive("/home/hduser/common_functions","zip", root_dir="/home/hduser/PycharmProjects/IzProject/")


##### LIFE CYCLE OF ETL and Data Engineering PIPELINEs
# VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW , WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
'''
Starting point -(Data Governance (security) - Tagging, categorization, classification, masking)
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for further Transformation/Enrichment, Egress/Outbound, analytics & Reporting
a. Data Discovery (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Combining Data + Schema Evolution/Merging (Structuring)
c. Validation, Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format (Dataengineers/consumers)

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration

3. Data Customization & Processing - Application of Tailored Business specific Rules
a. User Defined Functions
b. Building of Frameworks & Reusable Functions

4. Data Curation
a. Curation/Transformation
b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization

5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
a. Lookup/Reference
b. Enrichment
c. Joins
d. Sorting
e. Windowing, Statistical & Analytical processing

6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
a. Discovery,
b. Outbound/Egress,
c. Reports/exports
d. Schema migration
'''

#EXTRACTION PART#
print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Data Structurizing - Combining Data + Schema Evolution/Merging (Structuring)
c. Validation, 
Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation, Validation, 
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization - Column name/type/order/number of columns changes/Replacement & Deletion of columns to make it in a usable format
""")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# define spark configuration object
spark = SparkSession.builder\
   .appName("Very Important SQL End to End App") \
   .config("spark.eventLog.enabled", "true") \
   .config("spark.eventLog.dir", "file:///tmp/spark-events") \
   .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
   .config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar") \
   .enableHiveSupport()\
   .getOrCreate()

#.config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar")\
# .config("spark.jars", jdbc_lib)\
#spark.conf.set("hive.metastore.uris","thrift://127.0.0.1:9083")
#.config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
# to connect with Remote metastore, but we can't do it in Organization when we develop the pyspark app using Pycharm running in Windows

# Set the logger level to error
spark.sparkContext.setLogLevel("ERROR")

print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
#I will first take some sample data or actual data and analyse about the columns, datatype, values, nulls, duplicates(low/high cardinality), format
#statistical analysis - min/max/difference/mean(mid)/counts

#mode- permissive (default)- permit all the data including the unclean data
#mode- failfast - as soon as you see some unwanted data, fail our program
#mode- dropmalformed - when there are unclean data (doesn't fit with the structure (customschema)/columns are lesser than the defined(custom schema)/identified(inferschema)) don't consider them
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive')
custdf1.printSchema()
custdf1.show(20,False)

#don't use count rather use len(collect)
print(custdf1.count())
print(len(custdf1.collect()))

custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',inferSchema=True)
custdf1.printSchema()
#Significant attribute is custid (_c0)
#Data in the first column has string, but it has to be integer, ensure that with he below function
custdf1.where("upper(_c0)<>lower(_c0)").count()
custdf1.show(20,False)
#null check
custdf1.where("_c0 is null").count()
#Data in the first column has null, may create challenges in the further data processing
#duplicate check
custdf1.select("_c0").distinct().count()
#Data in the first column has duplicate

#Let us stop processing this data, since it is unclean
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='failfast',inferSchema=True)
custdf1.printSchema()
custdf1.show(20,False)
#inferschema required 5 columns, but only 4 columns are found, so program is failing
#4000006,Patrick,Song,24
#trailer_data:end of file


custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True),
                              StructField("corrupt_data",StringType(),True)])#workaround1 - adding the corrupt_data derived column

custdf3=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',schema=custstructtype1,columnNameOfCorruptRecord="corrupt_data")
custdf3.printSchema()
custdf3.show(20,False)
custdf3.cache()#workaround2
corruptdt_to_reject_df4=custdf3.select("corrupt_data").where("corrupt_data is not null")
corruptdt_to_reject_df4.write.mode("overwrite").csv("file:///home/hduser/corruptdata")

#TYPE MISMATCH, MISSING COLUMNS - custom schema requires id column should be integer, but it is string and also it requires 5 columns, but only 4 columns are found, so program is failing
#ten,Elsie,Hamilton,43,Pilot
#4000006,Patrick,Song,24
#trailer_data:end of file

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

#Let us clean and get the right data for further consideration
#drop the unwanted/culprit data while creating the df
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()
custdf_clean.show(20,False)

#How to filter out the corrupted/malformed data alone, to send it to the source system (custom way)
custdf1.select("_c0").subtract(custdf_clean.select(col("id").cast("string"))).show(10,False)

#statistical analysis (Clean data)
custdf_clean.describe().show(20,False)
custdf_clean.summary().show(20,False)

#I realized null values in the key column, wanted to apply the contraint to fail my program incase if the structure is not fulfilled
custdf_clean.rdd.toDF(custstructtype1).show()
custdf_clean.unionByName()
#Exploring of data CONCLUSION - identifying the data quality - failfast(SHOW STOPPER)/permissive(VERACITY)/dropmalformed(CLEANUP), nulls, dups, type mismatch, missing of columns, statistics, rejection of corrupted data

print("b. Combining Data + Schema Evolution/Merging (Structuring)")
print("b.1. Combining Data - Reading from a path contains multiple pattern of files")
'''
mkdir ~/sparkdata/src1
mkdir ~/sparkdata/src1/src2
mkdir ~/sparkdata/src3
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse1.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse2.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse3.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/src2/bse1.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/src2/bse2.csv
'''
spark.read.csv("file:///home/hduser/sparkdata/src1/").count()#all the files in the base path
spark.read.csv("file:///home/hduser/sparkdata/src1/nyse*").count()#all the files in the base+first level of subdir also
spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True,sep='~').count() #all files in all sub directories
print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
#subdirectories contains multiple pattern of files nyse, bse, nse...
#cp /home/hduser/sparkdata/src1/nyse1.csv src2/
spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True,sep='~',pathGlobFilter="nyse[1-2].csv").count()
#The above function will recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

#main main directories and subdirs contains multiple pattern of files nyse, bse, nse...
spark.read.csv(path=["file:///home/hduser/sparkdata/src1/","file:///home/hduser/sparkdata/src3/"],recursiveFileLookup=True,sep='~',pathGlobFilter="nyse[1-2].csv").count()
#The above function will search in the main and recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

print("b.3. Schema Merging (Structuring) - Schema Merging data with different structures (we know the structure of both datasets)")
'''

cat ~/sparkdata/stockdata2/bse2.csv
stock~value~maxrate~minrate~dur_mth~incr
TIT~450.3~710~400~6~-10
SUN~754.62~900~500~6~12.0
TMB~1000.2~1210.50~700~6~100.0

cat /home/hduser/sparkdata/stockdata/bse2.csv
stock~value~incr~maxrate~cat
HUL~450.3~-10.0~710~retail
RAN~754.62~12.0~900~pharma
CIP~1000.2~100.0~1210.50~pharma
'''

#Merging data with different structure

#Below methodology leads to a wrong result
df1=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/","file:///home/hduser/sparkdata/stockdata2/"],sep='~',header=True)
df1.show()

#alternative right approach is using unionByName function to achive the above schema merging feature
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True,header=True)
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)
merged_stock_df=stock_df.unionByName(stock1_df,allowMissingColumns=True)
merged_stock_df.printSchema()
merged_stock_df.show()

#what if if any one of the data has different data type (value is string), but the same column name
'''
cat /home/hduser/sparkdata/stockdata/bse2.csv
stock~value~incr~maxrate~cat
HUL~450.3~neg~710~retail
RAN~754.62~pos~900~pharma
CIP~1000.2~pos~1210.50~pharma
'''

stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True,header=True)
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)
merged_stock_df=stock_df.unionByName(stock1_df,allowMissingColumns=True)
merged_stock_df.printSchema()
merged_stock_df.show()


'''
BSE~HUL~450.3~-10.0
BSE~CIP~754.62~12.0
BSE~TIT~1000.2~100.0

NYSE~CLI~35.3~1.1~EST
NYSE~CVH~24.62~2~EST
NYSE~CVL~30.2~11~EST
'''
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr")
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata1/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr","tz")
merge_df=stock_df.withColumn("tz",lit("")).union(stock1_df)#this is a workaround to convert both DFs with same number of columns

stock_df.unionByName(stock1_df,allowMissingColumns=True)#modern and auto way of merging the data

#Merging data with different structure from difference souces
#create table stock(excname varchar(100),stockname varchar(100),stocktype varchar(100));
#insert into stock values('NYSE','INT','IT');
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table='stock',properties={'driver':'com.mysql.jdbc.Driver'})
merge_df_structured=dfdb1.unionByName(stock1_df,allowMissingColumns=True)#structurizing

#Can we merge data between different sources
stock1_df.write.json("file:///home/hduser/sparkdata/stockdatajson2/")
stock1_json_df=spark.read.json(["file:///home/hduser/sparkdata/stockdatajson2/"])
merged_stock_df=stock_df.unionByName(stock1_json_df,allowMissingColumns=True)
merged_stock_df.printSchema()

print("b.3. Schema Evolution (Structuring) - source data is evolving with different structure")
#If we receive data from different source or in different time from different sources/same sources of different number of columns each time,
#we can perform schema migration from rawdf to parquetdf/orcdf in application1 and schema evolution in application2
'''
/home/hduser/sparkdata/stockdata
ls -lrt
-rw-rw-r--. 1 hduser hduser 110 Jul 12 07:34 bse1.csv
-rw-rw-r--. 1 hduser hduser 101 Jul 12 08:15 bse2.csv
cat bse1.csv
stock~value~incr~maxrate~cat
HUL~450.3~neg~710~retail
RAN~754.62~pos~900~pharma
CIP~1000.2~pos~1210.50~pharma

cat bse2.csv 
stock~value~incr~maxrate~ipo
HUL~450.3~neg~710~300
RAN~754.62~pos~900~500
CIP~1000.2~pos~1210.50~200
'''
#SCHEMA MIGRATION - write a spark application1 that run once in an hour to schema migrate the hourly data from csv to parquet/orc
stock_hr1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',inferSchema=True,header=True)#hour1 data schema migration
stock_hr1_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='overwrite')

stock_hr2_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse2.csv"],sep='~',inferSchema=True,header=True)#hour2 data schema migration
stock_hr2_df=stock_hr2_df.withColumn("incr",col("incr").cast("string"))#workaround for the datatype mismatch issue discussed below
#direct solution is use unionByName option
stock_hr2_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='append')

stock_hr1_df.write.orc("file:///home/hduser/sparkdata/stockorc/")
stock_hr2_df.write.orc("file:///home/hduser/sparkdata/stockorc/",mode='append')


#SCHEMA EVOLUTION - write another spark application2 that run once in a day to schema evolution by reading the parquet/orc data
evolved_stock_hr6_df=spark.read.parquet("file:///home/hduser/sparkdata/stockpar/",mergeSchema=True)
spark.read.orc("file:///home/hduser/sparkdata/stockorc/",mergeSchema=True).show()

#Some limitations:
#1. Column name/order/number of columns can be different, but type has to be same otherwise if type is different mergeschema will fail
#Failed to merge fields 'incr' and 'incr'. Failed to merge incompatible data types string and int

#to overcome the above challenge as a workaround
stock_hr1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',header=True)#hour1 data schema migration
stock_hr1_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='overwrite')
stock_hr2_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse2.csv"],sep='~',header=True)#hour2 data schema migration
stock_hr2_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='append')
merge_df_parquet=spark.read.parquet("file:///home/hduser/sparkdata/stockpar/",mergeSchema=True)

stoctstruct = StructType([StructField("stock", StringType(), False),
                              StructField("value", StringType(), False),
                              StructField("incr", StringType(), True),
                              StructField("maxrate", StringType(), True),
                              StructField("cat", StringType(), True),
                              StructField("ipo", StringType(), True)])

merge_df_parquet_structured=spark.createDataFrame(merge_df_parquet.rdd,stoctstruct)


print("c.1. Validation (active)- DeDuplication")

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

#Let us clean and get the right data for further consideration
#drop the unwanted/culprit data while creating the df
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()
custdf_clean.show(20,False)

#Record level de-duplication - retain only one record and eliminate all duplicate records
dedup_df=custdf_clean.distinct()#or dropDuplicates()
dedup_df.where("id=4000001").show(20,False)
dedup_dropduplicates_df=custdf_clean.dropDuplicates()
dedup_dropduplicates_df.where("id=4000001").show(20,False)

#column's level de-duplication - retain only one record and eliminate all duplicate records with custid is duplicated
#dedup_df=custdf_clean.distinct()#or dropDuplicates()
#dedup_df=custdf_clean.dropDuplicates(subset=["id"])#or dropDuplicates()
dedup_dropduplicates_df.dropDuplicates(subset=["id"]).where("id=4000003").show()

#want to retain a particular dup data
dedup_dropduplicates_df=custdf_clean.sort("custage",ascending=False).dropDuplicates(subset=["id"])
dedup_dropduplicates_df.where("id=4000003").show()

#can you write a spark sql to achive the same functionality? let us know writing DSL or SQL is good here?
#DSL wins

#custdf_clean.createGlobalTempView("gview1")#global temp view has the scope across the spark session within the application
#spark.stop()
#spark=SparkSession.builder.getOrCreate()

#Writing the above deduplication using SQL
custdf_clean.createOrReplaceTempView("custview1")
spark.sql("""select distinct * from custview1""").where("id in (4000001,4000003)").show(20,False)
spark.sql("""select id,custfname,custlname,custage,custprofession from custview1 
            where id in (4000001,4000003) 
group by id,custfname,custlname,custage,custprofession""").show(20,False)

#column level dedup using sql (analytical & windowing functions) - very very important for interview
spark.sql("""select id,custfname,custlname,custage,custprofession from 
                          (select id,custfname,custlname,custage,custprofession,
                          row_number() over(partition by id order by custage desc) rownum 
                          from custview1 
                          where id in (4000001,4000002,4000003))tmp 
            where rownum =1 """).show(20,False)

dedup_dropduplicates_df_sql=spark.sql("""select id,custfname,custlname,custage,custprofession from 
                                                   (select id,custfname,custlname,custage,custprofession,
                                                    row_number() over(partition by id order by custage desc) rownum 
                                                    from custview1)tmp 
                                     where rownum =1 """)

print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
print("Data Cleansing (removal of unwanted/dorment (data of no use) datasets eg. na.drop, drop malformed)")
print("Dropping the malformed records with all of the columns is null (null will be shown when "
      "datatype mismatch, format mismatch, column missing, blankspace in the data itself or null in the data itself)")

print("Dropping the null records with all of the columns are null ")
print(dedup_dropduplicates_df.na.drop("all").count())
print("Dropping the null records with any one of the column is null ")
print(dedup_dropduplicates_df.na.drop("any").count())
print("Dropping the null records with custid and custprofession is null")
print(dedup_dropduplicates_df.na.drop("all",subset=["id","custprofession"]).count())
print("Dropping the null records with custid or custprofession is null ")
dedup_dropduplicates_df.na.drop("any",subset=["id","custprofession"]).count()

dedup_dropna_clensed_df=dedup_dropduplicates_df.na.drop("any",subset=["id"])

print("set threshold as a second argument if non NULL values of particular row is less than thresh value then drop that row")
dedup_dropduplicates_df.na.drop("any",subset=["id","custprofession"],thresh=2).count()

#understanding thresh
'''
NYSE~~
NYSE~CVH~
NYSE~CVL~30.2
~~~
'''
dft1=spark.read.csv("file:///home/hduser/sparkdata/nyse_thresh.csv",sep='~').toDF("exch","stock","value")
dft1.na.drop("any",thresh=1).show()#drop any row that contains upto zero (<1) not null columns
# (retain all the rows that contains any minimum one not null column)
dft1.na.drop("any",thresh=2).show()#drop any row that contains upto one (<2) not null columns or retain rows with minimum 2 columns with not null
dft1.na.drop("any",thresh=3).show()#drop any row that contains upto two (<3) not null columns or retain rows with minimum 3 columns with not null
dft1.na.drop("any",thresh=4).show()#drop any row that contains upto three (<4) not null columns or retain rows with minimum 4 columns with not null

#na.drop - Parameters:
#how: This parameter is used to determine if the row or column has to remove or not.
#‘any’ – If any of the column value in Dataframe is NULL then drop that row.
#‘all’ – If all the values of particular row or columns is NULL then drop.
#thresh: If non NULL values of particular row or column is less than thresh value then drop that row or column.
#subset: If the given subset column contains any of the null value then drop that row or column.

print("Scrubbing (convert of raw to tidy na.fill or na.replace)")
# Writing DSL
print("If need to write the na.drop functionalities in SQL, is it easy or hard?        ")
print("fill the null with default values ")
dedup_dropna_clensed_df.where("custprofession is null").count()
dedup_dropna_clensed_df.where("custlname is null").show()
dedup_dropfillna_clensed_scrubbed_df1=dedup_dropna_clensed_df.na.fill("na",subset=["custlname","custprofession"])
dedup_dropfillna_clensed_scrubbed_df1.show()
dedup_dropfillna_clensed_scrubbed_df1.where("custprofession = 'na'").show()

print("Replace (na.replace) the key with the respective values in the columns "
       "(another way of writing Case statement)")
prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"prof not defined"}
dedup_dropfillreplacena_clensed_scrubbed_df1=dedup_dropfillna_clensed_scrubbed_df1.na.replace(prof_dict,subset=["custprofession"])
dedup_dropfillreplacena_clensed_scrubbed_df1.show()
#Writing equivalent SQL - for learning the above functionality more relatively & easily and to understand which way is more feasible
dedup_dropduplicates_df_sql.createOrReplaceTempView("dropduplicates_df_sql_view")
dedup_dropfillna_clensed_scrubbed_df1_sql=spark.sql("""select id,custfname,custlname,custage,
                                                        case when custprofession='Therapist' then 'Physician' 
                                                        when custprofession='Musician' then 'Music Director'
                                                        when custprofession='na' then 'prof not defined' 
                                                        else custprofession end as custprofession
                                                        from (select id,custfname,coalesce(custlname,'na') custlname,custage,coalesce(custprofession,'na') custprofession 
                                                     from dropduplicates_df_sql_view 
                                                     where id is not null)temp""")#.where("custprofession='prof not defined'")


print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
#select can be used in the starting stage to
#re order or choose the columns or remove or replace using select or add
#re ordering of columns
#DSL Functions to achive reorder/add/removing/replacing respectively select,select/withColumn,select/drop,select/withColumn
#select - reorder/add/removing/replacement/rename
#withColumn - add
#withColumn - Replacement of columns custlname with custfname
#withColumnRenamed - rename of a given column with some other column name
#drop - remove columns

#select - reorder/add/removing/replacement/rename
reord_add_rem_repl_ren_df1=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession",col("custage").alias("age"),col("custlname").alias("custfname"),lit("retailsystem").alias("sourcesystem"))
reord_df2=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage","custlname","custfname")#re-order select is the best option
reord_df2.show(10,False)
#apply some transformations using all the above columns .....

#select is not supposed to be used, rather use withColumn, drop, withColumnRenamed
# number of columns changes
# adding of columns (we don't do here, rather in the enrichment we do)
srcsys='Retail'
reord_added_df3=reord_df2.withColumn("srcsystem",lit(srcsys))

#replacement of column(s)
reord_added_replaced_df4=reord_added_df3.withColumn("custfname",col("custlname"))#preffered way if few columns requires drop
#we can do rename/duplicating column/derivation of a column also using withColumn, we will see further down
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",col("custlname").alias("custfname"),"srcsystem")
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",upper("custlname").alias("custfname"),"srcsystem")

# removal of columns
chgnumcol_reord_df5=reord_added_replaced_df4.drop("custlname")#preffered way if few columns requires drop
chgnumcol_reord_df6_1=reord_added_replaced_df4.select("id","custprofession","custage","custfname","srcsystem")

#achive replacement and removal using withColumnRenamed
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumnRenamed("custage","age").show()#preffered way if few columns requires drop
#above function withColumnRenamed("custlname","custfname") did 2 things, 1 - replacement of custfname with custlname and 2 - dropped custlname because
#both the columns exists in the df
#above function withColumnRenamed("custage","age") did 2 things, 1 - created a new column called age and 2 - dropped custage because only custage exists in the df
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumn("age",col("custage")).drop("custage").show()#equivalent to the above function
#columns will be reordered

#equivalent SQL
dedup_dropfillna_clensed_scrubbed_df1_sql.createOrReplaceTempView("dedup_dropfillna_clensed_scrubbed_view")
chgnumcol_reord_df6_1_sql=spark.sql("""select id,custprofession,custage,custlname as custfname,'Retail' as srcsystem 
from dedup_dropfillna_clensed_scrubbed_view""")#found to be ease of use

#conclusion of functions used:
#yet to add

print("********************data munging completed****************")

#TRANSFORMATION PART#
###########Data processing or Curation or Transformation Starts here###########
print("*************** Data Enrichment (values)-> Add, Rename, combine(Concat), Split, Casting of Fields, Reformat, "
      "replacement of (values in the columns) - Makes your data rich and detailed *********************")
munged_df=chgnumcol_reord_df5
#select,drop,withColumn,withColumnRenamed
#Adding of columns (withColumn/select) - for enriching the data
enrich_addcols_df6=munged_df.withColumn("curdt",current_date()).withColumn("loadts",current_timestamp())#both are feasible
#or
enrich_addcols_df6_1=munged_df.select("*",current_date().alias("curdt"),current_timestamp().alias("loadts"))#both are feasible

#Surrogate key - is a seqnumber column i can add on the given data set if the dataset doesn't contains natural key or
# if i want add one more local surrogate key for better processing of data
#munged_df.orderBy("custage",ascending=True).withColumn("skey",monotonically_increasing_id()).show()

#Rename of columns (withColumnRenamed/select/withColumn & drop) - for enriching the data
enrich_ren_df7=enrich_addcols_df6.withColumnRenamed("srcsystem","src")#preferrable way (delete srcsystem and create new column src without changing the order)
enrich_ren_df7_1=enrich_addcols_df6_1.select("id","custprofession","custage","custfname",col("srcsystem").alias("src"),"curdt","loadts")#not much preferred
enrich_ren_df7_2=enrich_addcols_df6_1.withColumn("src",col("srcsystem")).drop("srcsystem")#costly effort

#Concat to combine/merge/melting the columns
enrich_combine_df8=enrich_ren_df7.select("id","custfname","custprofession",concat("custfname",lit(" is a "),"custprofession").alias("nameprof"),"custage","src","curdt","loadts")
#try with withColumn (that add the derived/combined column in the last)
enrich_combine_df8=enrich_ren_df7.withColumn("nameprof",concat("custfname",lit(" is a "),"custprofession")).drop("custfname")

#Splitting of Columns to derive custfname
enrich_combine_split_df9=enrich_combine_df8.withColumn("custfname",split("nameprof",' ')[0])

#Casting of Fields
enrich_combine_split_cast_df10=enrich_combine_split_df9.withColumn("curdtstr",col("curdt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("yearstr",substring("curdtstr",1,4))

#Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
enrich_combine_split_cast_reformat_df10=enrich_combine_split_df9.withColumn("curdtstr",col("curdt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("curdtstr",concat(substring("curdtstr",3,2),lit("/"),substring("curdtstr",6,2))).withColumn("dtfmt",date_format("curdt",'yyyy/MM/dd hh:mm:ss'))

#try with ansi SQL
#enrich_combine_split_cast_reformat_df10
#SQL equivalent using inline view/from clause subquery
enrich_combine_split_cast_reformat_df10_SQL=spark.sql("""select id,custprofession,custage,src,curdt,loadts,nameprof,split(nameprof,' ')[0] as custfname,
             concat(substr(curdt,3,2),'/',substr(curdt,6,2)) curdtstr,year(curdt) year,
             date_format(curdt,'yyyy/MM/dd hh:mm:ss') dtfmt
                   from(
                        select id,custprofession,custage,srcsystem as src,
                               cast (current_date() as string) curdt,
                               current_timestamp() loadts,
                               concat(custfname,' is a ',custprofession) nameprof                        
                        from mungedview1
                       ) temp""")

enrich_combine_split_cast_reformat_df10_SQL.show(2)

#****************Data Enrichment Completed Here*************#

print("***************3. Data Customization & Processing (Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")
munged_enriched_df=enrich_combine_split_cast_reformat_df10
print("Data Customization can be achived by using UDFs - User Defined Functions")
print("User Defined Functions must be used only if it is Inevitable (un avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions.")
#Interview Question: Hi Irfan, Whether you developed any UDF's in your project?
# Yes, but very minumum numbers we have and I developed 1/2 UDFs - (data masking, filteration (CPNI,GSAM),data profiling/classification, custom business logic like promo ratio calculation, customer intent identification)
# But, we should avoid using UDFs until it is Inevitable
#some realtime examples when we can't avoid using UDFs

#case1: How to avoid using UDFs
#step1: Create a function (with some custom logic) or download a function/library of functions from online writtern in python/java/scala...
convertToUpperPyLamFunc=lambda prof:prof.upper()

#step2: Import the udf from the spark sql functions library
from pyspark.sql.functions import udf

#step3: Convert the above function as a user defined function (which is DSL ready)
convertToUpperDFUDFFunc=udf(convertToUpperPyLamFunc)

#step3: Convert and Register (in Spark Metastore) the above function as a user defined function (which is SQL ready)
spark.udf.register("convertToUpperDFSQLFunc",convertToUpperPyLamFunc)

#step4: Apply the UDF to the given column(s) of the DF using DSL program
customized_munged_enriched_df=munged_enriched_df.withColumn("custprofession",convertToUpperDFUDFFunc("custprofession"))
customized_munged_enriched_df.show(2)

#using builtin(predefined) function (avoid applying the above 4 steps and get the result directly)
predefined_munged_enriched_df=munged_enriched_df.withColumn("custprofession",upper("custprofession"))
predefined_munged_enriched_df.show(2)

#SQL Equivalent
customized_munged_enriched_df.createOrReplaceTempView("view1")
spark.sql("select id,convertToUpperDFSQLFunc(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)
spark.sql("select id,upper(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)

#In the above case, usage of built in function is better - built in is preferred, if not available go with UDF

#Quick Usecase:
#Write a python def function to calculate the age grouping/categorization of the people based on the custage column, if age<13 - childrens, if 13 to 18 - teen, above 18 - adults
#derive a new column called age group in the above dataframe (using DSL)
#Try the same with the SQL also

# Step1: Creating a Python function (custom logic) is not an optimized solution (prefer to use builtin functions)-
#Interview Question: When do you go and create a function explicitly?
#1. If already sql/dsl functionalities are not available or hard to implement
#2. If we are using some generic/common reusable functions (used across the Org) eg. masking, profiling/quality, promo calculation, bonus calculation, gst calculation, tax calcutions
#3. If some of the 3rd party functions we are going to use - eg. NLTK (Natural language toolkit), Standford NLP library
#4. If the function is complex to apply in DSL/SQL and used in multiple places of our programs
#3.
def age_validation(age):
   if age < 13:
      return "Children"
   elif age >=13 and age<=18:
      return "Teen"
   else:
      return "Adults"

#Step2: Importing UDF spark library
from pyspark.sql.functions import udf

#Step3A: Converting the above function using UDF into user-defined function (DSL)
age_custom_validation = udf(age_validation)

#or

#Step3B: Registering the Python function as UDF for spark SQL ready function (SQL)
spark.udf.register("custom_age_validation_sql",age_validation)

#Step4: New column deriviation called age group, in the above dataframe (Using DSL)
custom_agegrp_munged_enriched_df = munged_enriched_df.withColumn("agegroup",age_custom_validation("custage"))
#or
custom_agegrp_munged_enriched_df=munged_enriched_df.withColumn("agegroup",when(col("custage")<13,lit("Children")).when((col("custage")>=13),lit("Teen")).otherwise(lit("Adult")))
#or
#Marry DSL & SQL - Writing SQL expressions in DSL
#without creating a temp view, by using expr/selectExpr, we can use sql on top of df right or sql along with dsl
custom_agegrp_munged_enriched_df=munged_enriched_df.select("*",expr("case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup"),expr("cast(custage as string)"))
#or
custom_agegrp_munged_enriched_df=munged_enriched_df.selectExpr("*","case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup")

#case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup

#Step6: Display the dataframe and Filter the records based on age_group
custom_agegrp_munged_enriched_df.show(5,False)
custom_agegrp_munged_enriched_df.filter("agegroup = 'Adults'").show(5,False)

#SQL Equivalent
# (Not preferred way - because using udf)
enrich_combine_split_cast_reformat_df10_SQL.createOrReplaceTempView("munged_enriched_df_view")
#munged_enriched_df.createOrReplaceTempView("munged_enriched_df_view")
custom_agegrp_munged_enriched_df_SQL=spark.sql("select *,custom_age_validation_sql(custage) agegroup from munged_enriched_df_view")
custom_agegrp_munged_enriched_df_SQL.show(5,False)

# (Preferred way - don't use udf)
custom_agegrp_munged_enriched_df_SQL=spark.sql("""select *,"
                                               case when custage<13 then 'Children' 
                                               when custage>=13 and custage<=18 then 'Teen' 
                                               else 'Adults' end as agegroup 
                                               from munged_enriched_df_view""")
custom_agegrp_munged_enriched_df_SQL.show(5,False)

#MUST GO function - Below utils/functions/modularization/reusable framework are suggested to Standardize the code
def convertFileToDF(sparksess,loc,de,md):
 df=sparksess.read.option("delimiter",de).csv(loc,mode=md)
 return df
df1=convertFileToDF(spark,"file:///home/hduser/hive/data/custsmodified",',','permissive')

#Conclusion of Data Customization:
#1. Avoid usage of udfs if it is inevitable
#2. If udf has to be used in dsl - convert python func into udf
#3. If udf has to be used in sql - convert and register python func into udf
#4. If udf is avoidable, write custom functionality using DSL/SQL/DSL+SQL (using expr/selectExpr)
#5. utils/functions/modularization/reusable framework - Suggested to create Functions and reuse them eg. convertFileToDF

print("***************4. Core Data Processing/Transformation (Level1) (Pre Wrangling) Curation -> "
      "filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytics *********************")
#Transformation Functions -> select, filter, sort, group, aggregation, having, transformation/analytical function, distinct...
pre_wrangled_customized_munged_enriched_df=custom_agegrp_munged_enriched_df.select("id","custprofession","custage","src","curdt")\
    .where("convertToUpperDFSQLFunc(custprofession)='FIREFIGHTER' or custprofession='WRITER'")\
    .groupBy("custprofession")\
    .agg(avg("custage").alias("avgage"))\
    .where("avgage>49")\
    .orderBy("custprofession")

#pre wrangled data to consume in the next wrangling stage
#pre wrangled data to produce it to the consumer last persistant stage

#Filter rows and columns
filtered_adult_row_df_for_consumption1=custom_agegrp_munged_enriched_df.filter("agegroup='Adults'")#Will be used in the Data persistant last stage
filtered_nochildren_rowcol_df_for_further_wrangling1=custom_agegrp_munged_enriched_df.filter("agegroup<>'Children'").select("id","custage","curdt","custfname","year","agegroup")#Will be used in the Data persistant last stage

#Aggregation to derive pre wrangled data to consume in the next wrangling stage
#Aggregation to derive pre wrangled data to produce it to the consumer last persistant stage
#Dimensions & Measures we are going to derive
#Dimension - multiple view or viewing of data in different aspects/factors (agegroup,year,profession wise)
#Measures(number based)/Metrics - It is a factual value/metrics/key performance indicator
from pyspark.sql.functions import *
#Tell me year,agegroup,profession wise max/min/avg/count/distinccount/mean of age and availabledate of the given dataset
#(select,group by, aggregation, where, order by)
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2=custom_agegrp_munged_enriched_df.where("agegroup<>'Children'").groupby("year","agegroup","custprofession").agg(max("curdt").alias("max_curdt"),min("curdt").alias("min_curdt"),avg("custage").alias("avg_custage"),mean("custage").alias("mean_age"),countDistinct("custage").alias("distinct_cnt_age")).orderBy("year","agegroup","custprofession",ascending=[False,True,False])

#Tell me the average age of the above customer is >35 (adding having)
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2=dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2.where("avg_custage>50")#having clause

#Analytical Functionalities
#Data Random Sampling:
randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)
randomsample2_for_consumption4=custom_agegrp_munged_enriched_df.sample(.2,11)#Consumer (Datascientists needed for giving training to the models)
randomsample3_for_consumption5=custom_agegrp_munged_enriched_df.sample(.2,25)#Consumer (Datascientists needed for giving training to the models)

#Summary/Description (for the consumer's purpose)
summary1_for_consumption6=filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.summary()

#Co rrelation
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.corr("avg_custage","mean_age")
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.corr("avg_custage","distinct_cnt_age")

#co variation
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.cov("avg_custage","distinct_cnt_age")
#Frequecy Analysis
df_freq_prof_agegroup_consumption7=filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.freqItems(["custprofession","agegroup"],.4)

#SQL Equivalent of the Same
#spark will convert it...
#keep groupby and agg() columns in the select
#keep the first where clause in the where clause
#keep groupby in the groupby
#keep last where clause in the having clause
#keep orderby in the last
custom_agegrp_munged_enriched_df.createOrReplaceTempView("custom_agegrp_munged_enriched_df_view")

#write order of the sql - select,columns of groupby, columns of aggr, from, where, group by, having, order by
#IMPORTANT for Non SQL people
#execution order of the sql (traditional DB) - from (bring all data (rows/cols) from disk to memory), where,select, group by, having, order by
#execution order of the sql (Spark + parquet/orc) - from (select only specific cols where rows meet the filter condition from disk to memory applying PDO), group by, having, order by
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql1=spark.sql("""select * from (select year, agegroup, custprofession, max(curdt) as max_curdt, min(curdt) as min_curdt, 
avg(custage) as avg_custage, avg(custage) as mean_age, count(distinct custage) as distinct_cnt_age
from custom_agegrp_munged_enriched_df_view
where agegroup <> 'Children'
group by year, agegroup, custprofession
order by year desc, agegroup, custprofession desc)temp 
where avg_custage > 50""")

dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql1.show(2)
#or
custom_agegrp_munged_enriched_df.createOrReplaceTempView("df_view1")
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql2=spark.sql("""select year,agegroup,custprofession,
	count(distinct custage) distinct_cnt_age,
	max(curdt) max_curdt,
	min(curdt) min_curdt,
	avg(custage) avg_custage, 
	avg(custage) mean_age
	from df_view1
where agegroup<>'Children'
group by year,agegroup,custprofession	
having avg(custage)>50
order by year desc,
	agegroup Asc,
	custprofession desc""")

print("***************5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")
#Quick usecase
#Apply struct type for this data?
#Apply some of the Data Transformation steps on this given data as we did for custs file
#this data is also eligible for all above DE stages like munging, enrichment, customization, pre wrangling, wrangling, enablement finally
txnsstructtype2=StructType([StructField("txnid",IntegerType(),False),StructField("dt",StringType(),False)])
txns = spark.read.option("inferschema", True)\
.option("header", False).option("delimiter", ",")\
.csv("file:///home/hduser/hive/data/txns")\
.toDF("txnid", "dt", "custid", "amt", "category", "product", "city", "state", "transtype").na.drop("all")
#this data is also eligible for all above DE stages like munging, enrichment, customization, pre wrangling, wrangling, enablement finally

#On this transaction data you do munging, enrichment (we did some mandatory enrichment (date type)), customization, pre-wrangling, wrangling(we are doing below), publishing/persisting (we are doing below)

#Enrichment stage is must needed-
#Wanted to extract the transaction year from the dt column
txns.withColumn("dt",to_date("dt",'MM-dd-yyyy')).withColumn("yr",year("dt")).show(2)#return null value
#'06-26-2011' -> to_date('06-26-2011','MM-dd-yyyy')-> date('yyyy-MM-dd')->any date functions

print("Conversion & Formatting functions - Date Transformation")
#to_date(dt,format) - Trans date is in a format of MM-dd-yyyy -> Need to reformat to (yyyy-MM-dd) make spark recognize this date for applying date functions
#1. Convert the above format from MM-dd-yyyy to yyyy-MM-dd
#2. Convert the data type from string to date

txns_dtfmt=txns.withColumn("dt",to_date("dt",'MM-dd-yyyy'))
txns_dtfmt.show(2)#dt is converted to yyyy-MM-dd
txns_dtfmt.printSchema()#dt is converted from string to date

#Enrich the df with some date functions
enriched_dt_txns=txns_dtfmt.withColumn("yr",year("dt")).withColumn("mth",month("dt")).withColumn("lastday",last_day("dt")).withColumn("nextsunday",next_day("dt",'Sunday')).withColumn("dayofwk",dayofweek("dt")).withColumn("threedaysadd",date_add("dt",3)).withColumn("datediff",datediff(current_date(),"dt")).withColumn("daytrunc",trunc("dt",'mon'))
enriched_dt_txns.show(2,False)

#Enrichment Stage is completed for transaction data


#Joins (Interview Question)
# I will give you 2 datasets, 1 with 14 rows another with 13 rows and common data is 11, what is the count on different joins
#inner join    11
#left join   14
#right join    13
#cross join   182
#full join    27-11=16 (11 common records + 3 left excess + 2 right excess)

print("Just learn about Joins with examples")
#Connecting the tables using some common fields -> inner join,left join,right join, full outer, natural join, cross join, self join, anti joins, semi joins
#inner join,left join,right join - (natural joinS) - Regularly used joins
#full outer, self join - Rarely used joins
#anti joins, semi joins - Very Rarely used joins
#cross join/cartisian join - Join to be avoided and used only if it is inevitable

cust_df=filtered_nochildren_rowcol_df_for_further_wrangling1
#DSL Join syntax (Hard relative than SQL syntax)
cust_df.join(enriched_dt_txns,on=[col("id")==col("custid")]).show()

dim_df1=cust_df.where("id in (4000001,4000002)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))
dim_df2=cust_df.where("id in (4000001,4000003,4000004)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))

#Syntactically:
#Minimum We have have leftdf.join(rightdt) - Bydefault It will do cross join/cartisian join if no join condition is mentioned(which is not preferred)
dim_df1.join(dim_df2).show()#cross join

#If we mention atleast the one on condition, Bydefault It will do inner join/equi join if atleast one join condition is mentioned(which is preferred)
dim_df1.join(dim_df2,on="id").show()#if common join columns in both dfs
#dim_df1.join(dim_df2,on="id").select("id","custage").show()
#pyspark.sql.utils.AnalysisException: Reference 'custage' is ambiguous, could be: custage, custage.
dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on="id").select("dd1.id","dd2.custage").show()
#or
#Use the below syntax regulary
dim_df1.join(dim_df2,on=[col("id")==col("id")]).show()#if multiple join conditions or no common column names use this syntax
#dim_df1.join(dim_df2,on=[col("id")==col("id"),col("custage")==col("custage")]).show()#if multiple join conditions or no common column names use this syntax
#pyspark.sql.utils.AnalysisException: Reference 'id' is ambiguous, could be: id, id

#Best used syntax is using alias and on condition
joineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id"),col("dd1.custage")==col("dd2.custage")]).select("dd1.*","dd2.*")
joineddf.show()

#SQL Join syntax
dim_df1.createOrReplaceTempView("dim_df1_view")
dim_df2.createOrReplaceTempView("dim_df2_view")
spark.sql("""select dd1.*,dd2.* 
               from dim_df1_view dd1 inner join dim_df2_view dd2 
               on(dd1.id=dd2.id)""").show()

#How to find the common columns in a dataframe
#set(dim_df1.columns).intersection(set(dim_df1.columns))

dim_df1=cust_df.where("id in (4000001,4000002,4000003)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))
dim_df2=cust_df.where("id in (4000001,4000004,4000006,4000009)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))
fact_df2=enriched_dt_txns.where("custid in (4000002,4000003)").select("custid","txnid","amt","city","transtype")

##########Types of Joins################
#Scenario:
#Two stores are sending their customer data, we need to know  the following
#Common customer info - inner
#store1 customer info and store2 matching cust info- left
#store2 customer info and store1 matching cust info - right
#store1 and store2 all customer info - full

#Connecting the tables using some common fields -> inner join,left join,right join, full outer, natural join, cross join, self join, anti joins, semi joins
#inner join,left join,right join - (natural joinS) - Regularly used joins
#full outer, self join - Rarely used joins
#anti joins, semi joins - Very Rarely used joins
#Semi join is good in performance (the join iteration=first occurance(total number of customers ie 10000)), as it checks/lookup only the first occurance of the given customer (left data) in the transaction data (right dataset)

#cross join/cartisian join - Join to be avoided and used only if it is inevitable
#cross join will multiply the data between 2 dfs for eg. 2*2 =4 records will be returned
crossjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2")).select("dd1.*","dd2.*")
crossjoineddf.show()#If we join 2 dfs/tables without using a join condition then by default cross join will be executed

#SQL Join syntax

#Inner join - It is retrieving only the matched records
innerjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")]).select("dd1.*","dd2.*")
innerjoineddf.show()#If we join 2 dfs/tables using a join condition then by default inner join will be executed

#SQL Join syntax

#left join - All records from left and only matching from right then rest of right will be produced as null
# The total records from left df will be returned and matching right df will have values and non matching right df will have nulls
leftjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='left').select("dd1.*","dd2.*")
leftjoineddf.show()

#right join - All records from right and only matching from left then rest of left will be produced as null
# The total records from right df will be returned and matching left df will have values and non matching left df will have nulls
rightjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='right_outer').select("dd1.*","dd2.*")
rightjoineddf.show()

#full join - All records from both dfs will be displayed and only matching from left then rest of left will be produced as null
# The total records from right df will be returned and matching left df will have values and non matching left df will have nulls
rightjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='right_outer').select("dd1.*","dd2.*")
rightjoineddf.show()

#self join - All records from both dfs will be displayed and only matching from left then rest of left will be produced as null
#Self join can be achived by joining the same df/table with some common condition by itself
#Where it will be applied? It can be used for hierarchical retrival of data
selfjoineddf=dim_df1.alias("dd1").join(dim_df1.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='inner').select("dd1.*","dd2.*")
selfjoineddf.show()

#left semi join or semi join - Just to check/lookup the records from first table exist in the second table and display the only first table rows alone
#It extracts only the matched data from only the left table, by comparing with the right table
semijoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='semi').select("dd1.*")
semijoineddf.show()

spark.sql("select * from dim_df1_view where id in (select id from dim_df2_view)").show()

#left anti join or anti join - Just to check/lookup the records from first table not exist in the second table and display the only first table not exist rows alone
#It extracts only the non matched data from only the left table, by comparing with the right table
antijoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='anti').select("dd1.*")
antijoineddf.show()

spark.sql("select * from dim_df1_view where id not in (select id from dim_df2_view)").show()

#Usecase: SQL Equivalent, Provide the number of rows returned if we use different types of joins:

###Learning of Joins in Spark SQL (DSL/SQL) is completed here...

print("****************** Started performing 5. Wrangling from here************")
#5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized)
print("a. Lookup (Joins)")
#how many customers of our customer database visited our webpage today
#lookup is an activity of identifying some existing data with the help of new data
#lookup can be achived using joins (semijoin (preferrably), antijoin(preferable for anti lookup), leftjoin, innerjoin)
cust_3custs=custom_agegrp_munged_enriched_df.where("id in (4000000,4000001,4000002)")
txns_3custs=txns.where("custid in (4000000,4000001)")

#I did a lookup of howmany customer in my customer database have did transaction today
#Returns the details of the customers who did transactions, no transaction details will be provided
semilookup=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="semi")
#Semi join is good in performance (the join iteration=first occurance(total number of customers ie 10000)), as it checks/lookup only the first occurance of the given customer (left data) in the transaction data (right dataset)
#or
leftlookup=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="leftouter").where("t.custid is not null").select("c.*").dropDuplicates()
#or
innerlookup=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="inner").select("c.*").distinct()

#I did a (anti) lookup of howmany customer in my customer database did not do transaction today
#Returns the details of the customers who did not do transactions, no transaction details will be provided
antisemilookup=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="anti")

#Appu's question, why not we can use subquery to achive this
#join or in which is better - join is better because of different types available
spark.sql("select * from customer where id in (select id from txns))")#this will produce the same semi join result
spark.sql("select * from customer where id not in (select distinct id from txns))")#this will produce the same semi join result

print("b. Lookup & Enrichment (Joins)")
#lookup is an activity of enriching the new data with the existing data (leftjoin(preferably), rightouter, innerjoin, fulljoin)
#Transaction data of last 1 month I am considering above
#Returns the details of the customers who did transactions or didn't do the transactions along with what transactions details or null values
leftlookupenrichment=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="leftouter")
leftlookupenrichment.select("c.id","c.custprofession","t.*").show(100)
#or
#Returns the details of the customers who did transactions along with what transactions they did
innerlookupenrichment=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")

#We are using the enriched data to get some insight
semilookup.count()#how many customers did transaction today
antisemilookup.count()#how many customers didn't do transaction today
leftlookupenrichment.groupby("id").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction or didn't do the transaction for how much
innerlookupenrichment.groupby("id").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction for how much

print("Ways of joining tables using or without using alias")
print("case1 - if both df contains common join columns with same name between the dataframes, we can use the below simple methodology")
#we don't use alias
semilookup1=cust_3custs.join(txns_3custs,on="id",how="semi")
print("case2 - if both df contains common join columns and other columns also with same name between the dataframes")
# we are going to change the column names in one df using withColumnRenamed (which is costly to do)
# or we need to use alias to take the respective columns when we select the columns using alias (simple as given below)
semilookup2=cust_3custs.alias("c").join(txns_3custs.alias("t"),on="id",how="semi")
print("case3 (better common approach)- if both df doesn't contains common join columns and other columns also with same name between the dataframes")
# we are going to change the column names in one df using withColumnRenamed (which is costly to do)
# or we need to use alias to take the respective columns when we select the columns (simple as given below)
semilookup=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="semi")#best join syntax to use in DSL

print("Lookup and Enrichment Scenario using SQL")
cust_3custs.createOrReplaceTempView("cust_df_view")
txns_3custs.createOrReplaceTempView("enriched_dt_txns_view")

# Inner join
cust_txn_inner_join = spark.sql("""select cdv.*,edtv.* 
               from cust_df_view cdv inner join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_inner_join.count()  # 95881 records

# left join
cust_txn_left_join = spark.sql("""select cdv.*,edtv.* 
               from cust_df_view cdv left outer join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_left_join.count()  # 95882 records

# right join
cust_txn_right_join = spark.sql("""select cdv.*,edtv.* 
               from cust_df_view cdv right outer join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_right_join.count()  # 95904 records

# full join
cust_txn_full_join = spark.sql("""select cdv.*,edtv.* 
               from cust_df_view cdv full outer join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_full_join.count()  # 95905 records

# semi join
cust_txn_semi_join = spark.sql("""select cdv.*
               from cust_df_view cdv semi join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_semi_join.count()  # 2 records

# Anti Join
cust_txn_anti_join = spark.sql("""select cdv.*
               from cust_df_view cdv anti join enriched_dt_txns_view edtv 
               on(cdv.id=edtv.custid)""")
cust_txn_anti_join.count()  # 1 Record

print("Identify the count of Active Customers or Dormant Customers")
#take latest 1 month worth of txns data and compare with the customer data
txns_1month=txns_dtfmt.select("*",month("dt").alias("mth"),year("dt").alias("yr")).where("mth=12 and yr=2011")

#Active Customers (customers did transactions in last 1 month)
active_customers=custom_agegrp_munged_enriched_df.alias("c").join(txns_1month.alias("t"),on=[col("c.id")==col("t.custid")],how="leftsemi")

#Dormant Customers (customers didn't do transactions in last 1 month)
dormant_customers=custom_agegrp_munged_enriched_df.alias("c").join(txns_1month.alias("t"),on=[col("c.id")==col("t.custid")],how="leftanti")
#DSL
#SQL

print("c. (Star Schema model-Normalized) Join Scenario  to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business")
cust_dim=custom_agegrp_munged_enriched_df
txns_fact=txns_dtfmt

#benefits (flattened/widened/denormalized data)
#Once for all we do costly join and flatten the data and store it in the persistant storage
#Not doing join again and again when consumer does analysis, rather just query the flattened joined data
#drawback(flattened/widened/denormalized data)
#1. duplicate data
#2. occupies storage space

# DSL
flattened_denormalized_trans_cust_persist=cust_dim.alias("c").join(txns_fact.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")

print("Equivalent SQL for Denormalization (denormalization helps to create a single view for faster query execution without joining)")

print("d. Windowing Functionalities")
#clauses/functions - row_number(), rank(), dense_rank() - over(), partition(), orderBy()
txns_dtfmt=txns.withColumn("dt",to_date("dt",'MM-dd-yyyy'))
txns_3custs=txns_dtfmt.where("custid in (4000000,4000001)")

from pyspark.sql.window import Window

print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
txns.repartition(1).withColumn("sno",monotonically_increasing_id()).show(100000)
#montonic id will generate jumping sequence accross the partitions
rno_txns3=txns_3custs.select("*",row_number().over(Window.orderBy("dt")).alias("sno"))
#row_number will coalesce the number of partition to 1 and it will generate sequence in a right sequence without jumping values

print("bb.How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
trans_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy("dt")).alias("transorder"))
print("first, nth made by a given customer")
trans_order_txns3.where("transorder=1").show()
trans_order_txns3.where("transorder=3").show()#3rd transaction customers made
trans_order_txns3.where("transorder<=3").show()#first 3 transactions

print("last, penultimate transaction by a given customer")
trans_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("dt"))).alias("transorder"))
trans_order_txns3.where("transorder=1").show()
trans_order_txns3.where("transorder=2").show()#last but one transaction customers made
trans_order_txns3.where("transorder<=3").show()#last 3 transactions

print("Least 3 transactions in our overall transactions")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.orderBy("amt")).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top 3 transactions in our overall transactions")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.orderBy(desc("amt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top 3 transactions made by the given customer")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top transaction amount made by the given customer 4000000")
transamt_order_rno_rnk_drnk_txns3=txns_3custs.where("custid=4000000").select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtorder"),rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtrnk"),dense_rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtdrnk"))

#transamt_order_rno_rnk_drnk_txns3.where("transamtorder=1").show()#not valid for the given requirement
transamt_order_rno_rnk_drnk_txns3.where("transamtrnk=1").show()#rank

print("Top 2nd transaction amount made by the given customer 4000000")
transamt_order_rno_rnk_drnk_txns3.where("transamtdrnk=2").show()#dense rank

print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of game atleast once")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid","category").orderBy(desc("dt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder=1").show(22)
#DSL Equivalent with window functions
#DSL Equivalent with out using window functions #dropduplicates
#SQL Equivalent

print("e. Set Operations")


print("Aggregation on the joined data set")



print("SQL Aggregated final dataset")

###########Data processing or Curation or Transformation Starts here###########

print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
'''dfsqltransformedaggr.write.mode("append").csv("/user/hduser/dfsqltransformedaggr")
dfgroupingaggr.write.mode("overwrite").json("/user/hduser/aggrprofagerangejson")
dfjoinsqlagg.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="cust_aggr",mode="overwrite",properties={"driver": 'com.mysql.jdbc.Driver'})
df1innerjoined.write.mode("overwrite").saveAsTable("default.cust_trans_denormalized")
dfjoinsqlagg.write.mode("overwrite").saveAsTable("default.hive_aggr")
dfjoinsqlagg.write.partitionBy("profession").mode("overwrite").saveAsTable("default.hive_aggr_part")
dfjoinsqlagg.write.mode("overwrite").json("/user/hduser/aggrcustjson")
print("Spark App1 Completed Successfully")
'''