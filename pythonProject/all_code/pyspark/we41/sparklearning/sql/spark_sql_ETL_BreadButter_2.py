#https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
#/usr/local/spark/python/pyspark/sql/functions.py - date functions
#spark_sql_ETL_2.py start
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)

#1. SDLC life cycle -> requirement -> dev -> peer code review -> unit testing -> shift code to UAT/SIT-> Deployed
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

1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics
#EXTRACTION
a. Data Discovery/Data Exploration (EDA) - Performing an exploratory data analysis of the raw data to identify the attributes and patterns.
b. Combining Data + Schema Evolution/Migration/Merging (Structuring)

#TRANSFORMATION
c. Validation,
Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation, Validation,
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization, De Duplication and Replacement & Deletion of Attributes/Fields/Columns to make it in a usable format

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify
b. split, merge/Concat
c. Type Casting & Schema Migration

3. Data Customization & Processing - Application of Tailored Business specific Rules
a. User Defined Functions
b. Building of Frameworks & Reusable Functions

4. Data Curation/Pre Wrangling
a. Curation/Transformation
b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization

5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
a. Lookup/Reference
b. Enrichment
c. Joins
d. Sorting
e. Windowing, Statistical & Analytical processing

#LOAD
6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
a. Discovery
b. Outbound/Egress
c. Reports/exports
d. Schema migration
'''

print("***************1. Data Munging *********************")
#DSL and SQL - Interviewer will ask to write using ONLY DSL/ONLY SQL/both/any
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) (Passive) - Performing Data Exploration or (Data Discovery (Data governance, Data stewards, Data Security) and exploratory data analysis (Data Analysts, Data scientist) costly terms)  of the raw data to identify the attributes and patterns.
b. Combining Data + Schema Evolution/Merging (Structuring)
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
    .config("spark.jars","file:///home/hduser/install/mysql-connector-java.jar")\
    .enableHiveSupport()\
   .getOrCreate()
#.config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar")\
# .config("spark.jars", jdbc_lib)\
#spark.conf.set("hive.metastore.uris","thrift://127.0.0.1:9083")
#.config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
# to connect with Remote metastore, but we can't do it in Organization when we develop the pyspark app using Pycharm running in Windows

# Set the logger level to error
spark.sparkContext.setLogLevel("ERROR")

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

dfcsva = spark.read.format("csv").option("mode", "permissive").option("inferSchema", True).load("file:/home/hduser/hive/data/custsmodified")

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

#Prewrangling
#first day transaction customers
txns_dtfmt.where("dt=last_day(dt)").show(2)
#last day transaction customers
txns_dtfmt.where("dt=trunc(dt,'MM')").show(2)

#Munge, Enrich, Prewrangling Stages are completed for transaction data


print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
#Manually I will first take some sample data or actual data and analyse about the columns, datatype, values, nulls, duplicates(low/high cardinality), format (filetype/delimiter)
#mode- permissive (default)- permit all the data including the unclean data
#mode- failfast - as soon as you see some unwanted data, fail our program
#mode- dropmalformed - when there are unclean data (doesn't fit with the structure (customschema)/columns are lesser than the defined(custom schema)/identified(inferschema)) don't consider them

#FAIL FAST - Let us stop processing this data, since it is unclean (column numbers and datatype is not as expected)
#Failfast alone or Failfast with inferschema - MISSING COLUMNS
#custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='failfast',inferSchema=False)
#custdf11=readFile(spark,'csv',',','file:///home/hduser/hive/data/custsmodified','dropMalformed',True)
#custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='failfast',inferSchema=True)
#custdf1.printSchema()
#custdf1.show(20,False)

#mode='failfast',inferSchema=True/False - will fail only if the number of columns are lesser than the expected
#inferschema required 5 columns, but only 4 columns are found, so program is failing
#4000006,Patrick,Song,24
#trailer_data:end of file

#Failfast with custom schema - TYPE MISMATCH, MISSING COLUMNS
#custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='failfast',schema=custstructtype1)
#custdf1.printSchema()
#custdf1.show(20,False)

#TYPE MISMATCH, MISSING COLUMNS - custom schema requires id column should be integer, but it is string and also it requires 5 columns, but only 4 columns are found, so program is failing
#ten,Elsie,Hamilton,43,Pilot
#4000006,Patrick,Song,24
#trailer_data:end of file

#FAIL FAST ends here

#I want to fail my application if null is there in the significant column id (failfast will not work)?
custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropMalformed',schema=custstructtype1)
custdf1.printSchema()
custdf1.show(20,False)

#custdf1_clean=spark.createDataFrame(custdf1.rdd,custstructtype1)#workaround
#https://issues.apache.org/jira/browse/SPARK-10848
#ValueError: field id: This field is not nullable, but got None
#custdf1_clean.printSchema()
#custdf1_clean.show()

#PERMISSIVE STARTES HERE -
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive')#permissive is default
custdf1.printSchema()
custdf1.show(20,False)
custdf1.tail(1)
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',inferSchema=True).toDF("id","fname","lname","age","prof")
custdf1.printSchema()

#STATISTICAL DATA ANALYSIS
#BASIC DATA QUALITY CHECK ON THE PERMISSIVE DATA
#Significant attribute is custid (id)
#Data in the first column has string, but it has to be integer, ensure that with he below function
dtypecnt=custdf1.where("upper(id)<>lower(id)").count()
print(dtypecnt)
#2
#Data in the first column has null, may create challenges in the further data processing
#duplicate check
#Data in the first column has duplicate
distinctcnt=custdf1.select("id").distinct().count()
print(distinctcnt)
#10001
#null check
nullcnt=custdf1.where("id is null").count()
print(nullcnt)
#1

#COMPREHENSIVE STATISTICAL DATA ANALYSIS -(Raw data) - is not much preferred
custdf1.describe().show()
custdf1.summary().show()

#Lavanya asked to show the unwanted data to send back to the source system for applying the fix or for our (reject) analysis purpose
#How to filter out the corrupted/malformed data alone, to send it to the source system (custom way)
#custom methodology (costly approach to get the rejected data)
custstruct1 = StructType([StructField("id", IntegerType(), False),
                              StructField("fname", StringType(), False),
                              StructField("lname", StringType(), True),
                              StructField("age", ShortType(), True),
                              StructField("prof", StringType(), True)])
custdf_dropmalformed=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstruct1)
reject_df=custdf1.subtract(custdf_dropmalformed.withColumn("id",col("id").cast("string")))
reject_df.show()

#Simple and modern approach to achieve the same above reject data
custstruct1 = StructType([StructField("id", IntegerType(), False),
                              StructField("fname", StringType(), False),
                              StructField("lname", StringType(), True),
                              StructField("age", ShortType(), True),
                              StructField("prof", StringType(), True),
                              StructField("reject_data", StringType(), True)])
custdf_reject_col=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',schema=custstruct1,columnNameOfCorruptRecord='reject_data')
custdf_reject_col.printSchema()
custdf_reject_col.show()
custdf_reject_col.cache()#added after spark 2.4 version
reject_to_source_system=custdf_reject_col.where("reject_data is not null").select("reject_data")
clean_data_to_consume=custdf_reject_col.where("reject_data is null").drop("reject_data")#equivalent to drop malformed
reject_to_source_system.coalesce(1).write.mode("overwrite").csv("file:///home/hduser/hive/data/custsmodifiedrejected/")
#PERMISSIVE ENDS HERE -

#dropmalformed with custom schema - eliminate the TYPE MISMATCH, MISSING COLUMNS
#Let us clean and get the right data for further consideration (TYPE MISMATCH, MISSING COLUMNS)
#drop the unwanted/culprit data while creating the df (TYPE MISMATCH, MISSING COLUMNS)
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()
custdf_clean.show(20,False)
custdf_clean.count()#raw count
print(len(custdf_clean.collect()))#cleaned count
clean_cnt=len(custdf_clean.collect())
tot_cnt=custdf_clean.count()
print(tot_cnt-clean_cnt)

#statistical analysis (Clean data) - is good
custdf_clean.describe().show()
custdf_clean.summary().show()

#I realized null values in the key column, wanted to apply the contraint to fail my program incase if the structure is not fulfilled
#Exploring of data (Passive activity) CONCLUSION - data learning/identifying the data quality - failfast(SHOW STOPPER)/permissive(VERACITY)/dropmalformed(CLEANUP), nulls, dups, type mismatch, missing of columns, statistics, rejection of corrupted data

print("b. Combining Data (Structuring) + Schema Evolution/Merging (Structuring)")
print("b.1. Combining Data - Reading from ONE path contains multiple pattern of files")
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
#all the files in the base path
print(spark.read.csv("file:///home/hduser/sparkdata/src1").count())
#all the files in the base + first level of subdir also
print(spark.read.csv("file:///home/hduser/sparkdata/src1/*").count())

print("b.2. Combining Data - Reading from a path contains multiple pattern of files including all subfolders")
#all files in all sub directories including the main directory
print(spark.read.csv("file:///home/hduser/sparkdata/src1/*/*").count())#this will not work as expected, it will ignore the base directory data
print(spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True).count()) #it will get all files in a given directory including subdirs for all name pattern of the files
#The above function will read the main directory and subdirs contains multiple pattern of files nyse, bse, nse...
print(spark.read.csv(path="file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True,pathGlobFilter="nyse[1-2].csv").count())#it will get all files in a given directory including subdirs for given name pattern of the files
#The above function will recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

print("b.3. Combining Data - Reading from a multiple different base paths contains multiple pattern of files")
#subdirectories contains multiple pattern of files nyse, bse, nse...
#cp /home/hduser/sparkdata/src1/nyse1.csv /home/hduser/sparkdata/src1/src2/
print(spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True).count()) #it will get all files in a given directory including subdirs for all name pattern of the files

print(spark.read.csv(["file:///home/hduser/sparkdata/src1/","file:///home/hduser/sparkdata/src3/"],recursiveFileLookup=True).count())
#The above function will read the main directory and subdirs contains multiple pattern of files nyse, bse, nse...

print(spark.read.csv(["file:///home/hduser/sparkdata/src1/","file:///home/hduser/sparkdata/src3/"],recursiveFileLookup=True,pathGlobFilter="nyse[1-2].csv").count())
#The above function will search in the main and recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

print("b.4. Schema Merging (Structurization) - Schema Merging data with different structures from different sources (we know the structure of both datasets)")
#data from different sources or files may have column name, order, type, number of columns mismatches
'''
cat ~/sparkdata/stockdata2/bse2.csv
stock~value~maxrate~minrate~dur_mth~incr
TIT~450.3~710~400~6~-10
SUN~754.62~900~500~6~12.0
TMB~1000.2~1210.50~700~6~100.0
#what if if any one of the data has different data type (value is string), but the same column name
cat /home/hduser/sparkdata/stockdata/bse2.csv
stock~value~incr~maxrate~cat
HUL~450.3~-10.0~710~retail
RAN~754.62~12.0~900~pharma
CIP~1000.2~100.0~1210.50~pharma
'''

#Merging data with different structure of same type of data (csv)
#Below methodology of reading both paths leads to a wrong result
df_from_both_path=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/","file:///home/hduser/sparkdata/stockdata2/"],sep='~',header=True,inferSchema=True,pathGlobFilter="bse2.csv")
df_from_both_path.show()

#data from different sources or files may have column names, order, type, number of columns mismatches
#this is a workaround (costly traditional approach) to convert both DFs with same number of columns then perform union
#to perform union 2 thumb rules are there
#1. number of columns (with same order with the same column names) between the df should be same
#2. datatype of columns between the df should be same
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches

#structuring using traditional union ()
stock_df_1=stock_df.select("stock","value","maxrate",lit(-1).alias("minrate"),lit(-1).alias("dur_mth"),"incr","cat")
stock1_df_1=stock1_df.withColumn("cat",lit(''))
stock_df_1.union(stock1_df_1).show()#structurized data

#alternative easy/viable approach is using unionByName function to achive the above schema merging feature
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
merged_stock_df=stock_df.unionByName(stock1_df,allowMissingColumns=True)
merged_stock_df.printSchema()
merged_stock_df.show()

#Merging data with different structure from difference sources (RDBMS and FS source)
#create table stock(excname varchar(100),stockname varchar(100),stocktype varchar(100));
#insert into stock values('NYSE','INT','IT');
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table="stock",properties={"driver":"com.mysql.jdbc.Driver"})
#structurizing
merged_stock_df_db_file=stock_df.withColumnRenamed("stock","stockname").unionByName(dfdb1,allowMissingColumns=True)
merged_stock_df_db_file.printSchema()
merged_stock_df_db_file.show()

#Can we merge data between different file formats like orc and csv?
stock_df.write.mode("overwrite").orc("file:///home/hduser/sparkdata/stockdataorc/")
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock_df_orc=spark.read.orc("file:///home/hduser/sparkdata/stockdataorc/")
merged_df=stock1_df.unionByName(stock_df_orc,allowMissingColumns=True)
merged_df.show()

print("b.5. Schema Evolution -> Schema Migration + Merging (Structuring) using JSON/ORC/PARQUET")
#source is changing the columns order, numbers(adding/removing) and sending the data in csv format(most of source send data like this)
#structurizing different data format using SCHEMA MIGRATION & SCHEMA MERGING
#Source system is sending the data in a json format or
# if source is not ready to send in json then we are going to convert (SCHEMA MIGRATION) csv to json
# (json is not preferred way of schema migration because it is costly in storage and retrival (it is not intelligent tier storage as like orc/parquet))

'''
/home/hduser/sparkdata/stockdata/schemaevolution
ls -lrt
-rw-rw-r--. 1 hduser hduser 110 Jul 12 07:34 bseyesterday.csv
-rw-rw-r--. 1 hduser hduser 101 Jul 12 08:15 bsetoday.csv
cat bseyesterday.csv
stock~value~incr~maxrate~cat
HUL~450.3~neg~710~retail
RAN~754.62~pos~900~pharma
CIP~1000.2~pos~1210.50~pharma

cat bsetoday.csv
stock~value~incr~ipo~maxrate
HUL~450.3~neg~300~710
RAN~754.62~pos~500~900
CIP~1000.2~pos~200~1210.50
'''
#We have discussed in HIve classes -->
# json and AVRO(optimized json) is good for write and ORC & PARQUET for read. (APPLICABLE COMMONLY FOR spark, hive, athena, bigquery .....)

#SCHEMA EVOLUTION (SOURCE IS EVOLVING SCHEMA AND WE ARE ALSO PARALLELLY EVOLVING as per the source)
#Schema migration (JSON)
#yestday
stock_df_yest=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bseyesterday.csv"],sep='~',inferSchema=True,header=True)#order, number of columns mismatches
stock_df_yest.write.json("file:///home/hduser/sparkdata/stockdatajsondata/",mode="append")#schema migration
#today
stock_df_today=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bsetoday.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock_df_today.write.json("file:///home/hduser/sparkdata/stockdatajsondata/",mode="append")#schema migration
#SCHEMA MERGING + SCHEMA EVOLUTION (JSON)
stock_df_json=spark.read.json("file:///home/hduser/sparkdata/stockdatajsondata/")#By default json will apply unionByName to merge & evolve the schema

#Schema migration (ORC/PARQUET)
#spark application1
#yestday
stock_df_yest=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bseyesterday.csv"],sep='~',inferSchema=True,header=True)#order, number of columns mismatches
stock_df_yest.write.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mode="append")#schema migration
#today
stock_df_today=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bsetoday.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock_df_today.write.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mode="append")#schema migration

#spark application2 or same application
#SCHEMA MERGING + SCHEMA EVOLUTION (orc)
stock_df_orc=spark.read.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mergeSchema=True)#By default orc will apply unionByName to merge & evolve the schema? no

#PARQUET
#spark application1
#yestday
stock_df_yest=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bseyesterday.csv"],sep='~',inferSchema=True,header=True)#order, number of columns mismatches
stock_df_yest.write.parquet("file:///home/hduser/sparkdata/stockdatapardata/",mode="append")#schema migration
#today
stock_df_today=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bsetoday.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock_df_today.write.parquet("file:///home/hduser/sparkdata/stockdatapardata/",mode="append")#schema migration

#spark application2 or same application
#SCHEMA MERGING + SCHEMA EVOLUTION (orc)
stock_df_par=spark.read.parquet("file:///home/hduser/sparkdata/stockdatapardata/",mergeSchema=True)#By default orc will apply unionByName to merge & evolve the schema? no

#Some limitations while using orc & parquet:
#1. Orc and parquet works well for Column name/order/number of columns can be different, if the datatype is evolving , orc/parquet will fail
#Failed to merge fields 'incr' and 'incr'. Failed to merge incompatible data types string and int
#workaroud is --
stock1_df.withColumn("incr",col("incr").cast("string")).write.orc("file:///home/hduser/sparkdata/orcdata11/",mode="overwrite")
stock_df.write.orc("file:///home/hduser/sparkdata/orcdata11/",mode="append")
spark.read.orc("file:///home/hduser/sparkdata/orcdata11/",mergeSchema=True).show()

#direct solution is use unionByName option (provided data is from different sources) or use json (costly approach, but work for type mismatches)
#conclusion is use parquet/orc (no type mismatches, orc/parquet is preferred) or json (costly, but if type mismatches are there, json is preferred)
stock1_df.write.json("file:///home/hduser/sparkdata/stockdatajsondata1/",mode="append")#schema migration
stock_df.write.json("file:///home/hduser/sparkdata/stockdatajsondata1/",mode="append")#schema migration
spark.read.json("file:///home/hduser/sparkdata/stockdatajsondata1/")

#caveat - we dont do any data materialization at this stage - Data munging stage (EDA part alone)
#Vishagan - Hi Irfan - If we are facing this issue in production which will have millions of records (schema migration to parquet/orc),
#if json we are doing schema migration then no work around is needed, we can be free, but bit costly in a regular workload
# as a turn around they will do code change immediately if data type issue occurs or they request source to give the corrected data (long term process)
#Usecase: Consumers (Datascience Team):  source system (evolving data) -> data engineering team (evolving
# table loading) -> datascience team (consuming the evolving data)
# Irfan we are having a constant touch with the source system to send the additional attributes/columns based on our feature engineering
# we need a hive table where you can evolve the structure as per the source system's data you are going to receive
#Approach1:
#Hive (without using spark) -> Ask source to send the data in avro or convert the data to avro -> extract the avsc from avro file -> create hive table with avsc schema -> load the data

#Approach2:
#Hive (using spark) -> Ask source to send the data in avro or convert the data to avro -> extract the avsc from avro file -> create hive table with avsc schema -> load the data
#yestday
stock_df_yest=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bseyesterday.csv"],sep='~',inferSchema=True,header=True)#order, number of columns mismatches
stock_df_yest.write.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mode="overwrite")#schema migration
#df_read_orc=spark.read.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mergeSchema=True)
#df_read_orc.createOrReplaceTempView("orcmemview")
spark.sql("drop table if exists ext_orc_evolve_tbl2")
spark.sql("create external table ext_orc_evolve_tbl2 stored as orc location 'hdfs://localhost:54310/user/hduser/extorctbl2/'  as select * from orc.`file:///home/hduser/sparkdata/stockdataorcdata/` where 1=2")
spark.sql("load data local inpath 'file:///home/hduser/sparkdata/stockdataorcdata/' into table ext_orc_evolve_tbl2")

#today
stock_df_today=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/schemaevolution/bsetoday1.csv"],sep='~',inferSchema=True,header=True)#order, type, number of columns mismatches
stock_df_today.write.orc("file:///home/hduser/sparkdata/stockdataorcdata/",mode="overwrite")#schema migration
spark.sql("drop table if exists ext_orc_evolve_tbl2")
spark.sql("create external table ext_orc_evolve_tbl2 stored as orc location 'hdfs://localhost:54310/user/hduser/extorctbl2/' as select * from orc.`file:///home/hduser/sparkdata/stockdataorcdata/` where 1=2")
spark.sql("load data local inpath 'file:///home/hduser/sparkdata/stockdataorcdata/' into table ext_orc_evolve_tbl2")

print("c.1. Validation (active)- DeDuplication")

#Let us clean and get the right data for further consideration
#drop the unwanted/culprit data while creating the df
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()#clean data frame without datatype mismatch and without number of columns mismatches
custdf_clean.show(20,False)

#I want to continue my application if null is there in the significant column id,
# but I want to either eliminate this null record or replace this null with something else later?

#Record level de-duplication - retain only one record and eliminate all duplicate records
#INTERVIEW QUESTION - what is the differenct between distinct and dropduplicates
dedup_custdf_clean=custdf_clean.distinct()
dedup_custdf_clean.where("id =4000001").show()
#or
dedup_custdf_clean=custdf_clean.dropDuplicates()
dedup_custdf_clean.where("id =4000001").show()

#column's level de-duplication - retain only one record and eliminate all duplicate records with custid is duplicated
dedup_custdf_clean.where("id =4000003").show()
dedupcol_custdf_clean=custdf_clean.dropDuplicates(subset=["id"])#dropduplicates will retain only the first duplicate record
dedupcol_custdf_clean.where("id =4000003").show()

#want to retain a particular dup data
custdf_clean.sort("custage",ascending=False).where("id =4000003").show()
dedupcol_custdf_clean=custdf_clean.sort("custage",ascending=False).dropDuplicates(subset=["id"])
dedupcol_custdf_clean.where("id =4000003").show()

#can you write a spark sql to achive the same functionality?
custdf_clean.createOrReplaceTempView("cleanview1")
dedup_custdf_clean=spark.sql("select distinct * from cleanview1")
custdf_clean.createOrReplaceTempView("cleanview2")

#column level dedup using sql (analytical & windowing functions) - very very important for interview
#instead of saying Aschending, true or False is there a way to remove the second or 1st occurrence ?
dedupcol_custdf_clean=spark.sql("""select id,custfname,custlname,custage,custprofession from 
                                      (select *,row_number() over(partition by id order by custage desc) rno from cleanview2)view1
                                   where rno=1""")
dedupcol_custdf_clean.show()

# let us know writing DSL or SQL is simple here to achive this output? DSL wins here in terms of simplicity (performance wise both are same)

#Difference between tempview and globaltempview
#global temp view has the scope across the spark session WITHIN the application
#custdf_clean.createGlobalTempView("gtempview")
#spark.stop()
#spark=SparkSession.builder.getOrCreate()
#spark.sql("select * from gtempview").show()
#Writing the above deduplication using SQL


######### TRANSFORMATION PART ##########
###########Data processing or Curation or Transformation Starts here###########

print("c.2. Data Preparation/Validation/Standardization (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
print("Data Cleansing (removal of unwanted/dorment (data of no use) datasets eg. na.drop, drop malformed, deduplication)")
print("Dropping the unwanted/dorment records with all of the columns are null (null will be shown when "
      "datatype mismatch eg. string in the place of int, format mismatch eg. dt format, column missing/wrong mapping, blankspace in the data itself or None(null) in the data itself)")

print("Dropping the null records with all of the columns are null ")
allnulls=dedupcol_custdf_clean.na.drop(how="all")
allnulls.count()
custdf_clean.where("id is null and custfname is null and custlname is null and custage is null and custprofession is null").show()
allnulls.where("id is null and custfname is null and custlname is null and custage is null and custprofession is null").show()

print("Dropping the null records with any one of the column is null ")
anynulls=dedupcol_custdf_clean.na.drop(how="any")#datascientists will use this heavily (because all the features(attributes) should have value to run the Datascience model)
anynulls.count()
anynulls.where("id is null or custfname is null or custlname is null or custage is null or custprofession is null").show()
print("Dropping the null records with both custid OR custprofession is null")
allnulls_id_prof=dedupcol_custdf_clean.na.drop(how="all",subset=["id","custprofession"])
allnulls_id_prof.where("id is null and custprofession is null").show()
print("Dropping the null records with custid OR custprofession is null ")
anynulls_id_prof=dedupcol_custdf_clean.na.drop(how="any",subset=["id","custprofession"])
anynulls_id_prof.where("id is null or custprofession is null").show()

#Cleansed Dataframe for further scrubbing
dedupcol_custdf_cleansed_id_notnull=dedupcol_custdf_clean.na.drop(how="any",subset=["id"])
dedupcol_custdf_cleansed_id_notnull.where("id is null").show()

print("set threshold as a second argument if non NULL values of particular row is less than thresh value then drop that row")


#understanding thresh
'''
NYSE~~
NYSE~CVH~
NYSE~CVL~30.2
~~~
'''
dft1=spark.read.csv("file:///home/hduser/sparkdata/nyse_thresh.csv",sep='~').toDF("exch","stock","value")
#drop any row that contains upto zero (<1) not null columns
# (retain all the rows that contains any minimum one not null column)
#drop any row that contains upto one (<2) not null columns or retain rows with minimum 2 columns with not null
#drop any row that contains upto two (<3) not null columns or retain rows with minimum 3 columns with not null
#drop any row that contains upto three (<4) not null columns or retain rows with minimum 4 columns with not null

#na.drop - Parameters:
#how: This parameter is used to determine if the row or column has to remove or not.
#‘any’ – If any of the column value in Dataframe is NULL then drop that row.
#‘all’ – If all the values of particular row or columns is NULL then drop.
#thresh: If non NULL values of particular row or column is less than thresh value then drop that row or column.
#subset: If the given subset column contains any of the null value then drop that row or column.

print("Scrubbing (convert of raw to tidy na.fill or na.replace)")
# Writing DSL
print("fill the null with default values ")
dedupcol_custdf_cleansed_profession_na=dedupcol_custdf_cleansed_id_notnull.na.fill("Profession not given",subset=["custprofession"])
dedupcol_custdf_cleansed_profession_na.where("custprofession ='Profession not given'").show()
dedupcol_custdf_cleansed_id_notnull.where("custlname is null").show()
dedupcol_custdf_cleansed_profession_na=dedupcol_custdf_cleansed_id_notnull.na.fill("not given",subset=["custlname","custprofession"])
dedupcol_custdf_cleansed_profession_na.where("custprofession is null").count()
dedupcol_custdf_cleansed_profession_na.where("custlname is null").show()
#dedup_dropfillna_clensed_scrubbed_df1.show()
dedupcol_custdf_cleansed_profession_na=dedupcol_custdf_cleansed_id_notnull.na.fill("not given",subset=["custlname"]).na.fill("profession not given",subset=["custprofession"])

print("Replace (na.replace) the key with the respective values in the columns "
       "(another way of writing Case statement)")
dict1={"Reporter":"Media","Musician":"Instrumentist"}
dedupcol_custdf_cleansed_profession_scrubbed_replace=dedupcol_custdf_cleansed_profession_na.withColumn("srcsystem",lit("Retail")).na.replace(dict1,subset=["custprofession"])
dedupcol_custdf_cleansed_profession_scrubbed_replace.show()

#dedup_dropfillreplacena_clensed_scrubbed_df1.show()
#Writing equivalent SQL (drop,fill,replace) - for learning the above functionality more relatively & easily and to understand which way is more feasible
#DSL is more simple to write (provided if you know how to call the functions such as na.drop/fill/replace
dedupcol_custdf_cleansed_profession_scrubbed_replace_singleDSL=dedupcol_custdf_clean.na.drop(how="any",subset=["id","custprofession"]).na.fill("not given",subset=["custlname"]).na.fill("profession not given",subset=["custprofession"]).na.replace(dict1,subset=["custprofession"])

#OR (with SQL)
dedupcol_custdf_clean.createOrReplaceTempView("cleansedview1")
dedupcol_custdf_cleansed_profession_scrubbed_replace_sql=spark.sql("""select id,custfname,coalesce(custlname,'not given') as custlname,custage,
case when custprofession is null then 'profession not given' 
when custprofession ='Reporter' then 'Media'
when custprofession ='Musician' then 'Instrumentist' 
else custprofession 
end as custprofession
from cleansedview1
where id is not null""")
dedupcol_custdf_cleansed_profession_scrubbed_replace_sql.show()

#OR (Marry DSL and SQL)
#dedupcol_custdf_clean.select("id",expr("coalesce(custlname,'not given') as custlname,custage,case when custprofession is null then 'profession not given' when custprofession ='Reporter' then 'Media' when custprofession ='Musician' then 'Instrumentist' else custprofession end as custprofession")).show()

print("d.1. Data Standardization (columns) - Column re-order/number of columns changes (add/remove/Replacement/Renaming)  to make it in a usable format")
#select can be used in the starting stage to
#re order or choose the columns or remove or replace using select or add
#re ordering of columns
#DSL Functions to achive reorder/add/removing/replacing respectively select,select/withColumn,select/drop,select/withColumn
#select - reorder/add/removing/replacement/rename
#withColumn - add
#withColumn - Replacement of columns custlname with custfname
#withColumnRenamed - rename of a given column with some other column name and for replacing of columns
#drop - remove columns
#Select if preferred for reordering the columns, but it can achieve everything
reord_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custage","custlname","custfname","custprofession")#reordering
reord_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custlname","custfname","custprofession")#removing
reord_added_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custage",lit("custdata").alias("typeofdata"),"custlname","custfname","custprofession")#adding
rename_replace_reord_added_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id",col("custage").alias("age"),col("custlname").alias("custfname"),"custprofession")

#withColumn - (preferred for adding columns in the last)
add_col_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.withColumn("sourcesystem",lit("retail"))

#withColumn - Not preferred for Replacement of columns custlname with custfname (column order will be changed)
dedupcol_custdf_cleansed_profession_scrubbed_replace.withColumn("custfname",col("custlname")).drop("custlname").show()

#withcolumn - for renaming (not preferred much because it rearrange the column order)
dedupcol_custdf_cleansed_profession_scrubbed_replace.withColumn("custfname",col("custlname")).drop("custlname").show()

#withColumnRenamed - rename of a given column with some other column name (preferred much because it just rename without changing the column order)
dedupcol_custdf_cleansed_profession_scrubbed_replace.drop("custfname").withColumnRenamed("custlname","custfname").show(2)
dedupcol_custdf_cleansed_profession_scrubbed_replace.withColumnRenamed("custage","age").show()
#If already a column custfname is present, then rename after removing the custfname column
#dataframe with duplicate column name leads to ambuiguity issue, we should resolve it by having unique column names

dedup_dropfillreplacena_clensed_scrubbed_df1=dedupcol_custdf_cleansed_profession_scrubbed_replace

#we can do rename/duplicating column/derivation of a column also using withColumn, we will see further down
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",col("custlname").alias("custfname"),lit("Retail").alias("srcsystem"))
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",upper("custlname").alias("custfname"),lit("Retail").alias("srcsystem"))

# removal of columns
chgnumcol_reord_df5=reord_added_removed_df.drop("custlname")#preffered way if few columns requires drop
chgnumcol_reord_df6_1=reord_added_removed_df.select("id","custprofession","custage","custfname")

#achive replacement and removal using withColumnRenamed
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumnRenamed("custage","age").show()#preffered way if few columns requires drop
#above function withColumnRenamed("custlname","custfname") did 2 things, 1 - replacement of custfname with custlname and 2 - dropped custlname because
#both the columns exists in the df
#above function withColumnRenamed("custage","age") did 2 things, 1 - created a new column called age and 2 - dropped custage because only custage exists in the df
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumn("age",col("custage")).drop("custage").show()#equivalent to the above function
#columns will be reordered

#equivalent SQL
dedup_dropfillreplacena_clensed_scrubbed_df1.createOrReplaceTempView("dedup_dropfillna_clensed_scrubbed_view")
chgnumcol_reord_df6_1_sql=spark.sql("""select id,custprofession,custage,custlname as custfname,'Retail' as srcsystem 
from dedup_dropfillna_clensed_scrubbed_view""") #found to be ease of use

#conclusion of functions used:
#When to use what functions (conclusion)
# select is just used for reordering of columns or applying all strategies in the (starting point itself)
# select not supposed to be used, rather use withColumn, drop, withColumnRenamed for the specific functionalities individually
# number of columns changes by adding columns in the last - withColumn
# adding of columns (we don't do here, rather in the enrichment we do) - withColumn()
#removal of columns - drop()
#renaming of columns - withColumnRenamed()
#replacement of columns - withColumnRenamed()

print("******************** 1. data munging completed****************")

###########Data processing or Curation or Transformation###########
print("***************2. Data Enrichment (values)-> "
      "Add, Rename, combine(Concat), Split, Casting of Fields, Format(not in a expected format, we are formatting), Reformat(already formatted, but reformatting in the expected way), replacement of (values in the columns) "
      "- Makes your data rich and detailed *********************")
munged_df=dedupcol_custdf_cleansed_profession_scrubbed_replace

#select,drop,withColumn,withColumnRenamed
#Adding of columns (withColumn/select) - for enriching the data
enrich_addcols_df6=munged_df.withColumn("loaddt",current_date()).withColumn("loadts",current_timestamp())#both are feasible
#or
enrich_addcols_df6_1=munged_df.select("*",current_date().alias("load"),current_timestamp().alias("loadts"))#both are feasible

#Natural key - A key either sequence number or unique value like aadhar/pan/phonenumber which will be generated/arrived in the source system itself
#Surrogate key - is a seqnumber column i can add on the given data set if the dataset doesn't contains natural key or
# if i want add one more local surrogate key for better processing of data within this system

#munged_df.orderBy("custage",ascending=True).withColumn("skey",monotonically_increasing_id()).show()
enrich_addcols_df6.select(monotonically_increasing_id().alias("custsk"),"*").show(2)

#Rename of columns (withColumnRenamed/select/withColumn & drop) - for enriching the data
enrich_ren_df7=enrich_addcols_df6.withColumnRenamed("srcsystem","src")#preferrable way (delete srcsystem and create new column src without changing the order)
enrich_ren_df7_1=enrich_addcols_df6_1.select("id","custprofession","custage","custfname",col("srcsystem").alias("src"),"curdt","loadts")#not much preferred
enrich_ren_df7_2=enrich_addcols_df6_1.withColumn("src",col("srcsystem")).drop("srcsystem")#costly effort

#Concat to combine/merge/melting the columns
enrich_combine_df8=enrich_ren_df7.select("id","custfname","custprofession",concat("custfname",lit(" is a "),"custprofession").alias("nameprof"),"custage","sourcesystem","loaddt","loadts")

#try with withColumn (that add the derived/combined column in the last)
enrich_combine_df8=enrich_ren_df7.withColumn("nameprof",concat("custfname",lit(" is a "),"custprofession")).drop("custfname")

#Splitting of Columns to derive custfname
enrich_combine_split_df9=enrich_combine_df8.withColumn("custfname",split("nameprof",' ')[0])

#Derive a column from the given column values
enrich_combine_df8.selectExpr("*","case when nameprof like '%Secretary' then 'yes' else 'no' end as isSecretary")

#enrich_combine_df8.selectExpr("*,case when nameprof like '%Secretary' then 'yes' else 'no' end as isSecretary")#this won't work

#Using both DSL and SQL syntax using selectExpr or select(expr("sql logic"))
enrich_combine_df8.select("*",lit("dummy").alias("dummycol"),expr("case when nameprof like '%Secretary' then 'yes' else 'no' end as isSecretary"))

#Enriching the Datatype - Casting of Fields
#Changing the existing column type - custage from smallint to string
enrich_combine_split_df9.withColumn("custage",col("custage").cast("string"))
#Changing the datatype of a derived column
enrich_combine_split_df9.withColumn("loaddtstr",col("loaddt").cast("string")).withColumn("yrstr",year("loaddtstr").cast("string"))

enrich_combine_split_cast_df10=enrich_combine_split_df9.withColumn("curdtstr",col("curdt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("yearstr",substring("curdtstr",1,4))

#Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
#Deriving date using date_format from timestamp and viceversa, case conversion/formatting, using substr for formatting
enrich_combine_split_cast_reformat_df10=enrich_combine_split_df9.withColumn("sourcesystem",upper("sourcesystem"))\
    .withColumn("fmtdtstr",concat(substring("loaddt",6,2),lit("/"),substring("loaddt",9,2)))\
    .withColumn("fmtloaddt",date_format("loadts",'yyyy-MM-dd')).withColumn("dtfmt",date_format("loaddt",'yyyy/MM/dd hh:mm:ss')).withColumn("tsfmt",to_timestamp(date_format("loaddt",'yyyy-MM-dd hh:mm:ss')))
#enrich_combine_split_cast_reformat_df10=enrich_combine_split_df9.withColumn("curdtstr",col("loaddt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("curdtstr",concat(substring("curdtstr",3,2),lit("/"),substring("curdtstr",6,2))).withColumn("dtfmt",date_format("loaddt",'yyyy/MM/dd hh:mm:ss'))

#try with ansi SQL
#enrich_combine_split_cast_reformat_df10
#SQL equivalent using inline view/from clause subquery
#Try deriving the enrich_combine_split_cast_reformat_df10 dataframe using SQL
#combining of columns
munged_df.createOrReplaceTempView("view1")
enrich_combine_df8=spark.sql("""select id,custfname,custprofession,
concat(custfname,' is a ',custprofession) nameprof,
custage, srcsystem as src, current_date() loaddt, current_timestamp() loadts
from view1
""")
enrich_combine_df8.show(5)

#splitting and deriving of columns
enrich_combine_df8.createOrReplaceTempView("view2")
enrich_combine_split_df9=spark.sql("""select id, SUBSTRING_INDEX(nameprof,' ',1) as custfname ,custprofession,
nameprof, custage, src, loaddt, loadts,
case when nameprof like '%Secretary%' then 'yes' else 'no' end as isSecretary
from view2
""")
enrich_combine_split_df9.show(10,False)
enrich_combine_split_df9.where("isSecretary = 'yes'").show(5,False)

#Formatting/ReFormatting of daa
enrich_combine_split_df9.createOrReplaceTempView("view3")
enrich_combine_split_cast_reformat_df10=spark.sql("""select *,
date_format(loaddt,'%m-%d') fmtdt, 
date(loadts) fmtloaddt,
date_format(loaddt,'%Y/%m/%d %h:%i:%S') fmtts,
timestamp(loaddt) fmtloadts
from view3
""")
enrich_combine_split_cast_reformat_df10.show(5,False) #throwing below error
# pyspark.sql.utils.IllegalArgumentException: All week-based patterns are unsupported since Spark 3.0, detected: Y,
# Please use the SQL function EXTRACT instead


#****************2. Data Enrichment Completed Here*************#

print("***************3. Data Customization & Processing (Business logics) -> "
      "Apply User defined functions and "
      "utils/functions/modularization/reusable/generic functions & reusable framework creation *********************")
munged_enriched_df=enrich_combine_split_cast_reformat_df10
print("Data Customization can be achived by using UDFs - User Defined Functions")
print("1. User Defined Functions must be used only if it is Inevitable (un-avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions."
      "2. We go for UDFS if we want to implement some Organization specific business logic has to be used uniformly accross the Team/Projects"
      "If any change I do in one function that affects everywhere Eg. GST for rice is 4% fy2023 it is changed to 3% fy2024 "
      "If we don't create UDF then we have touch the code everywhere, rather than changing the code in one UDF")

#UDF's are a black box to Spark hence it can't apply optimization and you will lose all the optimization Spark does on Dataframe
#Interview Question: Hi Irfan, Whether you developed any UDF's in your project?
#some realtime examples when we can't avoid using UDFs
# Yes, but very minumum numbers we have and I developed 1/2 UDFs - (custom data masking (Shuffle, random, hash, positional..), filteration (CPNI,GSAM, PII),data profiling/classification, custom business logic like promo ratio calculation, customer intent identification)
# But, we should avoid using UDFs until it is Inevitable
#But , we can say it like instead of udf , we created some reusable frameworks


#case1: How to avoid using UDFs
#step1: Create a function (with some custom logic) or download a function/library of functions from online writtern in python/java/scala...
convertToUpperPyLamFunc=lambda prof:prof.upper()

#step2: Import the udf from the spark sql functions library
from pyspark.sql.functions import udf

#step3: Convert the above function as a user defined function (which is DF-DSL ready)
convertToUpperDFUDFFunc=udf(convertToUpperPyLamFunc)

#step3: Convert the above function as a user defined function and Register it in the spark metastore layer (which is SQL ready)
spark.udf.register("convertToUpperPyLamFuncSQL",convertToUpperPyLamFunc)
#pyspark.sql.utils.AnalysisException: Undefined function: 'udfConverttoupper'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 7

#step4: Apply the UDF to the given column(s) of the DF using DSL program
customized_munged_enriched_df=munged_enriched_df.withColumn("custprofession",convertToUpperDFUDFFunc("custprofession"))
customized_munged_enriched_df.show(2)

#using builtin(predefined) function (avoid applying the above 4 steps and get the result directly) - PREFERRED WAY
predefined_munged_enriched_df=munged_enriched_df.withColumn("custprofession",upper("custprofession"))
predefined_munged_enriched_df.show(2)

#SQL Equivalent
customized_munged_enriched_df.createOrReplaceTempView("view1")
spark.sql("select id,convertToUpperDFSQLFunc(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)
spark.sql("select id,upper(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)

#In the above case, usage of built in function is better - built in is preferred, if not available go with UDF

#Quick Usecase:
#Write a python def function to calculate the age grouping/categorization of the people based on the custage column, if age<13 - childrens, if 13 to 18 - teen, above 18 - adults
#derive a new column called agegroup in the above dataframe (using DSL)
#Try the same with the SQL also
#UDF
from pyspark.sql.functions import udf
def ageCat(age):
    if age < 13:
        return 'Children'
    elif age>=13 and age<=18:
        return 'Teen'
    else:
        return 'Adults'


dslAgeGroup=udf(ageCat)
customize_df1=enrich_combine_split_cast_reformat_df10.withColumn("agegroup",dslAgeGroup("custage"))
customize_df1.show(5,False)

spark.udf.register("sqlAgeGroup",ageCat)
enrich_combine_split_cast_reformat_df10.createOrReplaceTempView("view10")
spark.sql("select *, sqlAgeGroup(custage) agegroup from view10").show(5,False)
#or directly write the sql equivalent of the above udf, if the udf is not a common/generic function used accross the portfolio or project or Organization
spark.sql("select *, case when custage<13 then 'children' when custage>=13 and custage<=18 then 'teen' else 'adult' end as agegroup from view10").show(5,False)


#Step4: New column deriviation called age group, in the above dataframe (Using DSL)
#custom_agegrp_munged_enriched_df = munged_enriched_df.withColumn("agegroup",age_custom_validation("custage"))
#or
custom_agegrp_munged_enriched_df=munged_enriched_df.withColumn("agegroup",when(col("custage")<13,lit("Children")).when((col("custage")>=13),lit("Teen")).otherwise(lit("Adult")))
#or
#Marry DSL & SQL - Writing SQL expressions in DSL
#without creating a temp view, by using expr/selectExpr, we can use sql on top of df right or sql along with dsl
custom_agegrp_munged_enriched_df=munged_enriched_df.select("*",expr("case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup"),expr("cast(custage as string)"))
#or
custom_agegrp_munged_enriched_df=munged_enriched_df.selectExpr("*","case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup")

#case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup

custom_agegrp_munged_enriched_df=munged_enriched_df.select("*",expr("case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup"))

#Step6: Display the dataframe and Filter the records based on age_group
custom_agegrp_munged_enriched_df.show(5,False)
custom_agegrp_munged_enriched_df.filter("agegroup = 'Adults'").show(5,False)

#SQL Equivalent
# (Not preferred way - because using udf)
#enrich_combine_split_cast_reformat_df10_SQL.createOrReplaceTempView("munged_enriched_df_view")
#munged_enriched_df.createOrReplaceTempView("munged_enriched_df_view")
#custom_agegrp_munged_enriched_df_SQL=spark.sql("select *,custom_age_validation_sql(custage) agegroup from munged_enriched_df_view")
#custom_agegrp_munged_enriched_df_SQL.show(5,False)

# (Preferred way - don't use udf)
custom_agegrp_munged_enriched_df_SQL=spark.sql("""select *,"
                                               case when custage<13 then 'Children' 
                                               when custage>=13 and custage<=18 then 'Teen' 
                                               else 'Adults' end as agegroup 
                                               from munged_enriched_df_view""")
custom_agegrp_munged_enriched_df_SQL.show(5,False)

#Below function is a reusable function (accepts/returns anyobject - DF, Sparksession, RDD, View) and not a udf (udf means if we apply on the columns some functionality)
#MUST GO function - Below utils/functions/modularization/reusable framework are suggested to
# Standardize the code,
# create uniformity and for applying some peformance optimization and best practices
def readFile(ss,type,sep,loc,mod,hdr):
 if(type=='csv'):
  df1=ss.read.format(type).option("mode",mod).option("delimiter",sep).option("header",hdr).load(path=loc)
  if(df1.count()>1000000):
   return df1.repartition(20)
  else:
   return df1
 elif(type=='json'):
  return ss.read.type(loc,mode=mod)
 else:
  return ss.read.orc(loc)

custdf11=readFile(spark,'csv',',','file:///home/hduser/hive/data/custsmodified','dropMalformed',False)

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

#3. Data Customization Completed Here

print("***************4. Core Data Processing/Transformation/Curation (Level1) (Pre Wrangling)  -> "
      "filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytical functionalities *********************")
#Transformation Functions -> select, where, sort, group, aggregation, having, transformation/analytical function, distinct...
pre_wrangled_customized_munged_enriched_df=custom_agegrp_munged_enriched_df.select("id","custprofession","custage","src","curdt")\
    .where("convertToUpperDFSQLFunc(custprofession)='FIREFIGHTER' or custprofession='WRITER'")\
    .groupBy("custprofession")\
    .agg(avg("custage").alias("avgage"))\
    .where("avgage>49")\
    .orderBy("custprofession")

def munged_prewrangled(df1,useordby):#inline function
 if (useordby=='y'):
  pre_wrangled_customized_munged_enriched_df = df1.select("id", "custprofession",                                                                                     "custage", "src", "curdt") \
        .where("convertToUpperDFSQLFunc(custprofession)='FIREFIGHTER' or custprofession='WRITER'") \
        .groupBy("custprofession") \
        .agg(avg("custage").alias("avgage")) \
        .where("avgage>49") \
        .orderBy("custprofession")
 else:
  pre_wrangled_customized_munged_enriched_df = df1.select("id", "custprofession",                                                                                     "custage", "src", "curdt") \
        .where("convertToUpperDFSQLFunc(custprofession)='FIREFIGHTER' or custprofession='WRITER'") \
        .groupBy("custprofession") \
        .agg(avg("custage").alias("avgage")) \
        .where("avgage>49")
 return pre_wrangled_customized_munged_enriched_df

pre_wrangled_customized_munged_enriched_df=munged_prewrangled(custom_agegrp_munged_enriched_df,'y')

#SQL simple/regular/common Clauses
#select -
#aggregate function sum,mean,avg..., conversion, transformation, formatting,columns
#from - one or multiple tables or views
#where - filter conditions
#group by - non aggregated columns in the select
#having - if we need to apply filter on the aggregated fields
#order by - columns (aggregated or grouped columns)
#limit - to display only few records output

#pre wrangled data to consume in the next wrangling stage
#pre wrangled data to produce it to the consumer last persistant stage

#Filter rows and columns
#select * from where
filtered_adult_row_df_for_consumption1=custom_agegrp_munged_enriched_df.where("agegroup='Adults'")#Will be used in the Data persistant last stage
#select few-columns from where
filtered_nochildren_rowcol_df_for_further_wrangling1=custom_agegrp_munged_enriched_df.filter("agegroup<>'Children'").select("id","custage","loaddt","custfname","agegroup")#Will be used in the next stage for further wrangling

#Aggregation to derive pre wrangled data to consume in the next wrangling stage
#Aggregation to derive pre wrangled data to produce it to the consumer last persistant stage

#Dimensions, Measures & metrics (KPI-Key Performance Indicators) we are going to derive
#Dimension - multiple view or viewing of data in different aspects/factors (agegroup,year,profession wise)
#Measures(profit value of 10lakhs - number based)/Metrics (profit/loss/sales) - It is a actual value/metrics/key performance indicator
#Dimensions are - agegroup,custprofession
#Metrics are - cnt,minage,maxage,distcnt,avgage
#Measure are - count of 1, count of 208 : minage of 21, max age of 75
#+--------+--------------------+---+------+------+-------+------------------+
#|agegroup|      custprofession|cnt|minage|maxage|distcnt|            avgage|
#+--------+--------------------+---+------+------+-------+------------------+
#|Children|profession not given|  1|    11|    11|      1|              11.0|
#|  Adults|               Pilot|208|    21|    75|     54| 47.81730769230769|

#select few columns, where,group by, aggregation, order by
df_dim_met_measures=custom_agegrp_munged_enriched_df.\
    where("sourcesystem='RETAIL'").\
    select("custprofession","custage","agegroup").\
    groupBy("agegroup","custprofession").\
    agg(count("custage").alias("cnt"),min("custage").alias("minage"),max("custage").alias("maxage"),
        countDistinct("custage").alias("distcnt"),avg("custage").alias("avgage"))\
    .orderBy(["agegroup","custprofession"],ascending=[True,False])

#DSL - select few columns, where,group by, aggregation,having(where), order by
df_dim_met_measures=custom_agegrp_munged_enriched_df.where("sourcesystem='RETAIL'").\
    select("custprofession","custage","agegroup").\
    groupBy("agegroup","custprofession").\
    agg(count("custage").alias("cnt"),min("custage").alias("minage"),max("custage").alias("maxage"),
        countDistinct("custage").alias("distcnt"),avg("custage").alias("avgage")).\
    where("avgage<=50").\
    orderBy(["agegroup","custprofession"],ascending=[True,False])

#SQL Equivalent
custom_agegrp_munged_enriched_df.createOrReplaceTempView("cust_enrich_view1")
#Below is not much optimistic, because we are computing avg(custage) once and in the having also we are calculating avg(custage) again
spark.sql("""select agegroup,custprofession,count(custage) cnt,min(custage) minage,max(custage) maxage,
                    count(distinct custage) distcnt,avg(custage) avgage
                    from cust_enrich_view1
                    where sourcesystem='RETAIL'
                    group by custprofession,agegroup
                    having avg(custage)>50
                    order by agegroup asc,custprofession desc""").show(10)

#Below is more optimistic, because we are computing avg(custage) once and filtering later using that avgage without computing again
spark.sql("""select * from (select agegroup,custprofession,count(custage) cnt,min(custage) minage,max(custage) maxage,
                    count(distinct custage) distcnt,avg(custage) avgage
                    from cust_enrich_view1
                    where sourcesystem='RETAIL'
                    group by custprofession,agegroup                    
                    order by agegroup asc,custprofession desc)a
                    where avgage>50""").explain()

from pyspark.sql.functions import *
#Tell me year,agegroup,profession wise max/min/avg/count/distinccount/mean of age and availabledate of the given dataset
#(select,group by, aggregation, where, order by)
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2=custom_agegrp_munged_enriched_df.\
    where("agegroup<>'Children'").\
    groupby("year","agegroup","custprofession").\
    agg(max("curdt").alias("max_curdt"),min("curdt").alias("min_curdt"),avg("custage").alias("avg_custage"),
        mean("custage").alias("mean_age"),countDistinct("custage").alias("distinct_cnt_age")).\
    orderBy("year","agegroup","custprofession",ascending=[False,True,False])

#Tell me the average age of the above customer is >35 (adding having)
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2=dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2\
                                                                                                .where("avg_custage>50")#having clause

#Analytical Functionalities
#Data Random Sampling:
randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)
randomsample2_for_consumption4=custom_agegrp_munged_enriched_df.sample(.2,11)#Consumer (Datascientists needed for giving training to the models)
randomsample3_for_consumption5=custom_agegrp_munged_enriched_df.sample(.2,25)#Consumer (Datascientists needed for giving training to the models)

#Summary/Description (for the consumer's purpose)
summary1_for_consumption6=filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.summary()

#Co relation - relation between the attributes using which we can deliver the attributes to the consumers of
#we can identify the common columns between multiple dataframes or the same DF, if i calculate the correlation after joing the DFs
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.corr("avg_custage","mean_age")
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.corr("avg_custage","distinct_cnt_age")

#co variance
filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.cov("avg_custage","distinct_cnt_age")

#Frequecy Analysis - for calculating the cardinality of the data and for identifying the group of volume
df_freq_prof_agegroup_consumption7=filter_dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_further_wrangling2.freqItems(["custprofession","agegroup"],.4)

#SQL Equivalent of the Same (DSL vs SQL)
#spark will convert it...
#the groupby() and agg() in DSL is the columns (selected for group by later) and agg functions in the select in SQL
#the first where clause in DSL is the where clause in SQL
#DSL groupby() is the groupby in SQL
#DSL's last where clause is the having clause in SQL
#DSL's orderBy is the order by in SQL (kept in the last)

custom_agegrp_munged_enriched_df.createOrReplaceTempView("custom_agegrp_munged_enriched_df_view")

#write order of the sql - select,columns of groupby, columns of aggr, from, where, group by, having, order by
#IMPORTANT for Non SQL people
#execution order of the sql (traditional DB) - from (bring all data (rows/cols) from disk to memory), where,select, group by, having, order by
#execution order of the sql (Spark + parquet/orc) - from (select only specific cols where rows meet the filter condition from disk to memory applying PDO), group by, having, order by
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql1=spark.sql("""
select * from (select year, agegroup, custprofession, max(curdt) as max_curdt, min(curdt) as min_curdt, 
avg(custage) as avg_custage, avg(custage) as mean_age, count(distinct custage) as distinct_cnt_age
from custom_agegrp_munged_enriched_df_view
where agegroup <> 'Children'
group by year, agegroup, custprofession
order by year desc, agegroup, custprofession desc) inline_view 
where avg_custage > 50""")

dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql1.show(2)
#or
custom_agegrp_munged_enriched_df.createOrReplaceTempView("df_view1")
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql2=spark.sql("""
   select year,agegroup,custprofession,
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

print("***************5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization/Flatenning/Widening,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")

#Joins (Interview Question)
# I will give you 2 datasets, 1 with 14 rows another with 13 rows and common data is 11, what is the count on different joins
#inner join    11
#left join   14 (return all 14 from left table and return 11 matching data from right table with values and balance 3 rows without values (nulls))
#right join    13 (return all 13 from right table and return 11 matching data from left table with values and balance 2 rows without values (nulls))
#cross join   182 (14*13 rows)
#full join    27-11=16 (11 common records + 3 left excess + 2 right excess)
#(return all 14 from left table and return all 13 data from right table with values for matching 11 rows and balance 3 & 2 rows without values (nulls))

print("Just learn about Joins with examples")
#Connecting the tables using some common fields -> inner join,left join,right join, full outer, natural join, cross join, self join, anti joins, semi joins
#inner join,left join,right join - (natural(inner) join) - Regularly used joins
#full outer, self join - Rarely used joins
#anti joins, semi joins - Very Rarely used joins (efficient joins)
#cross join/cartisian join - Join to be avoided and used only if it is inevitable, if we don't have a common join column (eg. flight booking)
#'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer',
# 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'

cust_df=filtered_nochildren_rowcol_df_for_further_wrangling1
#DSL Join syntax (Hard relative than SQL syntax)
cust_df.join(enriched_dt_txns,on=[col("id")==col("custid")],how="inner").show()

dim_df1=cust_df.where("id in (4000001,4000002)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))
dim_df2=cust_df.where("id in (4000001,4000003,4000004)").select("id","custage","custfname","agegroup",lit("cust_right_tbl").alias("col1"))

#Syntactically:
#Minimum We have have leftdf.join(rightdt) - Bydefault It will do cross join/cartisian join if no join condition is mentioned(which is not preferred)
dim_df1.join(dim_df2).show()#cross join

#If we mention atleast the one on condition, Bydefault It will do inner join/equi join if atleast one join condition is mentioned(which is preferred)
dim_df1.join(dim_df2,on="id").show()#if common join columns in both dfs
#dim_df1.join(dim_df2,on="id").select("id","custage").show()
#pyspark.sql.utils.AnalysisException: Reference 'custage' is ambiguous, could be: custage, custage.
dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on="id",how="inner").select("dd1.id","dd2.custage").show()
#or
#Use the below syntax also
dim_df1.join(dim_df2,on=[col("id")==col("id")],how="left").show()#if multiple join conditions or no common column names between the dfs use this syntax
#dim_df1.join(dim_df2,on=[col("id")==col("id"),col("custage")==col("custage")]).show()#if multiple join conditions or no common column names use this syntax
#pyspark.sql.utils.AnalysisException: Reference 'id' is ambiguous, could be: id, id

#DSL BEST used syntax is using alias, on condition and how (standard syntax to use) , if multiple join conditions, need to define a particular join type
joineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id"),col("dd1.custage")==col("dd2.custage")],how="inner").\
    select("dd1.*","dd2.*")
joineddf.show()

#symantically
dim_df1=cust_df.where("id in (4000001,4000002)").select("id","custage","custfname","agegroup",lit("cust_left_tbl").alias("col1"))
dim_df2=cust_df.where("id in (4000001,4000003,4000004)").select("id","custage","custfname","agegroup",lit("cust_right_tbl").alias("col1"))

#all these joins of cross, inner, left, right, full, self returns data from both DFs/tables
dim_df1.join(dim_df2).show()#cartisian product-must avoided - 2*3=6
dim_df1.join(dim_df2,on="id",how="inner").show() # 1 matching row
dim_df1.join(dim_df2,on="id",how="left").show() #left all data 2 rows
#If i want to use left join itself to achive the right join's output
dim_df2.join(dim_df1,on="id",how="left").show() #left all data 2 rows
dim_df1.join(dim_df2,on="id",how="right").show() #right all data 3 rows
dim_df1.join(dim_df2,on="id",how="full").show() #left+right - 2 from left + 3 from right (1 common column) = 4 rows returned
dim_df1.join(dim_df1,on="id",how="inner").show() #return same number of matching rows as like inner join #Eg. Self join is Hierarchy retrival of data (Emp data with empid joined with manager id)

#Semi join and Anti join - returns data only from the left DF/table
dim_df1.join(dim_df2,on="id",how="semi").show()#lookup purpose
#I want to see the right data set using semi
dim_df2.join(dim_df1,on="id",how="semi").show()#lookup purpose
dim_df1.join(dim_df2,on="id",how="anti").show()#lookup purpose

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


#cross join/cartisian join - Join to be avoided and used only if it is inevitable
#cross join will multiply the data between 2 dfs for eg. 2*2 =4 records will be returned
crossjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2")).select("dd1.*","dd2.*")
crossjoineddf.show()#If we join 2 dfs/tables without using a join condition then by default cross join will be executed

#SQL Join syntax

#Inner join - It is retrieving only the matched records
innerjoineddf=dim_df1.alias("dd1").join(dim_df2.alias("dd2"),on=[col("dd1.id")==col("dd2.id")],how='inner').select("dd1.*","dd2.*")
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

#Visagan's question, why not we can use subquery to achive this (but semi/anit is good in performance comparing with any other ways to achieve the same result)
#join or in which is better - join is better because of different types available
cust_3custs.createOrReplaceTempView("cust")
txns_3custs.createOrReplaceTempView("trans")
spark.sql("select * from cust where id in (select distinct custid from trans)").show()#this will produce the same semi join result
spark.sql("select * from cust where id not in (select distinct custid from trans)").show()#this will produce the same anti join result

print("b. Lookup & Enrichment (Joins)")
#lookup & enrichment is an activity of enriching the new data with the existing data (leftjoin(preferably), rightouter, innerjoin, fulljoin)
#Transaction data of last 1 month I am considering above
#Returns the details of the customers who did transactions or didn't do the transactions along with what transactions details or for non transactions null values
leftlookupenrichment=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="leftouter")
leftlookupenrichment.select("c.id","c.custprofession","t.*").show(100)
#or
#Returns the details of the customers who did transactions along with what transactions they did
innerlookupenrichment=cust_3custs.alias("c").join(txns_3custs.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")

#We are using the enriched data to get some insight
semilookup.count()#how many customers did transaction today
antisemilookup.count()#how many customers didn't do transaction today
leftlookupenrichment.groupby("id").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction or didn't do the transaction for how much
innerlookupenrichment.groupby("id","custfname").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction for how much

print("Lookup and Enrichment Scenario using SQL")
cust_3custs.createOrReplaceTempView("cust_df_view")
txns_3custs.createOrReplaceTempView("enriched_dt_txns_view")

#above join and aggregation, we are achiveing using ansi SQL
spark.sql("""select c.id,c.custfname,count(c.id) txn_cnt,sum(t.amt) as txn_total_amt
from cust_df_view c inner join enriched_dt_txns_view t
on c.id=t.custid
group by c.id,c.custfname""").show()

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
txns_1month=txns_dtfmt.select("*").where("month(dt)=12 and year(dt)=2011")

#Active Customers (customers did transactions in last 1 month)
active_customers=custom_agegrp_munged_enriched_df.alias("c").join(txns_1month.alias("t"),on=[col("c.id")==col("t.custid")],how="semi")

#Dormant Customers (customers didn't do transactions in last 1 month)
dormant_customers=custom_agegrp_munged_enriched_df.alias("c").join(txns_1month.alias("t"),on=[col("c.id")==col("t.custid")],how="anti")

#Conclusion: Lookup and enrichement can be achieved using - joins, subqueries, filters/where clause

print("c. (Star Schema model-Normalized) Join Scenario  to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business")
cust_dim=custom_agegrp_munged_enriched_df
txns_fact=txns_dtfmt

#benefits (flattened/widened/denormalized data)
#Once for all we do (costly) join and flatten the data and store it in the persistant storage
#Consumers dont have to do join again and again when consumer does analysis, rather just query the flattened joined data
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

print("aa.How to generate seq or surrogate key (scope with in the project) column on the ENTIRE data sorted based on the transaction date")
txns.coalesce(1).withColumn("sno",monotonically_increasing_id()).show(100000)
#montonic id will generate jumping sequence accross the partitions
rno_txns3=txns_3custs.select("*",row_number().over(Window.orderBy("dt")).alias("sno"))
#row_number will coalesce the number of partition to 1 and it will generate sequence in a right sequence without jumping values

print("bb.How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
trans_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy("dt")).alias("transorder"))
print("first, nth made by a given customer")
trans_order_txns3.where("transorder=1").show()#very first trans made
trans_order_txns3.where("transorder=3").show()#initial 3rd transaction customers made
trans_order_txns3.where("transorder<=3").show()#first 3 transactions

print("last, penultimate transaction by a given customer")
trans_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("dt"))).alias("transorder"))
trans_order_txns3.where("transorder=1").show()#very recent trans made
trans_order_txns3.where("transorder=2").show()#last but one transaction customers made
trans_order_txns3.where("transorder<=3").show()#last 3 transactions

print("Least 3 transactions in our overall transactions")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.orderBy("amt")).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top 3 transactions in our overall transactions")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.orderBy(desc("amt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top 3 recent transactions made by the given customer")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder<=3").show()

print("Top transactions by amount made by the given customer 4000000")
#Rank()--Will skip sequence,dense_rank() & row_number()-will not skip seq
#row_number()- will assign different sequence number for even the top same amount and go with the next rownumber (as a continuous number of the total rownumber created)
#Rank()- will assign same rank for the top same amount and go with the next rank (skipping the total ranks)
#dense_rank()- will assign same rank for the top same amount and go with the next rank (as a continuous number of the total ranks)

transamt_order_rno_rnk_drnk_txns3=txns_3custs.where("custid=4000000").select("*",row_number().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtorder"),rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtrnk"),dense_rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("transamtdrnk"))

#transamt_order_rno_rnk_drnk_txns3.where("transamtorder=1").show()#not valid for the given requirement
transamt_order_rno_rnk_drnk_txns3.where("transamtrnk=1").show()#rank

print("Top 2nd transaction amount made by the given customer 4000000")
transamt_order_rno_rnk_drnk_txns3.where("transamtdrnk=2").show()#dense rank

print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of game atleast once")
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid","category").orderBy(desc("dt"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder=1").orderBy("custid").show(22)

#Conclusion of Windowing Functions:
#We can use window function for generating surrogate keys/sequence numbers accross the given data set
#Used for Top 'N' analysis
#Used for Deduplicating the data based on a given attribute

print("E. Analytical Functionalities")
print("What is the purchase patter of the given customer, whether his buying potential is increased or decreased transaction by transaction")
#lag
lag_txns_3custs=txns_3custs.withColumn("prioramt",lag("amt",1,0).over(Window.partitionBy("custid").orderBy("dt")))
#lead
lead_txns_3custs=txns_3custs.withColumn("nextamt",lead("amt",1,0).over(Window.partitionBy("custid").orderBy("dt")))

cust_purchase_patterndf=lag_txns_3custs.select("*",when(col("prioramt")==0,lit('very first trans')).when(col("prioramt")<col("amt"),lit('purchase increased')).otherwise(lit('purchase decreased')).alias("purchase_pattern"))

#DSL Equivalent with window functions
#DSL Equivalent with out using window functions #dropduplicates
#SQL Equivalent

print("d. Windowing Functionalities using SPARK SQL")

print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
txns_3custs.createOrReplaceTempView("txns_3custs_view")
spark.sql("""SELECT *, 
             ROW_NUMBER() over (ORDER BY dt) as surr_key
             FROM txns_3custs_view""").show(4)
print("bb.How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
spark.sql("""SELECT *, 
	     ROW_NUMBER() OVER(PARTITION BY custid ORDER BY dt) as cust_surr_key
	     FROM txns_3custs_view""").show(15)
print("last, penultimate transaction by a given customer")
spark.sql("""SELECT * FROM (
             SELECT *,
	     ROW_NUMBER() OVER(PARTITION BY custid ORDER BY dt DESC) as txnlastorder
	     FROM txns_3custs_view) AS txnlast
             WHERE txnlastorder=1""").show()
print("Least 3 transactions in our overall transactions")
spark.sql("""SELECT * FROM (
	     SELECT *, 
	     ROW_NUMBER() OVER(ORDER BY amt) as txnleastorder
	     FROM txns_3custs_view) AS txnleast
             WHERE txnleastorder<4""").show()
print("Top 3 transactions in our overall transactions")
spark.sql("""SELECT * FROM (
	     SELECT *, 
	     ROW_NUMBER() OVER(ORDER BY amt DESC) as txntoporder
	     FROM txns_3custs_view) AS txntop
	     WHERE txntoporder<4""").show()
print("Top 3 transactions made by the given customer")
spark.sql("""SELECT * FROM (
	     SELECT *, ROW_NUMBER() OVER(PARTITION BY custid ORDER BY amt DESC) as txntopcustorder
	     FROM txns_3custs_view) AS txntopcust
             WHERE txntopcustorder<4""").show()
print("Top transaction amount made by the given customer 4000000")
spark.sql("""SELECT * FROM (
	     SELECT *, 
	     ROW_NUMBER() OVER(PARTITION BY custid ORDER BY amt DESC) as txntopcustrow,
             RANK() OVER(PARTITION BY custid ORDER BY amt DESC) as txntopcustrank,
	     DENSE_RANK() OVER(PARTITION BY custid ORDER BY amt DESC) as txntopcustdense
	     FROM txns_3custs_view) AS txntopcustdense
	     WHERE (txntopcustrow = 1 or txntopcustrank=1 or txntopcustdense=1) 
	     AND custid=4000000""").show()
print("Top 2nd transaction amount made by the given customer 4000000")
spark.sql("""SELECT * FROM (
	     SELECT *, 
	     DENSE_RANK() OVER(PARTITION BY custid ORDER BY amt DESC) as txntopsecdense
	     FROM txns_3custs_view) AS txntopseconddense
	     WHERE txntopsecdense=2 AND custid=4000000""").show()
print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of game atleast once")
spark.sql("""SELECT * FROM (
	     SELECT *, 
	     ROW_NUMBER() OVER(PARTITION BY custid, category 
	     ORDER BY dt DESC) as gamecatdedup
	     FROM txns_3custs_view) AS game_cust_cat
	     WHERE gamecatdedup=1""").show()


print("e. Set Operations ")
#Thumb rules : Number, order and datatype of the columns must be same, otherwise unionbyname function you can use.
print("Common customers accross the city, state, stores, products")
df1=txns_3custs.where("custid=4000000")
df2=txns_3custs
bothdfdatawithduplicates=df2.union(df1)
bothdfdatawithoutduplicates=df2.union(df1).distinct()
bothdfcommondata=df2.intersect(df1)
df2subtracteddf1=df2.subtract(df1)

print("SQL set operations")

print("Aggregation on the joined/Windowed/Analysed/PreWrangled data set")
analysed_aggr1=cust_purchase_patterndf.groupBy("custid","purchase_pattern").agg(count("custid").alias("custcnt"))#customer wise purchase pattern count
leftjoined_aggr2=leftlookupenrichment.groupby("id").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction or didn't do the transaction for how much
innerjoined_aggr3=innerlookupenrichment.groupby("id").agg(count("id").alias("txn_cnt"),sum("amt").alias("txn_total_amt"))#how many customers did transaction for how much

print("SQL Aggregated final dataset")

###########Data processing or Curation or Transformation Starts here###########

print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
#wrangled data
analysed_aggr1.write.mode("append").csv("/user/hduser/analysed_aggr1")
#pre-wrangled data
randomsample1_for_consumption3.mode("overwrite").csv("/user/hduser/randomsample1_for_consumption3")
randomsample2_for_consumption4.mode("overwrite").csv("/user/hduser/randomsample2_for_consumption4")
leftjoined_aggr2.write.mode("overwrite").json("/user/hduser/leftjoined_aggr2")
filtered_adult_row_df_for_consumption1.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="cust_adult",mode="overwrite",properties={"driver": 'com.mysql.jdbc.Driver'})
flattened_denormalized_trans_cust_persist.write.mode("overwrite").saveAsTable("default.cust_trans_denormalized")
masked_custdata=cust_purchase_patterndf.groupBy("custid","purchase_pattern").agg(count("custid").alias("custcnt")).select("purchase_pattern",md5(col("custid").cast("string")).alias("masked_custid"),"custcnt")
masked_custdata.write.mode("overwrite").saveAsTable("default.cust_masked")
#munged data
dedup_dropfillna_clensed_scrubbed_df1.write.mode("overwrite").saveAsTable("default.hive_trans_munged")
print("Spark App1 Completed Successfully")

#Pls get me the technical, functions and the business terminologies used in this 2 programs...