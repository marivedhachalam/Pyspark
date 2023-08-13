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
d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify
b. split, merge/Concat
c. Type Casting & Schema Migration

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

print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
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
#mode- dropmalformed - when there are unclean data (doesn't fit with the structure/columns) don't consider them
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive')
custdf1.printSchema()
custdf1.show(20,False)
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

print("b.3. Schema Evolution/Merging (Structuring) - Merging data with different structure")
'''
BSE~HUL~450.3~-10.0
BSE~CIP~754.62~12.0
BSE~TIT~1000.2~100.0

NYSE~CLI~35.3~1.1~EST
NYSE~CVH~24.62~2~EST
NYSE~CVL~30.2~11~EST
'''
#Merging data with different structure
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr")
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata1/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr","tz")
merge_df=stock_df.withColumn("tz",lit("")).union(stock1_df)#this is a workaround to convert both DFs with same number of columns

stock_df.unionByName(stock1_df,allowMissingColumns=True)#modern and auto way of merging the data

#Merging data with different structure from difference souces
#create table stock(excname varchar(100),stockname varchar(100),stocktype varchar(100));
#insert into stock values('NYSE','INT','IT');
#dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table='stock',properties={'driver':'com.mysql.jdbc.Driver'})
#merge_df_structured=dfdb1.unionByName(stock1_df,allowMissingColumns=True)#structurizing

print("b.3. Schema Evolution (Structuring) - source data (schemamerge) different structure")
spark.read.csv(path=["file:///home/hduser/sparkdata/src1/","file:///home/hduser/sparkdata/src3/"],recursiveFileLookup=True,sep='~',pathGlobFilter="nyse[1-2].csv").count()

stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/","file:///home/hduser/sparkdata/stockdata1/"],sep='~',inferSchema=True,header=True)

stock_df1=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True,header=True)
stock_df2=spark.read.csv(["file:///home/hduser/sparkdata/stockdata1/"],sep='~',inferSchema=True,header=True)
stock_df1.unionByName(stock_df2,allowMissingColumns=True).show()

print("c.1. Validation - DeDuplication")
stock_df.dropDuplicates(["stock"]).show()
stock_df.dropDuplicates(["stock"]).explain()
print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
print("Cleansing (removal of unwanted datasets eg. na.drop, drop malformed)")
print("Dropping the malformed records with all of the columns is null (null will be shown when "
      "datatype mismatch, format mismatch, column missing, blankspace in the data itself or null in the data itself)")

print("Dropping the null records with all of the column is null ")
cstruct = StructType([StructField("id", IntegerType(), False),StructField("custfname", StringType(), False)])
df1=spark.read.csv("file:///home/hduser/sparkdata/file11",schema=cstruct)
df1.na.drop("all",subset=["id","custfname"])
print("Dropping the null records with any one of the column is null ")

print("Dropping the null records with custid is null ")
df1.na.drop("any",subset=["id"])

print("set threshold as a second argument if non NULL values of particular row is less than thresh value then drop that row")
df1.na.drop("all",subset=["id","custfname"],thresh=2)
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
df1.na.fill("x",subset=["id","custfname"])
print("Replace the key with the respective values in the columns "
       "(another way of writing Case statement)")
dic={"x":"X"}
df1.na.replace(dic,subset=["id","custfname"])

#Writing equivalent SQL
#dfcsva.createOrReplaceTempView("dfcsvview")


print("Current Schema of the DF")
#dfcsvc.printSchema()
print("Current Schema of the tempview DF")
#dfcsvcleanse.printSchema()

print("d.1. Standardization - Column name re-order/number of columns changes/Replacement & Deletion of columns to make it in a usable format")
df1.withColumnRenamed("id","id1").select("custfname","id1").withColumn("custfname",current_date()).drop("id1").show()
print("********************data munging completed****************")

print("*************** Data Enrichment -> Add, Rename, Concat, Split, Casting of Fields, Reformat, derivation/replacement of values *********************")
df1.withColumnRenamed("id","id1").select("custfname","id1").withColumn("custfname",current_date()).drop("id1").show()


print("*************** Data Customization & Processing -> Apply User defined functions and reusable frameworks *********************")
print("*************** Data (Pre wrangling) Curation/Processing, Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization *********************")
print("*************** Data Wrangling -> Lookup, Join, Enrichment *********************")
print("*************** Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")
