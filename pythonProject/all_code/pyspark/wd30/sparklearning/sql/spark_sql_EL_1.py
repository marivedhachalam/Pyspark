#Very High level (Bread & Butter1)
#Documentation Reference:
#https://spark.apache.org/docs/3.1.2/sql-data-sources-json.html
#/usr/local/spark/python/pyspark/sql/readwriter.py

#How we are going to learn Spark SQL (Ingestion/ETL/ELT):
#HOW to write a typical spark application

#spark_sql_EL_1.py start
#1. how to create dataframes from RDD using named list/reflection (Row object) (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#2. how to create DF from different (builtin) sources by inferring the schema automatically or manually defining the schema (very important)
#3. how to store the transformed data to targets(builtin)
#spark_sql_EL_1.py end

#spark_sql_ETL_2.py start
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)
#above and beyond
#6. how to standardize the code or how create generic/reusable frameworks (level 3) - masking engine, reusable transformation, data movement automation engine, quality suite, audit engine, data observability, data modernization...
#spark_sql_ETL_2.py end

#7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
#8. performance tuning
#9. Deploying spark applications in Cloud
#10. Creating cloud pipelines using spark SQL programs

#HOW to write a typical spark application

from pyspark.sql.session import SparkSession
spark=SparkSession.builder.master("local[2]").appName("WD30 Spark Core Prog learning").enableHiveSupport().getOrCreate()
sc=spark.sparkContext
#sqlContext=SQLContext(sc)
sc.setLogLevel("ERROR")
####1. creating DF from rdd - starts here######
#1. (not much important) how to create (representation) dataframes from RDD using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#How to convert an rdd to dataframe?Not so much important, because we don't create rdd initially to convert to df until it is in evitable
#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter,
rdd1=sc.textFile("file:///home/hduser/sample.dat").map(lambda x:x.split(","))
#2. Iterate on every splitted elements apply the respective, datatype to get the SchemaRDD
schema_rdd2=rdd1.map(lambda x:(int(x[0]), x[1], int(x[2])))
#filter_rdd=schema_rdd2.filter(lambda x:x[2]>100000)
#print(filter_rdd.collect())#rdd programming
#created using reflection
#schema_rdd2.collect()
#create dataframe further, to simplify the operation performed on the data
#3. Create column list as per the the structure of data
collist=["id","name","sal"]
#from pyspark.sql import Row
#row_rdd=rdd1.map(lambda cols:Row(id=int(cols[0]), name=cols[1], sal=int(cols[2])))
# 4. Convert the schemaRDD to DF using toDF or createDataFrame apply the column names by calling the column lists
df1=spark.createDataFrame(schema_rdd2,collist)#this createDF will convert the schemardd --> row_rdd (internally) --> DF
#or
df1=schema_rdd2.toDF(collist)
#[Row(id=1, name='irfan', sal=100000), Row(id=2, name='inceptez', sal=200000)]
#or alternatively, create row object directly and run dsl queries
#using concept of reflection (reflect each row of the schema rdd to the Row class to get the row objects created)

from pyspark.sql.types import Row
rdd1=sc.textFile("file:///home/hduser/sample.dat").map(lambda x:x.split(","))
row_rdd1=rdd1.map(lambda cols:Row(id=int(cols[0]), name=cols[1], sal=int(cols[2])))#if we reprepresent data from notepad to xls (colname and dtype)
df_rowrdd=spark.createDataFrame(row_rdd1)
#df1=spark.read.csv("file:///home/hduser/sample.dat")# all the above 4 steps are done by this one function (csv)
df1.select("*").where ("sal >100000").show()#dsl (spark domain specific language) query
df_rowrdd.select("*").where ("sal >100000").show()#dsl (spark domain specific language) query

#Rather writing the code like above (either schemardd+collist or rowrdd to DF) (create rdd then converting to DF),
# preferable write the code (directly create DFs) as given below
df1=spark.read.option("inferschema","true").csv("file:///home/hduser/sample.dat").toDF("id","name","sal")
df1.select("*").where ("sal >100000").show()#dsl programming

df1.createOrReplaceTempView("sample_view")#sql programming (familiar)
spark.sql("select * from sample_view where sal>100000").show()

#A typical example for forcefully creating rdd then df then temp view query.
#1. create an rdd to convert the data into structured from unstructured
#2. represent rdd as dataframe
#3. write dsl queries
#or
#4. represent df as tempview (since sql is simple and more familiar)
#5. write sql queries
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).map(lambda x:(x,))#functional transformation
collst=["coursename"]
#we cant avoid creating rdd, hence create rdd and represent as DF (ASAP)
df1=spark.createDataFrame(rdd1,collst)
df1.select("coursename").distinct().show()#dsl (declarative)
df1.createOrReplaceTempView("course_view")
spark.sql("select distinct coursename from course_view").show()#sql (declarative)

#named_col_list + schemaRdd -> toDF/createDataFrame
#Row + schemaRdd = RowRDD (Reflection) -> toDF/createDataFrame
####1. creating DF from rdd - ends here######

#How to write a typical python based spark application??
#hierarchy of writing code in python? pkg.subpkg.module.class.methods/function #framework developers have did this
#What we are going to do as a developer - pkg.subpkg.module.main(method).programs to run (leverage the classes/function developed already) (instantiating the class and use the functions)

#2. how to create DF from different sources (using different options) by inferring the schema automatically or manually defining the schema (very important)
####creating DF directly from different sources - starts here (very important)######
#when we create spark session object in the name of spark (it comprise of sparkcontext, sqlcontext, hivecontext)
#where we need spark context - to write rdd programs (manually or automatically)
#where we need sql context - to write dsl/sql queries
#where we need hive context - to write hql queries
#sc=spark.sparkContext#for spark sql program, sc is indirectly used

#creating DFs directly
#1. spark session object importance
#2. The methods with options we can use under read and write module/method of the spark session
#3. csv module important option -> (option/options/inline arguments/format & load) inferschema, header, delimiter/sep
df1=spark.read.csv("file:///home/hduser/sample.dat")#indirectly calling sqlContext that inturn wraps sparkContext
df1.select("*").show()#column names starts with _c0, _c1... with default delimiter , with default datatype String

#2. Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table) / external (cloud, api, nosql))
# or programatically
#****Possible way of calling the csv function with different options
#option(opt1,value1), options(opt1=value,opt2=value), csv(filename,opt1=value,opt2=value), format(csv).option/options.load("file")
delfile_df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with default comma delimiter
delfile_df2=spark.read.option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with custom ~ delimiter
print(delfile_df1.printSchema())#column names will _c0,c1 and datatype string
#multiple options to call the default functions like csv
delfile_df3=spark.read.option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with custom ~ delimiter and automatically infer schema (dataype)
#or if we have multiple options to be used, then go with the below ways
delfile_df4=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse.csv")
delfile_df5=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep='~',inferSchema=True)

#the below way of creating df is (not suggested) for the built in modules such as csv, json, orc ... legacy approach
delfile_df6=spark.read.format("csv").options(delimiter="~",inferschema=True).load("file:///home/hduser/sparkdata/nyse.csv")
#column names will _c0,c1 and datatype automatically inferred String, String, double

# How to define column names for the dataframe at the time of creation or how to change the column names (later)
#if header is not available, i will go simply toDF("column list")
delfile_df7=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep='~',inferSchema=True).toDF("exchange","stock","value")
#column names will "exchange","stock","value" and datatype inferred automatically
delfile_df7.printSchema()

#if header is available, i will go simply with header=true
delfile_df8_header=spark.read.option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_header.printSchema()#single column

#if i dont mention header or header,false?
delfile_df8_1_header=spark.read.option("header","false").option("inferschema","true").option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_1_header.printSchema()#
#it consider the first row and then assume the remaining also as the same
#it wont consider the first line as header, rather as a data
#column names will not be considered, so default _c0,_c1 .. will be used

#or
#preference 2 if one/two regular option are going to be used
delfile_df8_header=spark.read.option("header","true").option("inferschema","true").option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_header.printSchema()#multiple columns
#or
#preference 1 if regular options are going to be used
delfile_df9_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df9_header.printSchema()#multiple columns
#or
#preference 1 if additional options are going to be used
delfile_df10_header=spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv",header="true",inferSchema="true",sep="~")
delfile_df10_header.printSchema()#multiple columns
#or
#preference 3 if other sources we are going to use
delfile_df11_header=spark.read.format("csv").options(header="true",inferschema="true",delimiter="~").load("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df11_header.printSchema()#multiple columns

#if header is available in the data, but need to change MOST of the column names
#toDF(colname1,colname2)
#exchange_name~stock_name~closing_rate
#NYSE~CLI~35.3
#NYSE~CVH~24.62
#NYSE~CVL~30.2
delfile_df12_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df12_header.printSchema()
#exchange_name~stock_name~closing_rate -> exchange~stock~rate
delfile_df12_1_header=spark.read.options(header="true",inferschema="true",delimiter="~")\
    .csv("file:///home/hduser/sparkdata/nyse_header.csv").toDF("exchange","stock","rate")
delfile_df12_1_header.printSchema()

#if header is available but need to change few of the column names
#withColumnRenamed
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df13_header.printSchema()
delfile_df13_1_withColumnRenamed=delfile_df13_header.withColumnRenamed("exchange_name","exch")#to rename one
delfile_df13_1_withColumnRenamed.printSchema()
delfile_df13_2_withColumnRenamed=delfile_df13_header.withColumnRenamed("exchange_name","exch").withColumnRenamed("closing_rate","close_rate")#to rename one
delfile_df13_2_withColumnRenamed.printSchema()
#alias
from pyspark.sql.functions import col
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df13_1_alias=delfile_df13_header.select(col("exchange_name").alias("exch"),"stock_name",col("closing_rate").alias("close_rate"))#DSL #to rename one or more columns
delfile_df13_1_alias.printSchema()
#or
#convert df to tempview and write ansi sql
delfile_df13_header.createOrReplaceTempView("view1_header")
delfile_df13_2_alias=spark.sql("select exchange_name exch,stock_name,closing_rate as close_rate from view1_header")#familiar sql
delfile_df13_2_alias.printSchema()

#If header is available need to change few column name after applying some transformations/aggregations then use .alias()
#do aggregation
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
aggr_df14_1_alias=delfile_df13_header.groupby("exchange_name").agg(min(col("closing_rate")).alias("min_close_rate"))#dsl way
#aggr_df14_1_alias=delfile_df13_header.groupby("exchange_name").agg(min(col("closing_rate"))).withColumnRenamed("min(closing_rate)","min_close_rate")#dsl way
aggr_df14_1_alias.show()#alias is used just for proper column name definition
#We call this renaming as STANDARDIZATION
aggr_df14_1_alias.write.saveAsTable("hive_tbl11")#alias is mandatory for the column name definition as per hive table columname standard
#If we want to save the df in hive, then Alias/renaming of the column is mandatory - provided if the column names are not as per the hive standard

#If header is available need to change few column name after applying some transformations/aggregations then use SQL functions also
delfile_df13_header.createOrReplaceTempView("view1")
df_to_hive_json_orc=spark.sql("select exchange_name,min(closing_rate) as min_close_rate from view1 group by exchange_name")#ISO/ANSI SQL
df_to_hive_json_orc.write.saveAsTable("hive_tbl12")
df_to_hive_json_orc.write.json("file:///home/hduser/narashimanjson2")
df_to_hive_json_orc.write.orc("file:///home/hduser/narashimanorc")

#****very very important item to know for sure by everyone...
#I don't wanted to infer the schema (columname & datatype & null (constraint)), rather I want to define the custom schema?

#performance optimization -
# Don't use inferschema unless it is inevitable like the given example below
#If header is not available need to define column names and the DATATYPE also - we can simply go with inferschema and toDF to achieve it,
#provided if the volume of data is less , but using inferschema is always not a preferred/optimistic solution
#inferschema="true",
#For Eg (Explain like a story): I did an optimization in my company, where our developers created DF with inferschema mostly
# (because initially we don't had much data from the sources), subsequently the performance issue came, when analysing, i found the inferschema is used
# I fixed it for the applicable DFs converting to custom schema using structtype and structfield classes.
df14_noheader_inferschema=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse.csv").toDF("exch_name","stock_name","closed_rate")
#inferschema - must be avoided to use in the dataframe creation
#we can avoid scanning the whole data to infer the schema - read all records and identify the data type
#applies some default datatype like double, which will not allow me to define cust datatypes (It may leads to incorrect datatype)
#When do we use inferschema then -
#           If the volume of data is small and number of columns are more
#           If we are good with the format what inferschema produces, then we can use.
#           For Testing, data validation, allowing all column values to be identified (eg. do we have any non integer type in integer column)
df14_noheader_inferschema1=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse_vinoth.csv").toDF("exch_name","stock_name","closed_rate")
df14_noheader_inferschema1.where("upper(closed_rate)<>lower(closed_rate)").show()
#inferschma indentifies entire COLUMN as STRING even if one column in a record is number
#inferschma indentifies entire column as string even if one record with the given integer column is string

#If header is not available need to define column names and the DATATYPE also - we need to go with custom schema, use schema option to achieve this
#Structure Type & Structure Fields
from pyspark.sql.types import StructType,StructField,StringType,FloatType#better way
#from pyspark.sql.types import *#not preferred way
#format for defining structure
#strct=StructType([StructField("col1name",DataType(),False),StructField("col2name",DataType(),True),StructField("col3name",DataType(),False)])
strct=StructType([StructField("exch_name",StringType(),False),StructField("stock_name",StringType()),StructField("close_rate",FloatType())])
df14_noheader_customschema=spark.read.schema(strct).options(delimiter="~").csv("file:///home/hduser/sparkdata/nyse.csv")
df14_noheader_customschema.printSchema()
df14_noheader_customschema.show()

#If header is available need to define column name and the DATATYPE also - we need to go with custom schema, use schema option to achieve this
from pyspark.sql.types import StructType,StructField,StringType,FloatType#better way
strct=StructType([StructField("exch_name",StringType(),False),StructField("stock_name",StringType(),True),StructField("close_rate",FloatType(),True)])
df15_header_customschema=spark.read.schema(strct).options(header=True,delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df15_header_customschema.printSchema()
df15_header_customschema.show()

#I have only few columns (5) only to process, do you want to create structtype for all the columns (100 columns) in the given source data?
df16_choose_minimal_columns=spark.read.csv("file:///home/hduser/hive/data/txns").select(col("_c0").alias("txnno").cast("int"),col("_c3").alias("amt").cast("float"),col("_c6").alias("state"))
df16_choose_minimal_columns.printSchema()
df16_choose_minimal_columns.show()

#or

#can we use schema option here? not suggested, because the above select itself can achieve it
df17_choose_minimal_columns=spark.read.csv("file:///home/hduser/hive/data/txns").select(col("_c0").cast('int'),col("_c3").cast("float"),"_c6")
df17_choose_minimal_columns.printSchema()
df17_choose_minimal_columns.show()
from pyspark.sql.types import *
strct1=StructType([StructField("txnno",IntegerType(),False),StructField("amount",FloatType(),True),StructField("state",StringType(),True)])
newdf18_structapplied=spark.createDataFrame(df17_choose_minimal_columns.rdd,strct1)
newdf18_structapplied.printSchema()
newdf18_structapplied.show(2)

#performance optimization - PROJECTION PUSHDOWN
# Don't choose all the columns until it is needed, Specify only the needed columns, because spark can project only the columns what is needed to be loaded in memory - PROJECTION PUSHDOWN
#For Eg (Explain like a story): I did an optimization in my company, where our developers created DF with all columns in most of the programs,
# (because initially they don't know what are the columns we are going to use in the program), subsequently the performance issue came, when analysing,
# i realized and modified the code to choose only the necessary columns

####Completed the csv module + important common methodologies of creating dataframe
# (spark.read.module, format().load(), option, options, inline args, schema (structtype,structfield etc.,), header,
# sep,withcolumnrenamed, col(), alias, Row, toDF, createDataframe, peformance optimization, cast, inferschema, delimiter,
# some libraries such as pyspark.sql.functions/types, what is dsl/tempview/select)

#Next important source for the data ingestion is RDBMS
#Reading/importing & Writing/exporting the data from Databases (Venkat's usecase)
#Traditionally we are using
# Oracle -> sqoop import -> HDFS -> HIVE(RAW) -> ETL (HQL) -> HIVE(CURATED) -> EXPORT TOOL(Zaloni)/Sqoop export -> Oracle
#Modernization (Legacy(MF/DBs/DWHs/FSs) Migration/Conversion -> Bigdata Modernization(traditional -> spark -> cloud))
# Oracle -> sqoop import -> HDFS -> HIVE(RAW) -> ETL (HQL) -> HIVE(CURATED) -> EXPORT TOOL(Zaloni) -> Oracle
#Modernization
# Why we have to do this ETL operation using spark/traditional bigdata tools rather using Oracle itself?
#1. For reducing the workload of Oracle DB because it is meant for OTLP/Traditional ETL and not for Complex ETL on the large/growing dataset.
#2. We wanted to integrate/converge data between ORACLE & BD platform, like oracle data with HDFS data/Hive data (Lookup and Enrichment)
#3. We wanted to complex ETL and persist the data in the Bigdata platform, because the tradition users use the data in Oracle and Analytical/power
# users use the data in BigData platform like Hive
#I need to reduce the coding & maintainance complexity of using traditional tools like sqoop, hive, zaloni etc.,
#I don't wanted the data to be persisted in the persistant storage like hdfs/hive etc.,
#Why we are bringing the data to Spark Cluster rather performing the ETL in DB itself, Lookup and
# Oracle Singapore (DBEngine memory) -> spark (US) import -> Executors(memory) -> Spark Lookup & Enrichment(DSL/SQL) ETL -> Spark Export -> Oracle/TD/SQLServer
#                                                                                 Spark (Aggregation/Summarization) ETL -> Spark Hive -> Hive -> BigData Power/Analytical Users

#1. Oracle Singapore (DBEngine memory) -> spark (US) import -> Executors(memory) -> Spark Lookup & Enrichment(DSL/SQL) ETL -> Spark Export -> Oracle/TD/SQLServer
#Reading/importing the data from Databases (E-extraction)
complete_url='jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$'
table_name='customer'
driver_program='com.mysql.jdbc.Driver'
dbdf1=spark.read.jdbc(url=complete_url,table=table_name,properties={'driver':driver_program})
#dbdf2=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/custdb").option("dbtable","customer").option("user","root").option("password","Root123$").option("driver","com.mysql.jdbc.Driver").load()
dbdf1.printSchema()
dbdf1.show()

#1. For reducing the workload of Oracle DB because it is meant for OTLP/Traditional ETL and not for Complex ETL on the large/growing dataset.
#2. We wanted to integrate/converge data between ORACLE & BD platform, like oracle data with HDFS data/Hive data (Lookup and Enrichment)

#2. Oracle Singapore (DBEngine memory) -> spark (US) import -> Executors(memory) -> Spark (Aggregation/Summarization) ETL -> Spark Hive -> Hive -> BigData Power/Analytical Users
#T - Transformation-> Lookup & Enrichment, filteration, conversion
filedf1=spark.read.csv("file:///home/hduser/hive/data/states.csv",inferSchema=True).toDF("city","state")
filedf1.show()
dbdf1.createOrReplaceTempView("dbview")
filedf1.createOrReplaceTempView("fileview")
lookup_enriched_df=spark.sql("select d.*,upper(f.state) as state from dbview d left join fileview f on d.city=f.city where d.city is not null")

#L - Load into RDBMS target
table_name2='customer_enriched'
lookup_enriched_df.write.jdbc(url=complete_url,table=table_name2,properties={'driver':driver_program},mode="append")

#3. We wanted to complex ETL and persist the data in the Bigdata Datalake platform, because the tradition users use the data in Oracle and Analytical/power
# users use the data in BigData platform like Hive
#T - Transformation-> Aggregation or Summarization
hive_summary_df=spark.sql("""select city,age,max(createdt) as max_create_dt,sum(transactamt) as sum_trans_amt 
                             from dbview 
                             group by city,age""")
hive_summary_df.show(2)
from pyspark.sql.functions import *
hive_summary_df=dbdf1.groupby("city","age").agg(max("createdt").alias("max_create_dt"),sum("transactamt").alias("sum_trans_amt"))
hive_summary_df.show(2)
#Writing/Reading the data into Hive Table
#1. Writing DF into Hive Directly
hive_summary_df.write.saveAsTable("default.hive_cust_summary")#If managed table is not present, then create and load
hive_summary_df.write.mode("overwrite").saveAsTable("default.hive_cust_summary")#if the table present, it will either allow me to append or overwrite in parquet format
#db data --> aggregating -> data frame --> hive table
#use the saveastable option if we are going to consume this hive data by SPARK itself subsequently

#Interview Question?
# can we tranfer data between one spark app to another ? not achievable directly, but by persisting the app1 data to fs/hive/db table in a serialized format (parquet)
# raw(csv/rdbms) -> DE curation (saveastable)-> parquet+snappy-hive table -> new spark app DS model -> hive table (consumption) -> visualization
# raw(csv/rdbms) -> DE curation (HQL)-> text/orc/parquet-hive table ->  HQLs/Visualization

#or
#2. Writing Tempview data into Hive table (HQL)
hive_summary_df.createOrReplaceTempView("hive_db_view")
#spark.sql("create table default.hive_cust_summary as select * from hive_db_view")#CTAS also can be done
spark.sql("insert overwrite table default.hive_cust_summary select * from hive_db_view")#If we need to write data into hive using SQL
#or
spark.sql("""create table if not exists default.hive_cust_summary_1(city string,age int,max_create_dt date,sum_trans_amt int) 
row format delimited fields terminated by ',' """)#my own formatted table
spark.sql("""insert overwrite table default.hive_cust_summary_1 select * from 
                                                            (select city,age,max(createdt) as max_create_dt,sum(transactamt) as sum_trans_amt 
                                                                from dbview 
                                                            group by city,age) t1""")#Inline view
#db data --> data frame --> spark temp view --> summary inlineview --> hive table
#We use HQL rather than the spark built in functions if the hive data is consumed by the Datalake HIVE users
#or

#Interview Question? Can you use spark to create and store hive external table?
spark.sql("""create external table if not exists hive_cust_summary_ext (city string,age int,max_create_dt date,sum_trans_amt int) 
row format delimited fields terminated by ','
location '/user/hduser/hive_cust_summary_ext'""")
spark.sql("""insert overwrite table hive_cust_summary_ext select * from 
                                                            (select city,age,max(createdt) as max_create_dt,sum(transactamt) as sum_trans_amt 
                                                                from dbview 
                                                            group by city,age) t1""")#Inline view

#Interview Question? Can you use spark to create and store partitions in hive table?
query="(select * from customer_upd where upddt='2023-04-11')t"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=query,properties={'driver':'com.mysql.jdbc.Driver'})

dfdb1.write.mode("overwrite").partitionBy('upddt').saveAsTable("hive_cust_summary_part")
#In the above saveastable function all the partitions are overwritterned, hence going with Hql option

#The DB Data I am going to write/persist into hive external partitioned table
dfdb1.createOrReplaceTempView("dbqueryview")
spark.sql("""create external table if not exists hive_cust_summary_ext_part_2 
(custid int, firstname string, lastname string,city string,age int, createdt date,transactamt int) partitioned by (upddt date)
row format delimited fields terminated by ','
location 'hdfs:///user/hduser/hive_cust_summary_ext_part_2'""")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
#spark.sql("insert into table hive_cust_summary_ext_part partition(upddt) select * from dbqueryview")
#spark.sql("alter table hive_cust_summary_ext_part_2 drop  if exists partition (upddt='2023-04-11')")#static
sc.setLogLevel("ERROR")
spark.sql("insert overwrite table hive_cust_summary_ext_part_2 partition(upddt) select * from dbqueryview")
#dfdb1.write.mode("overwrite").partitionBy('upddt').saveAsTable("hive_cust_summary_ext_part")
#dfdb1.write.mode("overwrite").saveAsTable("default.hive_cust_summary")

#Interview Question? Can you use spark to create and store buckets in hive table?
#Spark upto current version, doesn't support hive bucketing
#but spark support spark bucketing to load hive table (Spark currently populate bucketed output which is compatible with Spark bucketin.)

spark.sql("""create external table if not exists hive_cust_summary_ext_buck 
(custid int, firstname string, lastname string,city string,age int, createdt date,transactamt int,upddt date) 
clustered by (custid) into 3 buckets
row format delimited fields terminated by ','
location 'hdfs:///user/hduser/hive_cust_summary_ext_buck'""")
spark.sql("insert into table hive_cust_summary_ext_buck select * from dbqueryview")#Native buckets
#pyspark.sql.utils.AnalysisException: Output Hive table `default`.`hive_cust_summary_ext_buck` is bucketed but Spark currently does NOT populate bucketed output which is compatible with Hive.

dfdb1.write.bucketBy(3,"custid").saveAsTable("hivebuckettable")#spark buckets algorithm, works

#Reading Data from Hive Table
#2. Reading data from Hive table as a DF
dfhive1=spark.read.table("default.hive_cust_summary")#reading the data from hive, was created by spark earlier using saveastable option
#2. Reading data from Hive table using SQL
spark.sql("select * from default.hive_cust_summary").show()

dfdb1.createOrReplaceTempView("dbview")
hive_summary_df=spark.sql("""select city,age,max(createdt) as max_create_dt,sum(transactamt) as sum_trans_amt 
                             from dbview 
                             group by city,age""")
hive_summary_df.write.insertInto("default.hive_cust_summary",overwrite=True)
#If managed table is not present, insertinto will not create, Insertinto will write the data to hive as insert statements
#applicable more for small volume of data

##################Hive Read and write using spark native functions and HQL queries is completed#

#reading/writing the df into different file formats
#Writing of data into different file formats
#spark sql can handle semi structured data
hive_summary_df.write.json("/user/hduser/json_dataset",mode='overwrite',compression='gzip')
#performance optimization:
# Receive and read the data in compressed fashion & Write and transfer the data to the downstream system by compressing
# Benchmarking -
# Try with different options for transferring he data eg. sftp (multipart), distcp, different compression techniques, read without uncompressing in Spark

#interview question - want to write the df data in a single json file to share it to our consumers
df1.coalesce(1).write.format("json").mode("overwrite").option("compression",'gzip').save("/user/hduser/json_dataset")

#Multiple options to Explore
dfdb1.write.json("/user/hduser/json_dataset",mode='overwrite',lineSep=',',ignoreNullFields=True)

#reading json data from remote cluster
jsondf1=spark.read.json("hdfs://127.0.0.1:54310/user/hduser/json_dataset/")
jsondf1.createOrReplaceTempView("jsonview")
spark.sql("select count(distinct city) from jsonview").show()
jsondf1.show()
#remote cluster (50gb data compressed lfs) -> Revathi's cluster(spark operation)
#remote cluster (50gb data compressed lfs) -> hdfs -> spark (hdfs) -> Revathi's cluster(spark executor)

#Serialization common Features -> Fast(storage, transfer, processing), Compact, Interoperable, Secured, Extensible, Intelligent Encoding

#performance optimization
######## Top Features of ORC & Parquet #############
#1. Intelligent storage format (indexes, internal parquet chunks of partitions(orc strides), Encoded format - range, run length encoding and dictionary encoding)
#pull only the given row using index identified in a given partition (blocks) (predicate)
#transfer the data in the network only what is needed
#2. Row/Column oriented fashion - Store, Read and process (aggregaion, grouping) only what ever the column and row we needed (PDO optimization - predicate pushdown, projection pushdown)
#pull only the column what i needed (project)
#select sum(amount) - fetch only amount column (orc/parquet) and don't fetch other columns unlike databases
#3. Compression (Snappy) will be more optimistic.
#apply specific type of compression on the specific columns because of column oriented.
#compression -> 1,2,3,4,5,6 (column)
#compression -> 1,irfan,2010-10-01 (row)
#writing/reading orc data
dfdb1.coalesce(1).write.format("orc").mode("overwrite").save("/user/hduser/orc_dataset")

orcdf1=spark.read.orc("hdfs://127.0.0.1:54310/user/hduser/orc_dataset/")
orcdf1.createOrReplaceTempView("orcview")
spark.sql("select count(1) from orcview").show()
#or
#performance optimization - project only what you needed (columnar storage)
spark.sql("select count(distinct city) from orc.`/user/hduser/orc_dataset/`").show()

#Writing parquet data
dfdb1.coalesce(1).write.format("parquet").mode("overwrite").save("/user/hduser/parquet_dataset")
dfdb1.write.partitionBy("city").parquet("/user/hduser/parquet_dataset",mode='overwrite')

#Reading parquet data
parquetdf=spark.read.parquet("/user/hduser/parquet_dataset")
parquetdf.show()
spark.sql("select count(distinct city) from parquet.`/user/hduser/parquet_dataset`").show()

#hive usecase -
# usecase1: raw(csv) -> DE curation -> curated hive table -> new spark app DS model -> analytical hive table -> visualization
#DE team will do the below steps in spark application 1
from pyspark.sql.functions import *
df_csv1=spark.read.option("header","True").option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")
# approach1 for handling usecase1 is load the csv data in a df -> apply transformations -> saveAsTable() -> DS app will use spark.read.table()
df_transformed=df_csv1.withColumn("stock",lower(col("stock"))).withColumn("numeric_closerate",col("closerate").cast("int")).withColumn("curdt",current_date())
df_transformed.write.mode("overwrite").saveAsTable("hive_curated_table_for_DS")

#ds team is complaining the query is not performing well, so DE team stores the data applying partitions
df_transformed.write.partitionBy("stock").saveAsTable("hive_curated_table_for_DS_part1")
datascience_part_df1=spark.read.table("hive_curated_table_for_DS_part1")
ds_df2=datascience_part_df1.where("stock='hul'").dropDuplicates().na.drop("any")#if null is ther in any of the columns, i am dropping those rows

#DS team will do the below steps in spark application 2
datascience_df1=spark.read.table("hive_curated_table_for_DS")
ds_df2=datascience_df1.dropDuplicates().na.drop("any")
ds_df2.write.saveAsTable("hive_final_table_for_visualization")

# approach2 for handling usecase2 is for creating external tables and to make my end users leverage all hql functionalities,
# which will not be supported
# by the spark saveAsTable function
# load the csv data in a df ->convert the DF into Tempview ->  apply transformations using sql->
# spark.sql("create table as select"/"insert select")
# -> users may analyse the data in hive cli as an external table  or new spark app (DS model) -> hive table -> visualization
df_csv1=spark.read.option("header","True").option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")
df_csv1.createOrReplaceTempView("df_csv1_tv")
spark.sql("create table hive_curated_table_for_DS1 as "
          "select exchange,lower(stock) as stock,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt "
           "from df_csv1_tv")
# or if the target already present, use the below way..
#spark.sql("create table hive_curated_table_for_DS2 (exchange string,stock string,closerate decimal(10,2),numeric_closerate int,curdt date)")
spark.sql("insert overwrite table hive_curated_table_for_DS2 "
          "select exchange,lower(stock) as stock,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt "
           "from df_csv1_tv")

spark.sql("create table hive_curated_table_for_DS_part_hive (exchange string,closerate decimal(10,2),numeric_closerate int,curdt date) partitioned by (stock string)")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table hive_curated_table_for_DS_part_hive partition(stock)"
          "select exchange,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt ,lower(stock) as stock "
           "from df_csv1_tv")

# Important interview question? Can we create external table in hive using spark? if so how to create?
#yes, by only using the hive native sql and not by using spark dsl functions
spark.sql("create external table ext_hive_curated_table_for_DS_part_hive (exchange string,closerate decimal(10,2),"
          "numeric_closerate int,curdt date) partitioned by (stock string) location 'hdfs://localhost:54310/user/hduser/ext_hive_curated_table_for_DS_part_hive'")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table ext_hive_curated_table_for_DS_part_hive partition(stock)"
          "select exchange,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt ,lower(stock) as stock "
           "from df_csv1_tv")



