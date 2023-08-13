# Very High level
# How we are going to learn Spark SQL (Ingestion/ETL/ELT):
# HOW to write a typical standard spark application - hope we understood
# 1. don't write the code directly in the module, rather keep all the code in the main()
# 2. check for special attribute __name__=='__main__" -> main()
# 3. or check for number of attributes __name__=='__main__" -> check for the number of args using sys.argv -> main(sys.argv) -> consume the args in the main()
'''
vi sample_py.py
import sys
def main(args):
 print(args)
if (__name__=="__main__"):
 if (len(sys.argv)>=2):
  main(sys.argv)
 else:
  print("Pass minimum 1 argument to the python script")

$python3 sample_py.py hello
'''

# spark_sql_EL_BreadButter_1.py start
# 1. not important how to create dataframes from RDD using named list/reflection (Row object) (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
# 2. IMPORTANT how to create DF (directly) from different (builtin)sources by inferring the schema automatically,colnames,headers,delimiters.. or manually defining the schema (very important)
# 3. important how to store the transformed data to targets(builtin)
# spark_sql_EL_BreadButter_1.py end

# spark_sql_ETL_BreadButter_2.py start
# 4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
# 5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)
# above and beyond
# 6. how to create generic/reusable frameworks (level 3) - masking engine, reusable transformation, data movement automation engine, quality suite, audit engine, data observability, data modernization...
# spark_sql_ETL_BreadButter_2.py end

# 7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
# 8. performance tuning
# 9. Deploying spark applications in Cloud/ONPREM
# 10. Creating cloud pipelines using spark SQL programs

# HOW to write a typical spark application
# refer the prgram spark_sql_standard_application.py

# sqlContext=SQLContext(sc)

####1. creating DF from rdd - starts here######
# 1. (not much important) how to create (representation) dataframes from RDD using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
# How to convert an rdd to dataframe?
# Not so much important, because we don't create rdd initially to convert to df until it is in evitable
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("We41-Job").getOrCreate()
sc = spark.sparkContext  # renmed the sparkContext in the name of sc
sc.setLogLevel("ERROR")

# How to create dataframe from RDD using named list concept
# 1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter '~'
rdd1 = sc.textFile("file:///home/hduser/sparkdata/nyse.csv")
rdd2 = rdd1.map(lambda x: x.split("~"))

# 2. Iterate on every splitted elements apply the respective datatype to get the SchemaRDD
schemardd2 = rdd2.map(lambda x: (x[0], x[1], float(x[2])))
# schema rdd [('NYSE', 'CLI', 35.3), ('NYSE', 'CVH', 24.62), ('NYSE', 'CVL', 30.2)]

# create dataframe further, to simplify the operation performed on the data
# 3. Create column list as per the the structure of data
col_list = ["exchange", "stock", "amt"]

# 4. Convert the schemaRDD to DF using toDF or createDataFrame apply the column names by calling the column lists
df1 = spark.createDataFrame(schemardd2, col_list)
df2 = schemardd2.toDF(col_list)
df2.rdd.collect()  # this createDF will convert the schemardd --> row_rdd (internally) --> DF

# or

# How to create Dataframe from RDD using reflection (Row object) - not much important
# [Row(id=1, name='irfan', sal=100000), Row(id=2, name='inceptez', sal=200000)]
# using concept of reflection (reflect each tuple of the schema rdd to the Row class to get the row objects created)
# this reflection is going to happen automatically, if we don't do it explicitly
from pyspark.sql.types import Row

schemarowobjrdd2 = rdd2.map(lambda x: (Row(exchange=x[0], stock=x[1], value=float(
    x[2]))))  # reflecting every tuple to the Row class to get Row object reflected
df3 = schemarowobjrdd2.toDF()
df4 = spark.createDataFrame(schemarowobjrdd2)
df3.show()
df4.show()

# or

# Rather writing the code like above (either schemardd+collist or rowrdd(Reflection) to DF),
# PREFERABLY write the code (directly create DFs) as given below
# If i am not going to write rdd then representing to DF, rather directly creating DF
df1_direct = spark.read.option("delimiter", "~").option("inferschema", True).csv(
    "file:///home/hduser/sparkdata/nyse.csv") \
    .toDF("exchange", "stock", "amt")
# or
df2_direct = spark.read.csv("file:///home/hduser/sparkdata/nyse.csv", sep='~', inferSchema=True).toDF("exch", "stock",
                                                                                                      "value")
# dsl programming
df2_direct.select("stock").distinct().show()
# sql programming (familiar)
df2_direct.createOrReplaceTempView("stock_view")
spark.sql("select distinct stock from stock_view").show()

# A typical example for mandatorily writing rdd then moving to df or temp view queries?
# 1. create an rdd to convert the data into structured from unstructured
rdd1 = sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(
    lambda x: x.split(" "))  # I converted the unstructured data to structured
# 2. represent rdd as dataframe (#represent data from notepad to xls (colname and dtype))
schemarowrdd2 = rdd1.map(lambda x: (Row(coursename=x)))  # reflection
schemarowrdd2df1 = schemarowrdd2.toDF()
# or
schemardd3 = rdd1.map(lambda x: (x,))  # collist
collist = ['coursename']
schemardd3df2 = schemardd3.toDF(collist)
# 3. write dsl queries
schemardd3df2.select("coursename").where("coursename in ('hadoop','spark')").show()  # DSL query
# or
# 4. represent df as tempview (since sql is simple and more familiar)
schemardd3df2.createOrReplaceTempView("courseview")
# 5. write sql queries
spark.sql("select upper(coursename) from courseview").show()
# CONCLUSION - we cant avoid creating rdd, hence create rdd and represent as DF/tempview (ASAP)

####1. creating DF from rdd - ends here######

# VERY IMPORTANT
####creating DF directly from different sources - starts here (very important)######
# 2. how to create DF from different sources (using different options) by inferring the schema automatically or manually defining the schema (very important)

# when we create spark session object in the name of spark (it comprise of sparkcontext, sqlcontext, hivecontext)
# where we need spark context - to write rdd programs (manually or automatically)
# where we need sql context - to write dsl/sql queries
# where we need hive context - to write hql queries
# sc=spark.sparkContext#for spark sql program, sc is indirectly used

# creating DFs directly
# 1. spark session object important
spark = SparkSession.builder.getOrCreate()
# 2. The methods with options we can use under read and write module/method of the spark session
# spark.read.any built in modules...

# 3. csv module (important) option -> (option/options/inline arguments/format & load) inferschema, header, delimiter/sep
# column names starts with _c0, _c1... with default delimiter ',' with default datatype String
df1_notype_name_del = spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")
df1_notype_name_del.printSchema()
df1_notype_name_del = spark.read.option("delimiter", "~").csv("file:///home/hduser/sparkdata/nyse.csv")
df1_notype_name_del.printSchema()

# 2. Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table) / external (cloud, api, nosql))
# or programatically
# ****Possible way of calling the csv function with different options
# option(opt1,value1), options(opt1=value,opt2=value), csv(filename,opt1=value,opt2=value), format(csv).option/options.load("file")
# this will create df with default comma delimiter
df1_comma_del = spark.read.csv("file:///home/hduser/sparkdata/empdata.txt")
df1_comma_del.printSchema()
# this will create df with custom ~ delimiter
df1_custom_del = spark.read.option("delimiter", "~").csv(
    "file:///home/hduser/sparkdata/nyse.csv")  # option with key,value pair
df1_custom_del.printSchema()  # column names will _c0,c1.. and datatype string

# multiple option to call the default functions like csv
df1_custom_del_autoschema = spark.read.option("delimiter", "~").option("inferschema", True).csv(
    "file:///home/hduser/sparkdata/nyse.csv")
df1_custom_del_autoschema.printSchema()
# this will create df with custom ~ delimiter and automatically infer schema (respective dataype)

# or if we have multiple options to be used, then go with the below way (options rather than option)
df2_custom_del_autoschema = spark.read.options(inferSchema=True, delimiter='~').csv(
    "file:///home/hduser/sparkdata/nyse.csv")
df2_custom_del_autoschema.printSchema()
df2_custom_del_autoschema.show()

# or if we have multiple options to be used, then go with the below way (dont use option/options rather we can use even inline function arguments also)
df3_custom_del_autoschema = spark.read.csv("file:///home/hduser/sparkdata/nyse.csv", sep='~',
                                           inferSchema=True)  # inline arguments
df3_custom_del_autoschema.printSchema()
df3_custom_del_autoschema.show()

# the below way of creating df is (not suggested for the built in modules such as csv, json, orc), but suggested approach for external modules (cloud, redshift, bigquery) ... legacy approach
df4_custom_del_autoschema_format_load = spark.read.format("csv").option("delimiter", "~").option("inferschema",True).load("file:///home/hduser/sparkdata/nyse.csv")
df4_custom_del_autoschema_format_load.printSchema()  # column names will _c0,c1 and datatype automatically inferred String, String, double
df4_custom_del_autoschema_format_load.show()
df4_custom_del_autoschema_format_load = spark.read.format("csv").options(inferschema=True, delimiter='~').load(
    "file:///home/hduser/sparkdata/nyse.csv")
df4_custom_del_autoschema_format_load.printSchema()  # column names will _c0,c1 and datatype automatically inferred String, String, double
df4_custom_del_autoschema_format_load.show()

# How to DEFINE column names for the dataframe at the time of creation or how to CHANGE the column names (later)
# How to DEFINE column names for the dataframe at the time of creation?
# if header is not available, i will go simply toDF("column list")
df5_custom_del_autoschema_colname = spark.read.csv("file:///home/hduser/sparkdata/nyse.csv", sep='~',
                                                   inferSchema=True).toDF("exchange", "stock", "value")
df5_custom_del_autoschema_colname.printSchema()
df5_custom_del_autoschema_colname.show()
# column names will "exchange","stock","value" and datatype inferred automatically

# if header is available, i will go simply with header=true
# exchange_name~stock_name~closing_rate
# NYSE~CLI~35.3
# NYSE~CVH~24.62
df6_custom_del_autoschema_headercolname = spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv", sep='~',
                                                         inferSchema=True, header=True)
df6_custom_del_autoschema_headercolname.printSchema()
df6_custom_del_autoschema_headercolname.show()

# if i dont mention header,true -  it will consider the header as false
df6_custom_del_autoschema_noheadercolname = spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv", sep='~',
                                                           inferSchema=True)
df6_custom_del_autoschema_noheadercolname.show()
df6_custom_del_autoschema_noheadercolname.printSchema()
# it consider the first row and then assume the remaining also as the same
# it wont consider the first line as header, rather as a data
# column names will not be considered, so default _c0,_c1 .. will be used

# or

# preference 2 if one/two regular option are going to be used
df7_option = spark.read.option("delimiter", "~").option("header", True).option("inferschema", True).csv(
    "file:///home/hduser/sparkdata/nyse_header.csv")

# or
# preference 1 if regular options are going to be used
df7_options = spark.read.options(delimiter="~", header=True, inferschema=True).csv(
    "file:///home/hduser/sparkdata/nyse_header.csv")

# or
# preference 1 (inline arguments) if (more/other options) are going to be used
df7_inlineargs = spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv", sep='~', inferSchema=True, header=True)

# or
# preference 3 if other sources we are going to use
df7_format_load = spark.read.format("csv").options(inferschema=True, delimiter='~', header=True).load(
    "file:///home/hduser/sparkdata/nyse.csv")

################Defining/Changing Column Names in all possible ways
#1.header, toDF,header+toDF, alias,withColumnRenamed, SQL

# if header is available in the data, but need to change MOST of the column names - STANDARDISATION of the dataset
# toDF(colname1,colname2)
# exchange_name~stock_name~closing_rate
# NYSE~CLI~35.3
# NYSE~CVH~24.62
# NYSE~CVL~30.2
df8_change_allcol_names = spark.read.format("csv").options(inferschema=True, delimiter='~', header=True).load(
    "file:///home/hduser/sparkdata/nyse_header.csv") \
    .toDF("exchange", "stock", "rate")
# exchange_name~stock_name~closing_rate -> exchange~stock~rate

# if header is available but need to change few of the column names
df8_change_fewcol_names = spark.read.format("csv").options(inferschema=True, delimiter='~', header=True).load(
    "file:///home/hduser/sparkdata/nyse_header.csv")
# withColumnRenamed (DSL)
df9 = df8_change_fewcol_names.withColumnRenamed("exchange", "exch").withColumnRenamed("stock_name", "stock")
df9.printSchema()

# alias (DSL)
from pyspark.sql.functions import col
df10 = df8_change_fewcol_names.select(col("exchange").alias("exch"), col("stock_name").alias("stock"), col("close"))
#select will not put any constraints for selecting the column, it accepts both string and column type
df10.printSchema()

# or
# convert df to tempview and write ansi sql
df8_change_fewcol_names.createOrReplaceTempView("df8view")
df11 = spark.sql("select exchange as exch,stock_name stock,close from df8view")
df11.printSchema()
# familiar sql

# If header is available need to change few column name after applying some transformations/aggregations then use .alias()
# do aggregation
# DSL way
df8_change_allcol_names.groupby("exchange").agg(max("close").alias("max_close_rate"),min("close").alias("min_close_rate")).show(2)
df8_change_allcol_names.groupby("exchange").agg(max("close"),min("close")).withColumnRenamed("max(close)","max_close_rate").withColumnRenamed("min(close)","min_close_rate").show()
# alias is used just for proper column name definition to write it in the target or display in the console

# alias is mandatory for the column name definition as per hive table columname standardization
# If we want to save the df in hive, then Alias/renaming of the column is mandatory - provided if the column names are not as per the hive standard
df8_change_allcol_names.groupby("exchange").agg(col(max("close")).alias("max_close_rate")).write.saveAsTable("hiveaggrtbl")
#df8_change_allcol_names.groupby("exchange").agg(max(col("close")).alias("max_close_rate")).show()

# If header is available need to change few column name after applying some transformations/aggregations then use SQL functions also
# ISO/ANSI SQL
df8_change_allcol_names.createOrReplaceTempView("view1")
spark.sql("select exchange,max(close) as max_close_rate,min(close) min_close_rate from view1 group by exchange").show()

#column names can be defined using custom schema (structype,structfield etc.,)

#####Completed the defining and changing of column names using (toDF,header,both,withColumnRenamed,alias, ANSI/ISO SQL, custom schema) in a DF########

#####Started the defining and changing of Datatype########

# ****very very important item to know for sure by everyone... StructureType & StructFields
# I don't wanted to infer the schema (columname & datatype & null (constraint)), rather I want to define the custom schema?
# Interview Question: Tell me some performance
# performance optimization - dont use inferschema unless it is inevitable like the given example below
#initially in our code the developers have used inferschema to identify the type,
# later when data grows exponentially then inferschema everytime degraded the performance of the job, hence removed infer schema and used custom schema
# If header is not available need to define column names and the DATATYPE also - we can simply go with inferschema and toDF to achieve it,
# but not a preferred/optimistic solution
# inferschema="true",

# inferschema - must be avoided to use or must be cautiously used in the dataframe creation
# we can avoid scanning the whole data (if it is huge) to infer the schema - read all records and identify the data type
# applies some default datatype like double, which will not allow me to define cust datatypes (It may leads to incorrect datatype)
# When do we use inferschema then -
#           If the volume of data is small and number of columns are more
#           If the source data is variable schema in nature
#           If we are good with the data type what inferschema produces (double type for close_rate column), then we can use.
#           Importantly - For Testing, data validation, allowing all column values to be identified
#           (eg. do we have any non integer type in integer column)

df1.createOrReplaceTempView("view1")
spark.sql("select * from view1 where upper(close)<>lower(close)").show()

# inferschma indentifies entire column as string even if one record with the given integer/float column is string

# ****** Important case ->
# If header is not available need to define custom column names and need to define the custom DATATYPE also -
# we need to go with custom schema, use 'schema' option to achieve this
# 'Structure Type' & 'Structure Fields'
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType,TimestampType
# from pyspark.sql.types import *#not preferred way
nysestruct=StructType([StructField("exchange_name",StringType()),StructField("stock_name",StringType()),StructField("close_rate",FloatType())])
#not so easy, you have to practice, but very important
#general template syntax ,format for defining structure
#custom_structure_schema=StructType([StructField("col1name",DataType()),StructField("col2name",DataType()),....])
df9_custom_schema_noheader = spark.read.format("csv").schema(nysestruct).option("delimiter",'~').load("file:///home/hduser/sparkdata/nyse.csv")
df9_custom_schema_noheader.show()
df9_custom_schema_noheader_csv = spark.read.schema(nysestruct).option("delimiter",'~').csv("file:///home/hduser/sparkdata/nyse.csv")
df9_custom_schema_noheader_csv.show()
df9_custom_schema_noheader_csv = spark.read.schema(nysestruct).csv("file:///home/hduser/sparkdata/nyse.csv",sep='~')
df9_custom_schema_noheader_csv.show()

# If header is available need to define column name and the DATATYPE also - we need to go with custom schema, use schema option to achieve this
# we are NORMALIZING/STANDARDIZING...
df9_custom_schema_header = spark.read.schema(nysestruct).option("delimiter",'~').option("header",True).csv("file:///home/hduser/sparkdata/nyse_header.csv")
df9_custom_schema_header.show()

#IF I want to use my source provided header as my column names (but not the datatype to be inferred automatically), how to manage it?
df9_custom_schema_header = spark.read.format("csv").option("delimiter",'~').option("header",True).load("file:///home/hduser/sparkdata/nyse_header.csv")#extracting
df9_custom_schema_header.select("stock_name",col("close").cast("float").alias("close")).printSchema()

#Writing of the csv dataframe data into filesystem
df10=df9_custom_schema_header.select("stock_name",col("close").cast("float").alias("close"))#type conversion + Transforming
df10.write.option("delimiter","|").option("header",True).save("/user/hduser/csvdata",format="csv",mode='overwrite')#write the df into csv format using tradition save & format function
spark.read.option("header",True).csv("/user/hduser/csvdata",sep='|').show()
#or
df10.write.csv(path="/user/hduser/csvdata",mode="overwrite",sep='|',header=True)
#schema migration (csv to json)
df10.write.mode("append").save(path="/user/hduser/jsondata",format="json")#schema migration

# I have only few columns (5) only to process, do you want to create structtype for all the columns
# (100 columns) in the given source data?
df10_noheader_morecolumns_csv = spark.read.csv("file:///home/hduser/sparkdata/empdata.txt",sep=',')
df11=df10_noheader_morecolumns_csv.select(col("_c0").alias("name"),col("_c4").cast("int").alias("amount"))
df11.show()

df10_noheader_morecolumns_csv.createOrReplaceTempView("view1")
spark.sql("select _c0 name,cast(_c4 as int) amount from view1").printSchema()

###### Important basic part of using some option/options/format-load ,inline , usage methodology of the functions,mode(append/overwrite),read, write, schema definition, basic functions alias, cast, col, withcolumnrenamed etc.,
##### which is mostly common for all the below other built in sources also

###Further morefully concentrate on the different source and target systems...

# Reading/importing & Writing/exporting the data from Databases (Venkat's usecase)
#currently -> Oracle -> imported Sqoop -> HDFS -> Hive ETL -> Hive -> Export (some tool -> sqoop export) -> Oracle
#the reason behind is to push the hard work to bigdata platform, bigdata platform HIVE need the oracle data,
# some data in bigdata platform is needed for some lookup and enrichment requirement
#current requirement -> Oracle -> imported Spark ETL-> Hive -> consumers consume from hive
#                       Oracle -> imported Spark ETL+lookup and enrichement-> Export spark -> Oracle

# writing the df into different formats/tables/hive

# Reading/importing & Writing/exporting the data from Databases (Venkat's usecase)
#Interview Question: Prerequisite for connecting with any databases using spark jdbc?
#1. Identify the DB source where we are read/writing the data
#2. Identify the username,password, connection url etc., from the source/target system
#3. Identify the driver/connector provided by the DB providers, download, include in your spark (inline) program,
#ask admin to add this driver.jar in the /usr/local/spark/jars path
#pyspark --jars file:///home/hduser/install/mysql-connector-java.jar
#spark-submit --jars file:///home/hduser/install/oracle-odbc.jar mysparkapp.py

#how to read the data from an RDBMS
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.jdbc.Driver"})

#how to read the data from an RDBMS applying custom schema (not possible directly, but have workaround)
#Benifits:
#1. I can define my custom datatypes for the respective columns
#2. I can apply not null constraint when i pull data from DB - #ValueError: field custid: This field is not nullable, but got None
#create df without schema applied -> apply schema using createDataframe(df.rdd,schema)
from pyspark.sql.types import *
#schema can be used applying constraints (NULL)
dbschema=StructType([StructField("custid",IntegerType(),True),StructField("firstname",StringType()),StructField("lastname",StringType()),StructField("city",StringType()),StructField("age",IntegerType()),StructField("createdt",DateType()),StructField("trans_amt",IntegerType())])
#dfdb1=spark.read.schema(dbschema).jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.jdbc.Driver"})
#applying schema directly is not allowed in database sources -pyspark.sql.utils.AnalysisException: User specified schema not supported with `jdbc`
#As a workaround, I am creating the dfdb1 without schema applied
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.jdbc.Driver"})
#later I am representing/converting the dfdb1 to rdd then converting to DF applying the schema
newdf1=spark.createDataFrame(dfdb1.rdd,dbschema)
#ValueError: field custid: This field is not nullable, but got None

#ETL -> Hive table (Aggreation/Summarization)
from pyspark.sql.functions import *
etl_df1=newdf1.where("city is not null").groupby("city","age").agg(max("createdt").alias("max_create_dt"),sum("trans_amt").alias("sum_trans_amt"))
etl_df1.show()
etl_df1.write.saveAsTable("hivetbl",mode='overwrite')

#LOOKUP AND ENRICHMENT
#ETL+Lookup the file+Enrichment the statename -> RDBMs table
filedf1=spark.read.option("inferschema",True).csv("file:///home/hduser/sparkdata/states.csv").toDF("cityname","statename")
newdf1.createOrReplaceTempView("dbview")
filedf1.createOrReplaceTempView("fileview")
joinedDF=spark.sql("select d.*,f.statename from dbview d left join fileview f on d.city=f.cityname")#lookup of city and enrichment of state
joinedDF.write.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",mode='overwrite',table="customer_spark_join",properties={"driver":"com.mysql.jdbc.Driver"})

#How to answer this usecase as a story telling?


#####Conclusion of venkat's usecase########

#reading/writing the df into different file formats (parquet/orc/json) - xml, avro, fixedwidth..

#performance optimization:
#reading json data from remote cluster (Cluster1-ingestion)
#remote cluster (50gb data compressed hdfs) -> Revathi's Cluster2(Processing - spark operation)
#remote cluster (50gb data compressed hdfs) -> hdfs -> spark (hdfs) -> Revathi's cluster(spark executor)
#spark.read.csv("hdfs://127.0.0.1:54310/user/hduser/txns_big2").rdd.getNumPartitions()
#spark.read.csv("hdfs://127.0.0.1:54310/user/hduser/txns_big2.gz").rdd.getNumPartitions()
# Receive and read the data in compressed fashion & Write and transfer the data to the downstream system by compressing
# Benchmarking - trying with different option
# Try with different options for transferring the data
# eg. sftp (multipart) (LFS -> LFS)- how long taking?
# lfs -> hdfs -> disctp -> cluster2 hdfs -> spark read...
# distcp (cluster to cluster)- how long taking?
# different compression techniques - bzip,gzip,snappy,lzo - how long taking?
# read without uncompressing in Spark () - how long taking?

#Writing of data into different file formats
# spark sql can handle semi structured data - yes, eg. i handled json data
dfdb1.write.json("file:///home/hduser/dbjsondata")
#Multiple json options to Explore
dfdb1.write.partitionBy("city").json("file:///home/hduser/dbjsondata",mode="overwrite",compression="gzip",lineSep=',',ignoreNullFields=False)
dfdb1.write.format("json").save("file:///home/hduser/dbjsondata",partitionBy="city")
#interview question - want to write the df data in a single json file to share it to our consumers (outbound/egress)
dfdb1.coalesce(1).write.json("file:///home/hduser/dbjsondata")
dfdb1.rdd.getNumPartitions()#only rdd supports getnumpartitions

#Where can i find the description for all these functions options (csv/orc/json/parquet/jdbc/hive)
#1. spark documentation - https://spark.apache.org/docs/latest
#2. open the pyfile/module and see /usr/local/spark/python/pyspark/sql/readwriter.py

#Interview question: If partition column has null value, what happens?
# Spark/Hive will generate default partition and place all null partition data over there
#city=__HIVE_DEFAULT_PARTITION__

#Reading of json data as a dataframe
jsondf1=spark.read.json("file:///home/hduser/sparkdata/ebayjson.json")#by default schemainference will happen, better to use custom schema
jsondf2=spark.read.format("json").load("file:///home/hduser/sparkdata/ebayjson.json")

#If we get some malformed data, how do we identify them and reject/send to the source system
#If spark application has to proceed even if data is malformed (keeping the malformed data in corrupt column) - mode=permissive
jsondf1=spark.read.json("file:///home/hduser/sparkdata/ebayjson_corrupt.json",mode='permissive',columnNameOfCorruptRecord="objects_to_reject")
jsondf1.cache()
jsondf1.select("objects_to_reject").where("objects_to_reject is not null").show(20,False)
jsondf1.select("objects_to_reject").where("objects_to_reject is not null").write.csv("file:///home/hduser/corruptedjson/")
#send the above data to the source system

#If spark application has to fail if data is malformed - mode=failfast
jsondf1=spark.read.json("file:///home/hduser/sparkdata/ebayjson_corrupt.json",mode="failfast")#mode-permissive/failfast/dropmalformed

#If spark application has to ignore  malformed and continue with the rest- mode=dropmalformed
jsondf1=spark.read.json("file:///home/hduser/sparkdata/ebayjson_corrupt.json",mode="dropmalformed")#mode-permissive/failfast/dropmalformed

#below option with help us read the files in different pattern from dir and subdirs
jsondfregular_options1=spark.read.json("file:///home/hduser/jsondir1",pathGlobFilter="ebay*[0-5].json",recursiveFileLookup=True)

#below option help us read the malformed json data as a proper data
jsondfmalformed_options1=spark.read.json("file:///home/hduser/sparkdata/ebayjson_corrupt.json",allowUnquotedFieldNames=True,allowSingleQuotes=True,allowNumericLeadingZero=True,dropFieldIfAllNull=True)

#,columnNameOfCorruptRecord="objects_to_reject",allowUnquotedFieldNames=True,allowSingleQuotes=True,allowNumericLeadingZero=True,dropFieldIfAllNull=True
#{"auctionid":"3018594562",bid:65.99,'bidtime':1.08876,"bidder":"cbock64","bidderrate":042,"openbid":0.01,"price":244.0,"item":"palm","daystolive":3}

# spark sql can handle serialized data - yes, eg. i handled serialized data
#Writing orc (optimized row columnar) data
dfdb1.write.orc("file:///home/hduser/sparkorcdata/",mode='append')
dfdb1.write.save("file:///home/hduser/sparkorcdata/",format="orc",mode='overwrite')
#Reading orc data -
#performance optimization: my ex peers, where they read the data from serialized files using traditional mechanism
#I changed some of the programs to only read the columns what are needed directly from the file like a table/view
orcdf1=spark.read.orc("file:///home/hduser/sparkorcdata/")#entire columns loaded to the memory
orcdf1.select("custid").show()#dsl
orcdf1.createOrReplaceTempView("orcview")#representing as tempview, bcz i want to write sql query
spark.sql("select custid from orcview").show()#sql choosing only one column from 10 columns

#as Orc is a intelligent file format, we can conside orc like a hive/NOSQL/RDBMS Database table
orcdf2=spark.sql("select custid from orc.`file:///home/hduser/sparkorcdata/`")#project only what we needed
orcdf2.show()

#Writing parquet data
dfdb1.write.parquet("file:///home/hduser/sparkparquetdata/",mode='append')
dfdb1.write.save("file:///home/hduser/sparkparquetdata/",format='parquet',mode='overwrite')

#Reading parquet data
pardf1=spark.read.parquet("file:///home/hduser/sparkparquetdata/")#entire columns loaded to the memory
pardf1.select("custid").show()#dsl
pardf1.createOrReplaceTempView("parview")#representing as tempview, bcz i want to write sql query
spark.sql("select custid from parview").show()#sql choosing only one column from 10 columns

#as Orc is a intelligent file format, we can conside parquet like a hive/NOSQL/RDBMS Database table
pardf2=spark.sql("select custid from parquet.`file:///home/hduser/sparkparquetdata/`")#project only what we needed
pardf2.show()

#Interview Question?
# Can we tranfer data between one spark app to another ? not achievable directly, but by persisting the app1 data to fs/hive/db table is possible, but preferably store in a serialized format (parquet+snappy)
# raw(csv/rdbms) -> DE curation (saveastable)-> parquet+snappy-hive table -> new spark app DS model -> hive table (consumption) -> visualization
# raw(csv/rdbms) -> DE curation (HQL)-> text/orc/parquet-hive table ->  HQLs/Visualization

#performance optimization - Why serialized data? why parquet? why orc? difference between parquet & orc? when do we go for what?
#Serialization common Features -> Fast(storage, network transfer, processing), Compact, Interoperable, Secured, Extensible, Intelligent Encoding
######## Top Features of ORC & Parquet #############
#1. Fast(storage/retrive, network transfer, processing), Compact, Interoperable, Secured, Extensible(schema evolution), Intelligent Encoding
#2. Intelligent storage format (indexes (read the particular row), internal (parquet chunks) of partitions(orc strides) (read the particular row from a chunk/partition/strides), Encoded format - range, run length encoding and dictionary encoding)
#2. Intelligent storage - indexing(predicate), partitioning(strides/stripes/chunks)(predicate), encoding (save space)
#pull only the given row using index identified in a given partition (blocks) (predicate pushdown)
#transfer the data in the network only what is needed
#3. Row/Column oriented fashion - Store, Read and process (compression, aggregaion, grouping) only what ever the column
# and row we needed (PDO optimization - predicate pushdown, projection pushdown)
#pull only the column what i needed (project pushdown)
#select sum(amount) - fetch only amount column (orc/parquet) and don't fetch other columns unlike databases
#4. Compression (Snappy) will be more efficient than the compression on the csv/json/xml.
#apply specific type of compression on the specific columns because of column oriented.
#compression -> 1,2,3,4,5,6 (column)
#compression -> 1,irfan,2010-10-01 (row)
#writing/reading orc data

#or
#performance optimization - project only what you needed (columnar storage) and predict only what row you want
spark.sql("select custid from parquet.`file:///home/hduser/sparkparquetdata/` where city='Chennai'").explain()

#Writing/Reading the data into Hive Table
#Writing the spark data into HIVE table?
#1. Writing DF into Hive table Directly using spark native functions
dfdb1.write.saveAsTable("retail.cust_mysql_hivetbl") #If managed table is not present, then create and load using spark function (parquet+snappy)
#if the table present, it will either allow me to append or overwrite in parquet format
#overwrite option (complete refresh)
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table="customer",properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.write.saveAsTable("retail.cust_mysql_hivetbl",mode='overwrite')

#append option if incremental/delta data load is needed
tbl_query="(select * from customer where createdt>=current_date)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.write.saveAsTable("retail.cust_mysql_hivetbl",mode='append')
    #db data --> aggregating -> data frame --> hive table
    #use the saveastable option if we are going to consume this hive data by SPARK itself subsequently

#simple usecase - Read data from rdbms -> spark (aggregate) -> hive table
#best way
#methodology - 1
tbl_query="(select city,transactamt from customer)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.groupby("city").agg(sum("transactamt").alias("sum_transamt")).write.saveAsTable("retail.cust_mysql_hiveaggr")

#methodology - 2
tbl_query="(select city,transactamt from customer)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.createOrReplaceTempView("dbview")
spark.sql("select city,sum(transactamt) sum_transamt from dbview group by city").write.saveAsTable("retail.cust_mysql_hivesqlaggr")

#methodology - 3 (Best methodology - PDO with the DB)
#here we are doing a PDO
tbl_query="(select city,sum(transactamt) sum_transamt from customer group by city)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.write.saveAsTable("retail.cust_mysql_hive_pdo_sqlaggr")

#or
#2. Writing Tempview data into Hive table (HQL) - Using HQL itself and not the spark native functions
tbl_query="(select city,transactamt from customer)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
#db data --> data frame --> spark temp view --> summary inlineview --> hive table
dfdb1.createOrReplaceTempView("dbview")
#We use HQL rather than the spark built in functions if the hive data is consumed by the Datalake HIVE users
spark.sql("insert overwrite table retail.cust_mysql_hivesqlaggr select city,sum(transactamt) sum_transamt from dbview group by city")#HQL+SQL


#Important - nitty-gritty things When do we use saveastable (spark builtin functions to interact with hive) or HQL itself to interact with hive
#We use spark functions rather than the HQL if the hive data is consumed by the Datalake spark users or other spark applications
#spark uses his own (optimization) serialization and compression by default.
#spark saveastable will be only supporting some of the hive functionalities
#for eg. Loading of buckets in hql under spark is not possible, but it is possible if we use saveastable

#We use HQL rather than the spark built in functions if we need to have more control on the delimiter, format, compression etc., the hive data is consumed by the Datalake HIVE users/visualization tool
#another reason for going with HQL is, not all the hive functionalities are available in saveastable
# for eg. hive external table can't be created using saveastable
#another eg. hive partition overwrite is not working as expected with saveastable

#CTAS also can be done
#If we need to create and write data into hive table using spark SQL
spark.sql("create table cust_aggr_spark(city string,sum_transactamt int) row format delimited fields terminated by ','")
#or
#create hql hive table with necessary options
spark.sql("create table cust_aggr_spark(city string,sum_transactamt int) row format delimited fields terminated by ','")
spark.sql("insert overwrite table retail.cust_mysql_hivesqlaggr select city,sum(transactamt) sum_transamt from dbview group by city")#Inline view

#or

#Interview Question? Can you use spark to create and store hive external table?
#External table can be only created using hql and spark saveastable will not support
dfdb1.write.saveAsTable("mantable")#creation of external table using spark native function is not possible
spark.sql("""create external table ext_cust_aggr_spark(city string,sum_transactamt int)
stored as orcfile
location '/user/hduser/ext_cust_aggr_spark'""")

#Inline view

#Interview Question? Can you use spark to create and store partitions in hive table?
#Db to hive table partition load using spark native function
tbl_query="(select * from customer where createdt<current_date)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.write.partitionBy("createdt").saveAsTable("retail.cust_spark_part1",mode="overwrite")
#In the above saveastable function all the partitions are over writterned, hence going with Hql option

#DB to hive table partition load using HQL is working as expected for overwriting the partition if exists already and not the entire table
#The DB Data I am going to write/persist into hive external partitioned table
tbl_query="(select * from customer where createdt>=current_date)temp"
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table=tbl_query,properties={"driver":"com.mysql.jdbc.Driver"})
dfdb1.createOrReplaceTempView("dfdbpart")
spark.sql("""create external table retail.cust_spark_part_orc
(custid int,firstname string,lastname string,city string,age int,transactamt int)
partitioned by (createdt date)
stored as orcfile
location '/user/hduser/cust_spark_part_orc/'""")
spark.sql("insert overwrite table retail.cust_spark_part_orc partition(createdt) select custid,firstname,lastname,city,age,transactamt,createdt from dfdbpart")

#Interview Question? Can you use spark to create and store buckets in hive table?
#Spark upto current version, doesn't support hive bucketing
#HQL Native buckets is not supported
spark.sql("""create external table retail.cust_spark_bucket
(custid int,firstname string,lastname string,city string,age int,transactamt int,createdt date)
clustered by (custid) into 3 buckets
location '/user/hduser/cust_spark_bucket/'""")
spark.sql("insert overwrite table retail.cust_spark_bucket select custid,firstname,lastname,city,age,transactamt,createdt from dfdbpart")
#pyspark.sql.utils.AnalysisException: Output Hive table `retail`.`cust_spark_bucket` is bucketed but Spark currently does NOT populate bucketed output which is compatible with Hive.

#but spark support spark bucketing to load hive table (Spark currently populate bucketed output which is compatible with Spark bucketin function like saveastable with bucketBy)
#spark buckets algorithm, works
dfdb1.write.bucketBy(3,"custid").saveAsTable("retail.cust_spark_bucket",mode='overwrite')
spark.sql("select * from retail.cust_spark_bucket").show()

#Write data into hive using insertinto option
dfdb1.write.insertInto("retail.cust_spark_bucket")
# If managed table is not present, insertinto will not create,
# Insertinto will write the data to hive as insert statements
# applicable more for small volume of data

#Reading Data from Hive Table
#1. Reading data from Hive table as a DF
#reading the data from hive, was created by spark earlier using saveastable option (not hard and fast, vice versa will also work)
hivedf1=spark.read.table("retail.cust_mysql_hivesqlaggr")
#I want to perform DSL queries on the dataframes
hivedf1.select("*").show()

#2. Reading data from Hive table using HQL
hivedf2=spark.sql("select * from cust_aggr_spark")
#I want to perform HQL/SQL queries on the direct hive table
spark.sql("from cust_aggr_spark select distinct city ").show()

#CONCLUSION: what are all we know in the SPARK+HIVE?
#write a df into hive table (overwriting/appending), partition, ext/man, buckets
#write df->tempview -> hive table using spark native or hql native queries
#HIVE -> managed, external, partition, bucketing
#Spark native function supports -> managed, bucketing
#HQL native in Spark supports -> anything other than bucketing alone (managed, external, partition)

##################Hive Read and write using spark native functions and HQL queries is completed#

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
