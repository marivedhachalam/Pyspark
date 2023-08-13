# PySpark hackathon 2023
from pyspark.sql.session import SparkSession
from pyspark import StorageLevel
from configparser import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import shutil

spark = SparkSession.builder.master("local[2]")\
    .appName("WD30 Hackathon Mari")\
    .config("spark.jars","file:///home/hduser/install/mysql-connector-java.jar")\
    .config("spark.sql.shuffle.partitions", 4)\
    .enableHiveSupport()\
    .getOrCreate()

sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

print("")
print(f"Hackathon 2023 - Mari's Version ")
print("")
print(f"               Part A - SPARK CORE RDD TRANSFORMATIONS / ACTIONS (Step 1 & 2) ")
print(f"1. Data cleaning, cleansing, scrubbing - Loading RDDs, remove header, blank lines and impose caseclass schema")
print(f"-------------------------------------------------------------------------------------------------------------")
print(f"1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata")

path = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo1.csv"
insuredata1 = sc.textFile(path)
cnt1 = insuredata1.count()
print(f"RDD1 count of the file insuranceinfo1.csv before the cleanup process :{cnt1}")
print(insuredata1.take(10))

print(f"2 Header record removed ")
header = insuredata1.first()
insuredata1a = insuredata1.filter(lambda row: row != header)  # filter out the header
print(insuredata1a.take(10))

cnt3 = insuredata1a.count() - 1
tailer = insuredata1a.zipWithIndex().filter(lambda x: x[1] == cnt3)
print(f"Footer record - {tailer.collect()}")
insuredata1b = insuredata1a.zipWithIndex().filter(lambda x: x[1] != cnt3)
print(f"3 Footer record removed ")
#print(insuredata1b.collect())

print(f"4 Display the count and show few rows and check whether header and footer removed")
cnt4 = insuredata1b.count()
print(f" First record of the RDD1 is : {insuredata1b.first()} - This is not a header record")
istailer = insuredata1b.zipWithIndex().filter(lambda x: x[1] == (cnt4-1))
print(f" Last record of the RDD1 is : {istailer.collect()} - This is not a footer record")
print(f" Count of the RDD2 after header and Trailer removed is : {cnt4} ")

print(f"5 Remove the blank lines in the rdd ")
insuredata1c = insuredata1b.filter(lambda line: len(line[0].strip()) > 0)
cnt5 = insuredata1c.count()
print(f"RDD1 count after removing the blank lines and cleanup :{cnt5}")
print(insuredata1c.take(10))

print(f"6 Map and split using ‘,’ delimiter")
insuredata1d = insuredata1c.map(lambda line: line[0].split(",", -1))
print(insuredata1d.take(10))

print(f"7 Filter number of fields which are equal to 10 columns only")
num_fields = 10
insuredata1e = insuredata1d.filter(lambda line: len(line) == num_fields)
cnt7 = insuredata1e.count()
print(f"RDD1 count after selecting 10 columns only :{cnt7}")
print(insuredata1e.take(10))

print(f"8 Take the count of the RDD1 created in step 7 and step 1 and print how many rows are removed/rejected")
print(f"in the cleanup process of removing the number of fields does not equals 10 ")
cnt8 = cnt1 - cnt7
print(f"Records removed / rejected from cleanup process :{cnt8}")

print(f"9 Create another RDD namely rejectdata and store the row that does not equals 10 columns and with a new column added in the first column contains number of columns")

def add_new_column(row):
    numcols = len(row)
    return str(numcols) + ',' + str(row)
numcolval = insuredata1d.filter(lambda line: len(line) != 10)
insuredata1f = numcolval.map(add_new_column)
print(insuredata1f.take(10))

print(f"10 Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2")
path = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo2.csv"
insuredata2 = sc.textFile(path)
print(insuredata2.take(10))
print(f"RDD2 count of the file insuranceinfo2.csv before the cleanup process :{insuredata2.count()}")

print(f"11 Repeat from step 2 to 8 for this file also and create the schema rdd from the insuranceinfo2.csv and")

print(f"2 Header record removed ")
header2 = insuredata2.first()
print(header2)
insuredata2a = insuredata2.filter(lambda row: row != header2)  # filter out the header
print(insuredata2a.take(10))

cnt3 = insuredata2a.count() - 1
tailer = insuredata2a.zipWithIndex().filter(lambda x: x[1] == cnt3)
print(f"Footer record - {tailer.collect()}")
insuredata2b = insuredata2a.zipWithIndex().filter(lambda x: x[1] != cnt3)
print(f"3 Footer record removed ")

print(f"4 Display the count and show few rows and check whether header and footer removed")
cnt4 = insuredata2b.count()-1
print(f" First record of the RDD2 is : {insuredata2b.first()} - This is not a header record")  # should not be a header record
istailer2 = insuredata2b.zipWithIndex().filter(lambda x: x[1] == cnt4)
print(f" Last record of the RDD2 is : {print(istailer2.collect())} - This is not a footer record")
print(f" Count of the RDD2 after header and Trailer removed is : {print(insuredata2b.count())} ")

print(f"5 Remove the blank lines in the rdd ")
insuredata2c = insuredata2b.filter(lambda line: len(line[0].strip()) > 0)
cnt5 = insuredata2c.count()
print(f"RDD2 count after removing the blank lines and cleanup :{cnt5}")
print(insuredata2c.take(10))

print(f"6 Map and split using ‘,’ delimiter")
insuredata2d = insuredata2c.map(lambda line: line[0].split(",", -1))
print(insuredata2d.take(10))

print(f"7 Filter number of fields which are equal to 10 columns only")
num_fields = 10
insuredata2e = insuredata2d.filter(lambda line: len(line) == num_fields)
cnt7 = insuredata2e.count()
print(f"RDD2 count after selecting 10 columns only :{cnt7}")
print(insuredata2e.take(10))

print(f"8 Take the count of the RDD1 created in step 7 and step 1 and print how many rows are removed/rejected")
print(f"in the cleanup process of removing the number of fields does not equals 10 ")
cnt1 = insuredata2.count()
cnt8 = cnt1 - cnt7
print(f"Records removed / rejected from cleanup process :{cnt8}")

print(f" 9 Create the schema rdd from the insuranceinfo2.csv ")
print(f"filter the records that contains blank or null IssuerId,IssuerId2 for eg: remove the records with pattern given below. # ,,,,,,,13,Individual,Yes")
print(f"with the field names used as per the header record in the file and apply to the above data to create schemaed RDD")

schema_RDD = insuredata2e.map(lambda x:(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9]))
print(schema_RDD.take(10))

insuredata2f = schema_RDD.filter(lambda row: row[0].strip() != "" and row[0] is not None).filter(lambda row: row[1].strip() != "" and row[1] is not None)
print(insuredata2f.take(10))
cnt11 = insuredata2f.count()
print(f"Count of the schema RDD insured2 after cleanup :{cnt11}")

print(f"        2. Data merging, Deduplication, Performance Tuning & Persistence")
print(f"        ==================================================================")
print(f"12. Merge the both header and footer removed RDDs derived in steps 8 and 11 into an RDD namely insuredatamerged")
insuredatamerged = insuredata1e.union(insuredata2f)
cnt12 = insuredatamerged.count()
print(f"Count of merged RDDs insuredata1 and insuredata2 after cleanup is :{cnt12}")

print(f"13 Persist the step 12 RDD to memory by serializing ")
insuredatamerged_persisted = insuredatamerged.persist(StorageLevel.MEMORY_ONLY)
print(insuredatamerged_persisted)
#insuredatamerged.persist()

print(f"14. Calculate the count of rdds created in step 8+11 and rdd in step 12, check whether they are matching ")
print(f"Count of insuredata1 after cleanup is                          :{cnt7}")
print(f"Count of insuredata2 after cleanup is                          :{cnt11}")
cnt8and11 = cnt7 + cnt11
print(f"Count of merged RDDs insuredata1 and insuredata2 after cleanup :{cnt12}")
print(f"Count of RDDs insuredata1 + insuredata2 after cleanup  is      :{cnt8and11}")

if cnt12 != cnt8and11:
    print(f"The count is NOT matching before and after Merge process : {cnt8and11-cnt12}") #2931
else:
    print(f"The count is matching before and after Merge process : {cnt8and11}")

print(f"15 Increase the number of partitions in the above rdd to 8 partitions and name it as insuredatarepart ")
num_partitions = insuredatamerged.getNumPartitions()
print("Number of partitions:", num_partitions)

insuredatarepart = insuredatamerged.repartition(8)

num_partitions = insuredatarepart.getNumPartitions()
print("Number of partitions increased to :", num_partitions)

print(f"16 Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002, based on the BusinessDate of 2019-10-01 and 2019-10-02 respectively using Filter function")

target_business_date1 = '2019-10-01'
rdd_20191001 = insuredatarepart.filter(lambda x: x[2] == target_business_date1)
print(rdd_20191001.take(10))

target_business_date2 = '01-10-2019'
rdd_20191002 = insuredatarepart.filter(lambda x: x[2] == target_business_date2)
print(rdd_20191002.take(10))

print(f"17. Store the RDDs insuredatarepart, rdd_20191001 and rdd_20191002 created in step 15, 16 into HDFS location ")

hdfs_output_path = "hdfs:///user/hduser/sparkhack2/"
from py4j.java_gateway import java_import

def deletehdfsfile(file_path_to_delete):
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hdfs = sc._gateway.jvm.FileSystem.get(hadoop_conf)
    file_path = sc._gateway.jvm.Path(file_path_to_delete)
    if hdfs.exists(file_path):
        hdfs.delete(file_path, True)
#    else:
#        print(f"The file {hdfs_output_path}+/insuredatarepart is created")

file_path_to_store = hdfs_output_path+"/insuredatarepart"
deletehdfsfile(file_path_to_store)
insuredatarepart.saveAsTextFile(file_path_to_store)

file_path_to_store = hdfs_output_path+"/rdd_20191001"
deletehdfsfile(file_path_to_store)
rdd_20191001.saveAsTextFile(file_path_to_store)

file_path_to_store = hdfs_output_path+"/rdd_20191002"
deletehdfsfile(file_path_to_store)
rdd_20191002.saveAsTextFile(file_path_to_store)

print(f"18 Convert the RDDs created in step 15 above into Dataframe namely insuredaterepartdf using rdd.toDF() function")
collist=("IssuerId","IssuerId2","BusinessDate","StateCode","SourceName","NetworkName","NetworkURL","custnum","MarketCoverage","DentalOnlyPlan")
insuredaterepartdf=insuredatarepart.toDF(collist)

print(f"         Part B - Spark DF & SQL - DataFrames operations")
print(f"         ===============================================")
print(f"Apply Structure, DSL column management functions, transformation, custom udf & schema migration")
print(f"-----------------------------------------------------------------------------------------------")
print(f"19. Dataframe creation using the built-in modules")
print(f"A. Create first structuretypes for all the columns as per the insuranceinfo1.csv with the columns such as")
print(f"IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan")
# Hint: Do it carefully without making typo mistakes. Fields issuerid, issuerid2 should be of IntegerType, businessDate should be DateType and all other fields are StringType.

instructtype1 = StructType([StructField("IssuerId", IntegerType(), False),
                            StructField("IssuerId2", IntegerType(), False),
                            StructField("BusinessDate", DateType(), True),
                            StructField("StateCode", StringType(), True),
                            StructField("SourceName", StringType(), True),
                            StructField("NetworkName", StringType(), True),
                            StructField("NetworkURL", StringType(), True),
                            StructField("custnum", StringType(), True),
                            StructField("MarketCoverage", StringType(), True),
                            StructField("DentalOnlyPlan", StringType(), True)])


print(f"B. Create second structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as")
print(f"IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan")
# Hint: Do it carefully without making typo mistakes. Fields issuerid, issuerid2 should be of IntegerType, businessDate should be StringType and all other fields are StringType.

instructtype2 = StructType([StructField("IssuerId", IntegerType(), False),
                            StructField("IssuerId2", IntegerType(), False),
                            StructField("BusinessDate", StringType(), True),
                            StructField("StateCode", StringType(), True),
                            StructField("SourceName", StringType(), True),
                            StructField("NetworkName", StringType(), True),
                            StructField("NetworkURL", StringType(), True),
                            StructField("custnum", StringType(), True),
                            StructField("MarketCoverage", StringType(), True),
                            StructField("DentalOnlyPlan", StringType(), True)])

print(f"C. Create third structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as")
print(f"IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan,RejectRows")
# Hint: Do it carefully without making typo mistakes. Fields issuerid, issuerid2 should be of IntegerType, businessDate should be StringType and all other fields are StringType.

instructtype2a = StructType([StructField("IssuerId", IntegerType(), False),
                            StructField("IssuerId2", IntegerType(), False),
                            StructField("BusinessDate", StringType(), True),
                            StructField("StateCode", StringType(), True),
                            StructField("SourceName", StringType(), True),
                            StructField("NetworkName", StringType(), True),
                            StructField("NetworkURL", StringType(), True),
                            StructField("custnum", StringType(), True),
                            StructField("MarketCoverage", StringType(), True),
                            StructField("DentalOnlyPlan", StringType(), True),
                            StructField("RejectRows", StringType(), True)])

print(f"20. Create dataframe using csv module accessing the insuranceinfo1.csv file and remove the footer from both using header true and remove the footer using dropmalformed ")
print(f"options and apply the schema of the structure type created in the step 19.A")
path1 = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo1.csv"
path2 = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo2.csv"

insuredf1=spark.read.csv(path1,mode='dropmalformed',schema=instructtype1,header=True)
insuredf1.printSchema()
insuredf1.show(20,False)
print(f"Create another dataframe using csv module accessing the insuranceinfo2.csv file. Remove header from dataframes using header true and remove footer using dropmalformed options")
print(f"and apply the schema of the structure type created in the # step 19.B")
# Note: for insuranceinfo2.csv as the BusinessDate column format is dd-MM-yyyy, we need to explicitly convert it to yyyy-MM-dd format using to_date() function.

insuredf2_t=spark.read.csv(path2,mode='dropmalformed',schema=instructtype2,header=True)
insuredf2=insuredf2_t.withColumn("BusinessDate",to_date("BusinessDate",'MM-dd-yyyy'))
insuredf2.printSchema()
insuredf2.show(20,False)
print(f"Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from the dataframe using header true, permissive options and apply")
print(f"the schema of the structure type created in the step 19.C with the csv options of I. columnnameofcorruptrecord and store the rejected data in a new DF")
# Eg: ,,,,,,,13,SHOP (Small Group) this record has to be rejected
# II. Ignoreleading and trailing whitespaces
# ,,,,,,,13, SHOP (Small Group) ,Yes

insuredf3_t=spark.read.csv(path2,mode='permissive',schema=instructtype2a,header=True,columnNameOfCorruptRecord='RejectRows',ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True)
insuredf3=insuredf3_t.withColumn("BusinessDate",to_date("BusinessDate",'MM-dd-yyyy'))
insuredf3.cache()

reject_to_source_system=insuredf3.where("RejectRows is not null").select("RejectRows")
reject_to_source_system.show(20,False)
clean_data_insuredf3=insuredf3.where("RejectRows is null").drop("RejectRows").show(5,False)

print(f"Finally store the rejected records in a new dataframe in a single csv file")
reject_to_source_system.coalesce(1).write.mode("overwrite").csv("file:///home/hduser/Documents/hackathon_data_2023/RejectRows/")

print(f"Apply the below DSL functions in the two DFs created (A and B dataframes) in step 20 , after merging these 2 dataframes (A and B dataframes)")
insuredf_merged = insuredf1.union(insuredf2)
insuredf_merged.show(20)

print(f"a. Rename the fields StateCode and SourceName as stcd and srcnm respectively")
insuredf3_renamecol=insuredf_merged.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")
insuredf3_renamecol.show(5)

print(f"b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field. Hint : Cast to string and concat")
insuredf3_concatcol=insuredf3_renamecol.withColumn("issueridcomposite",concat(col("IssuerId").cast("string"),col("IssuerId2").cast("string")))
insuredf3_concatcol.show(5)

print(f"c. Remove DentalOnlyPlan column")
insuredf3_dropcol=insuredf3_concatcol.drop("DentalOnlyPlan")
insuredf3_dropcol.show(5)

print(f"d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively")
insuredf3_addcol=insuredf3_dropcol.withColumn("sysdt",current_date()).withColumn("systs",current_timestamp())
insuredf3_addcol.show(10,False)

print(f"Try the below interesting usecases seperately")
print(f"---------------------------------------------")
print(f"i. Identify all the column names and store in an List variable – use columns function")

column_names = insuredf3_addcol.columns
print(column_names)

print(f"ii. Identify all columns with datatype and store in a list variable and print using dtypes function ")
data_types = insuredf3_addcol.dtypes
print(data_types)

print(f"iii. Identify all integer columns alone and store in a list variable and print")
int_data_types_only = [col_name for col_name, col_type in insuredf3_addcol.dtypes if col_type == "int"]
print(int_data_types_only)

print(f"iv. Select only the integer columns identified in the above statement and show 10 records in the screen. Hint: add all the column names in a list and apply it in df.select(list)")
insuredf3_addcol.select(int_data_types_only)
insuredf3_addcol.show(10)

print(f"v. Identify the additional column in the reject dataframe created in step 20 above by subtracting the columns between dataframe1 and dataframe3")
print(f"created in step20. Hint: use columns function, then convert into set type and then do a subtraction")

columns_set1 = set(insuredf1.columns)
columns_set2 = set(insuredf3.columns)
additional_columns = columns_set2 - columns_set1
print(list(additional_columns))

print(f"22. Take the DF created in step 21.d and Remove the rows contains null in any one of the field and count the number of rows which contains all columns with some value")
print(f"Total records in this enriched dataframe insuredf3 is :{insuredf3_addcol.count()}")

insuredf3_anynull = insuredf3_addcol.na.drop(how="any")
print(f"Total records in this enriched dataframe insuredf3 after removing any null columns :{insuredf3_anynull.count()}")

insuredf3_allnonulls=insuredf3_addcol.na.drop(how="all")
insuredf3_allnonulls.show(10,False)
print(f"Total records in this enriched dataframe(insuredf3_addcol) with all columns with some value :{insuredf3_allnonulls.count()}")

print(f"23 Custom Method creation: Create a package (org.inceptez.hack), module (allmethods), method (remspecialchar) to remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]")
# Hint: First create the function/method directly and then later add in a seperate pkg.module
# a. Method should take 1 string argument and 1 return of type string
# b. Method should remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
# Hint: Use python regular expression function ie “re” function, usage of [] symbol should use \\ escape sequence. Eg: regexp = re.sub("[;\\@%-^/:~,*?\"<>|&'0-9]",'',a)
# c. For eg. If I pass to the above method value as Pathway - 2X (with dental) it has to return Pathway X with dental as output.

print(f"24. Import the package and refer the method generated in step 23 as a udf for invoking in the DSL function")
from org.inceptez.hackathon.allmethods import remspecialchar,writetofile
input_string = "Pathway - 2X (with dental)"
print(f" The string before cleansing :{input_string}")
cleaned_string = remspecialchar(input_string)
print(f" The string after cleansing :{cleaned_string}")

print(f" 25. Call the udf remspecialchar in the DSL by passing NetworkName column as an argument to get the special characters removed DF")
from pyspark.sql.functions import udf
#Convert the above function as a user defined function (which is DF-DSL ready)
remspecialcharfunc=udf(remspecialchar, StringType())
writetofilefunc=udf(writetofile)

insuredf3_allnonulls_enriched_df=insuredf3_allnonulls.withColumn("NetworkName",remspecialcharfunc("NetworkName"))
#insuredf3_allnonulls_enriched_df.show(10)

print(f" Step 25 to cleanse NetworkName column from insuredf3_allnonulls dataframe is completed ")
print(f"26. Save the DF generated in step 25 in JSON format into HDFS with overwrite option")

print(f"27. Save the DF generated in step 25 into CSV and json format with header name as per the DF and delimited by ~ into HDFS with overwrite option")
writetofilefunc('json','/user/hduser/sparkhack2/insuredf3_allnonulls_enriched_df.json','','overwrite','insuredf3_allnonulls_enriched_df')
print(f"Call a generic function to save the dataframe insuredf3_allnonulls_enriched_df as json format into HDFS location - completed")

writetofilefunc('csv','/user/hduser/sparkhack2/insuredf3_allnonulls_enriched_df.csv','~','overwrite','insuredf3_allnonulls_enriched_df')
print(f"Call a generic function to save the dataframe insuredf3_allnonulls_enriched_df as CSV format into HDFS location - completed")

# Note: Create a generic function namely writeToFile that should have 5 arguments passed as sparksession, filetype, location, delimiter, mode
# and call this function in the above step 26 and 27 to save the data rather than calling the write.csv and write.json directly.

print(f"28. Save the DF generated in step 25 into hive external table and append the data without overwriting it")

insuredf3_allnonulls.write.mode("append").saveAsTable("default.insuredf3_allnonulls_enriched")

print(f"Stored the DF insuredf3_allnonulls_enriched into Hive table default.insuredf3_allnonulls_enriched ")
result = spark.sql("SELECT * FROM default.insuredf3_allnonulls_enriched")
result.show(10)

print(f"4. Tale of handling RDDs, DFs and TempViews")
print(f"===========================================")
print(f"Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF, register to use in Spark SQL, Transform, Aggregate, store in disk/DB")
print(f"-------------------------------------------------------------------------------------------------------------------------------------------------------")
#Use RDD functions:
print(f"29. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates,")
print(f"this file contains 2 type of data one with 5 columns contains customer master info and other data with statecode and description of 2 columns")
path = "file:///home/hduser/Documents/hackathon_data_2023/custs_states.csv"
custstates = spark.sparkContext.textFile(path)
custstates.take(10)

print(f"30. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data and second RDD namely statesfilter should be only loaded with 2 columns data")

def process_line(line):
    # Assuming customer master info has 5 columns
    if len(line.split(',')) == 5:
        return ("customer", line)
    # Assuming state data has 2 columns
    elif len(line.split(',')) == 2:
        return ("state", line)

processed_data = custstates.map(process_line)

custfilter = processed_data.filter(lambda x: x[0] == "customer").map(lambda x: x[1])
statesfilter = processed_data.filter(lambda x: x[0] == "state").map(lambda x: x[1])

print("Filtered customer RDD from customer data *****")
custfilter.take(10)

print("Filtered State RDD from customer data ******")
statesfilter.take(10)

print(f"Use DSL functions ")
print(f"31. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF custstatesdf, this file contains 2 type of data one with 5 columns contains customer")
print(f"master info and other data with statecode and description of 2 columns")

custstatesdf = spark.read.csv(path)
custstatesdf.show(20)

print(f"32. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data and second DF namely statesfilterdf should be only loaded with 2 columns data")
# Hint: Use filter/where DSL function to check isnull or isnotnull to achieve the above functionality then rename, change the type and drop columns in the above 2 DFs accordingly.

print("Filtered customer DF from customer data with enriched cloumn names  *****")
custfilterdf = custstatesdf.filter(col("_c0").isNotNull() & col("_c1").isNotNull() & col("_c2").isNotNull() & col("_c3").isNotNull() & col("_c4").isNotNull()) \
              .select(col("_c0").alias("custid"), col("_c1").alias("fname"),col("_c2").alias("lname"), col("_c3").alias("age"),col("_c4").alias("profession"))
custfilterdf.show()

print("Filtered State DF from customer data with enriched column names  ******")
statesfilterdf = custstatesdf.filter(col("_c2").isNull() & col("_c3").isNull()).select(col("_c0").alias("statecode"), col("_c1").alias("statedesc"))
statesfilterdf.show()

print(f"se SQL Queries")
print(f"33. Register the above filtered customer and state data of two DFs as temporary views as custview and statesview")
custfilterdf.createOrReplaceTempView("custview")
spark.sql("select * from custview").show(5)

statesfilterdf.createOrReplaceTempView("statesview")
spark.sql("select * from statesview").show(5)

print(f"34. Register the DF generated in step 21.d as a tempview namely insureview")
insuredf3_addcol.createOrReplaceTempView("insureview")
spark.sql("select * from insureview").show(5)

print(f"35. Import the package and refer the method created in step 23 in the name of remspecialcharudf using spark udf registration")
spark.udf.register("remspecialcharPySQLUDF",remspecialchar)

print(f"36. Write an SQL query with the below processing")
#Set the spark.sql.shuffle.partitions to 4 ")
print(f"a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname")

insureview_sqludf = spark.sql("SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname FROM insureview")
insureview_sqludf.show(10)

print(f"b. Add current date, current timestamp fields as curdt and curts")
insureview_sqludf_ts = spark.sql("SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname, current_date() as curdt, current_timestamp() as curts FROM insureview")
insureview_sqludf_ts.show(10)

print(f"c. Extract the year and month from the businessdate field and get it as 2 new fields # called yr,mth respectively")

insureview_sqludf_ts_upd = spark.sql("""
                                      SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname,
                                      current_date() as curdt,
                                      current_timestamp() as curts,
                                      year(businessdate) as yr,
                                      month(businessdate) as mth     
                                      FROM insureview
                                    """)
insureview_sqludf_ts_upd.show(10)

print(f"d. Extract from the protocol either http/https from the NetworkURL column, if http then print http non secured if https then secured # else no protocol found then display noprotocol")
# For Eg: if http://www2.dentemax.com/ then show http non secured # else if https://www2.dentemax.com/ then http secured
# else if www.bridgespanhealth.com then show as no protocol store in a column called protocol.

insureview_sqludf_ts_upd1 = spark.sql("""
                                      SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname,
                                      current_date() as curdt, current_timestamp() as curts,
                                      year(businessdate) as yr, month(businessdate) as mth,   
                                      CASE
                                         WHEN NetworkURL LIKE 'http%' THEN 'http non secured'
                                         WHEN NetworkURL LIKE 'https%' THEN 'http secured'
                                         ELSE 'no protocol'
                                      END AS protocol  
                                      FROM insureview
                                    """)
insureview_sqludf_ts_upd1.show(10)

print(f"e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column from statesview with age,profession")
# column from custview . Do an Inner Join of insureview with statesview using stcd=stated and join insureview with custview using custnum=custid.

insureview_sqludf_ts_final = spark.sql("""
                                      SELECT i.*, remspecialcharPySQLUDF(NetworkName) as cleannetworkname,
                                      current_date() as curdt, current_timestamp() as curts,
                                      year(businessdate) as yr, month(businessdate) as mth,   
                                      CASE
                                         WHEN NetworkURL LIKE 'http%' THEN 'http non secured'
                                         WHEN NetworkURL LIKE 'https%' THEN 'http secured'
                                         ELSE 'no protocol'
                                      END as protocol,
                                      s.statedesc,
                                      c.age,
                                      c.profession
                                      FROM insureview i
                                      INNER JOIN statesview s ON i.stcd = s.statecode
                                      INNER JOIN custview c ON i.custnum = c.custid                                      
                                    """)
insureview_sqludf_ts_final.show(10)

print(f"37 Store the above selected Dataframe in Parquet formats in a HDFS location as a single file")
path = "hdfs:///user/hduser/sparkhack2/insureview_sqludf_ts_parquet"
insureview_sqludf_ts_final.coalesce(1).write.parquet(path, mode="overwrite")

print(f" Writing of dataframe as paraquet format is done into HDFS - /user/hduser/sparkhack2/insureview_sqludf_ts_parquet")

insureview_sqludf_ts_final.createOrReplaceTempView("insureview_upd")

print(f"38. Write an SQL query to identify average age, count group by statedesc, protocol, profession including a seqno column added which should have running sequence")
print(f"number partitioned based on protocol and ordered based on count descending and display the profession whose second highest count of a given state and protocol")
# For eg.
# Seqno,Avgage,count,statedesc,protocol,profession
# 1,48.4,10000, Alabama,http,Pilot
# 2,72.3,300, Colorado,http,Economist
# 1,48.4,3000, Atlanta,https,Health worker
# 2,72.3,2000, New Jersey,https,Economist
'''
professional_result_df=spark.sql("""
WITH ranked_data AS (
  SELECT
    statedesc,
    protocol,
    profession,
    AVG(age) AS avg_age,
    COUNT(*) AS count,
    ROW_NUMBER() OVER (PARTITION BY protocol ORDER BY COUNT(*) DESC) AS seqno
  FROM
    insureview_upd
  GROUP BY
    statedesc, protocol, profession
)
SELECT
  statedesc,
  protocol,
  profession,
  avg_age,
  count,
  seqno
FROM
  ranked_data
ORDER BY
  statedesc, protocol
""")

professional_result_df=spark.sql("""
    SELECT s.statedesc,
           i.protocol,
           c.profession,
           AVG(c.age) AS avg_age,
           COUNT(*) AS count,
           ROW_NUMBER() OVER (PARTITION BY i.protocol, c.profession ORDER BY COUNT(*) DESC) AS seqno
    FROM insureview_upd i
    INNER JOIN statesview s ON i.stcd = s.statecode
    INNER JOIN custview c ON i.custnum = c.custid
    GROUP BY s.statedesc, i.protocol,c.profession
    """)
'''
professional_result_df=spark.sql("""select * from 
                                                (select row_number() over(partition by protocol order by count(Age) desc) seqno,
                                                    avg(Age) avg_age, count(Age) as count, statedesc, protocol, Profession
                                                    from insureview_upd
                                                    group by statedesc, protocol, Profession) tmp
                                                 where seqno <=2 """)
professional_result_df.show(10)
'''
print(f" Determine count, avgage and seqno by statedesc, protocol, profession ")

professional_result_df = spark.sql("""
    SELECT statedesc,
           protocol,
           profession,
           AVG(age) as avg_age,
           COUNT(*) as count,
           ROW_NUMBER() OVER (PARTITION BY protocol ORDER BY COUNT(*) DESC) as seqno
    FROM insureview_upd
    GROUP BY statedesc, protocol, profession
""")
professional_result_df.show(2)

professional_result_df.createOrReplaceTempView("insure_highest_prof")
print(" Determine and show second highest profession by statedesc, protocol")
second_highest_prof_df = spark.sql("""
    SELECT statedesc,
           protocol,
           profession,
           COUNT(*) as second_highest_count
    FROM insure_highest_prof
    WHERE seqno = 2
    GROUP BY statedesc, protocol
""")
second_highest_prof_df.show(5)
'''
#39. Store the DF generated in step 38 into MYSQL table insureaggregated.
professional_result_df.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="insureaggregated",mode="overwrite",properties={"driver": 'com.mysql.cj.jdbc.Driver'})
print(f"39. Store the final dataframe generated in step 38 into MYSQL table custdb.insureaggregated - completed ")

print(f"40. Test the code in REPL/PyCharm and save it as a .py file and try submit with driver memory of 512M, number of executors as 4, executor memory as 1GB and executor cores with 2 cores")
print(f"")
shutil.make_archive("/home/hduser/Documents/hackathon_data_2023/hackathon","zip", root_dir="/home/hduser/PycharmProjects/Hackathon2023_Mari")
print(f" Packaging of the Hackathon application code from /home/hduser/PycharmProjects/Hackathon2023_Mari package is archived and saved as /home/hduser/Documents/hackathon_data_2023/hackathon.zip")

print(f"")
print(f"The application code is ready to trigger using the below spark-submit command")
print(f"spark-submit --driver-memory 512M --executor-memory 1g --executor-cores 4 --py-files /home/hduser/Documents/hackathon_data_2023/hackathon.zip "
      f"--jars /home/hduser/install/mysql-connector-java.jar /home/hduser/PycharmProjects/Hackathon2023_Mari/org/inceptez/hackathon/Hackathon_Mari.py")

print(f"")
print(f" ***  Hurray !!! The Hackathon execution is complete *** ")

