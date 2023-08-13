# PySpark hackathon 2023
from pyspark.sql.session import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import year, month, col, udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
import shutil
from org.inceptez.hack.allmethods import *

spark = SparkSession.builder.master("local[2]")\
    .appName("WD30 Hackathon Mari")\
    .config("spark.jars","file:///home/hduser/install/mysql-connector-java.jar")\
    .config("spark.sql.shuffle.partitions", 100)\
    .enableHiveSupport()\
    .getOrCreate()

sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")
# 1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata

# path="hdfs:/user/hduser/sparkhack2/insuranceinfo1.csv"
path = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo1.csv"
# insuredata=sc.textFile(path).map(lambda x:x.split(","))
insuredata = sc.textFile(path)
cnt1 = insuredata.count()
# 2 remove header record
header = insuredata.first()
insuredata = insuredata.filter(lambda row: row != header)  # filter out the header

# 3 remove footer record
cnt3 = insuredata.count() - 1
insuredata = insuredata.zipWithIndex().filter(lambda x: x[1] != cnt3)
print(insuredata.collect())

# 4 Display the count and show few rows and check whether header and footer is removed.
cnt4 = insuredata.count() - 1
print(f"RDD count before removing header and trailer records :{cnt4}")
print(insuredata.first())  # should not be a header record
istailer = insuredata.zipWithIndex().filter(lambda x: x[1] == cnt4)
print(istailer.collect())  # should not be a tailer like record

# 5 Remove the blank lines in the rdd.
insuredata5 = insuredata.filter(lambda line: len(line[0].strip()) > 0)
cnt5 = insuredata5.count()
print(f"RDD count after removing the blank lines :{cnt5}")
#print(insuredata5.collect())

# 6 Map and split using ‘,’ delimiter.
insuredata6 = insuredata5.map(lambda line: line[0].split(",", -1))
print(insuredata6.collect())

# 7 Filter number of fields are equal to 10 columns only - analyze why we are doing this and provide your view here
num_fields = 10
insuredata7 = insuredata6.filter(lambda line: len(line) == num_fields)
print(insuredata6.collect())
cnt7 = insuredata7.count()
print(f"RDD count after selecting 10 columns only :{cnt7}")

# 8 Take the count of the RDD created in step 7 and step 1 and print how many rows are removed/rejected
# in the cleanup process of removing the number of fields does not #equals 10.
cnt8 = cnt1 - cnt7
print(f"Records removed / rejected from cleanup process :{cnt8}")


# 9 Create another RDD namely rejectdata and store the row that does not equals 10 columns.
# With a new column added in the first column called numcols contains number of columns

def add_new_column(row):
    numcols = len(row)
    return str(numcols) + ',' + str(row)
numcolval = insuredata6.filter(lambda line: len(line) != 10)
insuredata7 = numcolval.map(add_new_column)
print(insuredata7.collect())

#10 Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2
path = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo2.csv"
insuredata2 = sc.textFile(path)
print(f"rdd count : {insuredata2.count()}")

#11 Repeat from step 2 to 8 for this file also and create the schema rdd from the insuranceinfo2.csv and
# filter the records that contains blank or null IssuerId,IssuerId2 for eg: remove the records with pattern given below. # ,,,,,,,13,Individual,Yes
# with the field names used as per the header record in the file and apply to the above data to create schemaed RDD

# remove header record
header2 = insuredata2.first()
print(header2)
insuredata2a = insuredata2.filter(lambda row: row != header2)  # filter out the header
print(f"rdd count after header removed : {insuredata2a.count()}")

# remove footer record
cnt3 = insuredata2a.count() -1
insuredata2b = insuredata2a.zipWithIndex().filter(lambda x: x[1] != cnt3)
#print(insuredata2b.collect())
print(f"rdd count after trailer removed : {insuredata2b.count()}")

# Display the count and show few rows and check whether header and footer is removed.
cnt4 = insuredata2b.count()-1
print(f"RDD count before removing header and trailer records :{cnt4}")
print(insuredata2b.first())  # should not be a header record
istailer2 = insuredata2b.zipWithIndex().filter(lambda x: x[1] == cnt4)
print(istailer2.collect())  # should not be a tailer like record

# Remove the blank lines in the rdd.
insuredata2c = insuredata2b.filter(lambda line: len(line[0].strip()) > 0)
cnt5 = insuredata2c.count()
print(f"RDD count after removing the blank lines :{cnt5}")
#print(insuredata2c.collect())

# Map and split using ‘,’ delimiter.
insuredata2d = insuredata2c.map(lambda line: line[0].split(",", -1))
#print(insuredata2d.collect())

#  Filter number of fields are equal to 10 columns only - analyze why we are doing this and provide your view here
insuredata2e = insuredata2d.filter(lambda line: len(line) == num_fields)
#print(insuredata2e.collect())
cnt7 = insuredata2e.count()
print(f"RDD count after selecting 10 columns only :{cnt7}")

# Take the count of the RDD created in step 7 and step 1 and print how many rows are removed/rejected
# in the cleanup process of removing the number of fields does not #equals 10.
cnt1 = insuredata2.count()
cnt8 = cnt1 - cnt7
print(f"Records removed / rejected from cleanup process :{cnt8}")

#and create the schema rdd from the insuranceinfo2.csv and
# filter the records that contains blank or null IssuerId,IssuerId2 for eg: remove the records with pattern given below. # ,,,,,,,13,Individual,Yes
# with the field names used as per the header record in the file and apply to the above data to create schemaed RDD

schema_insuredata2e = insuredata2e.map(lambda x:(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9]))
#print(schema_insuredata2e.collect())

##    return row[0].strip() == "" or row[0] is None and row[1].strip() == "" or row[1] is None

#insuredata2f = schema_insuredata2e.filter(lambda row: not is_blank_or_null_record(row))
insuredata2f = schema_insuredata2e.filter(lambda row: row[0].strip() != "" and row[0] is not None).filter(lambda row: row[1].strip() != "" and row[1] is not None)
#print(insuredata2f.collect())
cnt11 = insuredata2f.count()
print(f"Count of the schema RDD insured2 after cleanup :{cnt11}")

# 2. Data merging, Deduplication, Performance Tuning & Persistence
#============================================================================
#12. Merge the both header and footer removed RDDs derived in steps 8 and 11 into an RDD namely insuredatamerged
insuredatamerged = insuredata7.union(insuredata2f)
cnt12 = insuredatamerged.count()
print(f"Count of merged RDDs insuredata1 and insuredata2 after cleanup :{cnt12}")

#13 Persist the step 12 RDD to memory by serializing
insuredatamerged_persisted = insuredatamerged.persist(StorageLevel.MEMORY_ONLY)
#insuredatamerged.persist()

#14. Calculate the count of rdds created in step 8+11 and rdd in step 12, check whether they  are matching.
print(f"Count of insuredata1 after cleanup :{cnt7}")
print(f"Count of insuredata2 after cleanup :{cnt11}")
cnt8and11 = cnt7 + cnt11
print(f"Count of merged RDDs insuredata1 and insuredata2 after cleanup :{cnt12}")
print(f"Count of RDDs insuredata1 + insuredata2 after cleanup :{cnt8and11}")

if cnt12 != cnt8and11:
    print(f"Mismatch in the count before and after Merge process : {cnt8and11-cnt12}") #2931
else:
    print(f"Match in the count before and after Merge process : {cnt8and11}")

#15 Increase the number of partitions in the above rdd to 8 partitions and name it as insuredatarepart.
num_partitions = insuredatamerged.getNumPartitions()
print("Number of partitions:", num_partitions)

insuredatarepart = insuredatamerged.repartition(8)

num_partitions = insuredatarepart.getNumPartitions()
print("Number of partitions:", num_partitions)

#16 Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002
# based on the BusinessDate of 2019-10-01 and 2019-10-02 respectively using Filter function.
'''
keyed_rdd = insuredatarepart.keyBy(lambda record: record[2])  # the date is in the third field
#print(keyed_rdd.collect())
# Group partitions by business date
grouped_partitions = keyed_rdd.groupByKey()
#print(grouped_partitions.collect())

def filter_partition(partition_records, target_business_date):
    filtered_records = [record for record in partition_records if record[0] == target_business_date]
    return filtered_record
# Convert grouped partitions into a dictionary of business_date -> partition pairs
target_business_date = "01-01-2019"
#rdd_20191001 = grouped_partitions.flatMapValues(lambda partition: filter_partition(partition, target_business_date)).values().collect()
selected_partitions = grouped_partitions.filter(lambda partition: partition[0] == target_business_date)
rdd_20191001 = selected_partitions.flatMap(lambda partition: partition[1])
#print(rdd_20191001.collect())
target_business_date = "02-01-2019"
#rdd_20191002 = grouped_partitions.flatMapValues(lambda partition: filter_partition(partition, target_business_date)).values()
selected_partitions = grouped_partitions.filter(lambda partition: partition[0] == target_business_date)
rdd_20191002 = selected_partitions.flatMap(lambda partition: partition[1])
print(rdd_20191002.collect())
'''
target_business_date1 = '2019-10-01'
rdd_20191001 = insuredatarepart.filter(lambda x: x[2] == target_business_date1)
#print(rdd_20191001.collect())

target_business_date2 = '01-10-2019'
rdd_20191002 = insuredatarepart.filter(lambda x: x[2] == target_business_date2)
#print(rdd_20191002.collect())

#17. Store the RDDs created in step 15, 16 into HDFS location.
'''
hdfs_output_path = "hdfs:///user/hduser/sparkhack2/"

# Delete the file from HDFS
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

file_status = fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_output_path+"/insuredatarepart"), False)
insuredatarepart.saveAsTextFile(hdfs_output_path+"/insuredatarepart")
file_status = fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_output_path+"/rdd_20191001"), False)
rdd_20191001.saveAsTextFile(hdfs_output_path+"/rdd_20191001")
file_status = fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_output_path+"/rdd_20191002"), False)
rdd_20191002.saveAsTextFile(hdfs_output_path+"/rdd_20191002")

'''
#18 Convert the RDDs created in step 15 above into Dataframe namely insuredaterepartdf using
#rdd.toDF().toDF("IssuerId","IssuerId2","BusinessDate","StateCode","SourceName","Net
#workName","NetworkURL","custnum","MarketCoverage","DentalOnlyPlan") function
collist=("IssuerId","IssuerId2","BusinessDate","StateCode","SourceName","NetworkName","NetworkURL","custnum","MarketCoverage","DentalOnlyPlan")
insuredaterepartdf=insuredatarepart.toDF(collist)

#Part B - Spark DF & SQL (Step 3, 4 & 5) - 3. DataFrames operations (20% Completion) – Total 55%
# Apply Structure, DSL column management functions, transformation, custom udf & schema migration.
#19. Dataframe creation using the built in modules
# A. Create first structuretypes for all the columns as per the insuranceinfo1.csv with the columns such as
# IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan
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


# B. Create second structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as
# IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan
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

#C. Create third structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as
# IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan,RejectRows
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

# 20. Create dataframe using csv module accessing the insuranceinfo1.csv file and remove the footer from both using header true and remove the footer using dropmalformed
# options and apply the schema of the structure type created in the step 19.A
path1 = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo1.csv"
path2 = "file:///home/hduser/Documents/hackathon_data_2023/insuranceinfo2.csv"

insuredf1=spark.read.csv(path1,mode='dropmalformed',schema=instructtype1,header=True)
insuredf1.printSchema()
insuredf1.show(20,False)
# Create another dataframe using csv module accessing the insuranceinfo2.csv file. Remove header from dataframes using header true and remove footer using dropmalformed options
# and apply the schema of the structure type created in the # step 19.B
# Note: for insuranceinfo2.csv as the BusinessDate column format is dd-MM-yyyy, we need to explicitly convert it to yyyy-MM-dd format using to_date() function.

insuredf2_t=spark.read.csv(path2,mode='dropmalformed',schema=instructtype2,header=True)
insuredf2=insuredf2_t.withColumn("BusinessDate",to_date("BusinessDate",'MM-dd-yyyy'))
insuredf2.printSchema()
insuredf2.show(20,False)
# Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from the dataframe using header true, permissive options and apply
# the schema of the structure type created in the step 19.C with the csv options of I. columnnameofcorruptrecord and store the rejected data in a new DF.
# Eg: ,,,,,,,13,SHOP (Small Group) this record has to be rejected
# II. Ignoreleading and trailing whitespaces
# ,,,,,,,13, SHOP (Small Group) ,Yes

insuredf3_t=spark.read.csv(path2,mode='permissive',schema=instructtype2a,header=True,columnNameOfCorruptRecord='RejectRows',ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True)
insuredf3=insuredf3_t.withColumn("BusinessDate",to_date("BusinessDate",'MM-dd-yyyy'))
insuredf3.cache()

reject_to_source_system=insuredf3.where("RejectRows is not null").select("RejectRows")
reject_to_source_system.show(20,False)
clean_data_insuredf3=insuredf3.where("RejectRows is null").drop("RejectRows").show(5,False)

# Finally store the rejected records in a new dataframe in a single csv file.
reject_to_source_system.coalesce(1).write.mode("overwrite").csv("file:///home/hduser/Documents/hackathon_data_2023/RejectRows/")

#Apply the below DSL functions in the two DFs created (A and B dataframes) in step 20 , after merging these 2 dataframes (A and B dataframes).
insuredf_merged = insuredf1.union(insuredf2)
insuredf_merged.show(20)
# a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.

insuredf3_renamecol=insuredf_merged.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")

# b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field. Hint : Cast to string and concat.
insuredf3_concatcol=insuredf3_renamecol.withColumn("issueridcomposite",concat(col("IssuerId").cast("string"),col("IssuerId2").cast("string")))

#insuredf3_concatcol=insuredf3_renamecol.withColumn("issueridcomposite", concat("IssuerId", "IssuerId2").show(5)
# c. Remove DentalOnlyPlan column

insuredf3_dropcol=insuredf3_concatcol.drop("DentalOnlyPlan")
insuredf3_dropcol.show(5)

# d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.
insuredf3_addcol=insuredf3_dropcol.withColumn("sysdt",current_date()).withColumn("systs",current_timestamp())
insuredf3_addcol.show(10,False)

# Try the below interesting usecases seperately:
# i. Identify all the column names and store in an List variable – use columns function.

column_names = insuredf3_addcol.columns
print(column_names)

# ii. Identify all columns with datatype and store in a list variable and print –use dtypes function.
data_types = insuredf3_addcol.dtypes
print(data_types)

# iii. Identify all integer columns alone and store in an list variable and print.
int_data_types_only = [col_name for col_name, col_type in insuredf3_addcol.dtypes if col_type == "int"]
print(int_data_types_only)
# iv. Select only the integer columns identified in the above statement and show 10 records in the screen. Hint: add all the column names in a list and apply it in df.select(list)
insuredf3_addcol.select(int_data_types_only).show(10)

# v. Identify the additional column in the reject dataframe created in step 20 above by subtracting the columns between dataframe1 and dataframe3
#created in step20. Hint: use columns function, then convert into set type and then do a subtraction

columns_set1 = set(insuredf1.columns)
columns_set2 = set(insuredf3.columns)
additional_columns = columns_set2 - columns_set1
print(list(additional_columns))

#22. Take the DF created in step 21.d and Remove the rows contains null in any one of the field and count the number of rows which contains all columns with some value.
print(f"Total records in this dataframe insuredf3_addcol are :{insuredf3_addcol.count()}")

insuredf3_anynull = insuredf3_addcol.na.drop(how="any")
print(f"Total records in this dataframe insuredf3_addcol after removing any null columns :{insuredf3_anynull.count()}")

insuredf3_allnonulls=insuredf3_addcol.na.drop(how="all")
insuredf3_allnonulls.show(50,False)
print(f"Total records in this dataframe (insuredf3_allnonulls) with all columns with some value :{insuredf3_allnonulls.count()}")

#23 Custom Method creation: Create a package (org.inceptez.hack), module (allmethods), method (remspecialchar)
# Hint: First create the function/method directly and then later add in a seperate pkg.module
# a. Method should take 1 string argument and 1 return of type string
# b. Method should remove all special characters and numbers 0 to 9 - ? , / _ ( ) [ ]
# Hint: Use python regular expression function ie “re” function, usage of [] symbol should use \\ escape sequence. Eg: regexp = re.sub("[;\\@%-^/:~,*?\"<>|&'0-9]",'',a)
# c. For eg. If I pass to the above method value as Pathway - 2X (with dental) it has to return Pathway X with dental as output.

#24. Import the package and refer the method generated in step 23 as a udf for invoking in the DSL function.
from org.inceptez.hack.allmethods import *

input_string = "Pathway - 2X (with dental)"
print(f" string before cleansing :{input_string}")
cleaned_string = remspecialchar(input_string)
print(f" string after cleansing :{cleaned_string}")

print(f" 25. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF")
#step2: Import the udf from the spark sql functions library

#step3: Convert the above function as a user defined function (which is DF-DSL ready)
remspecialcharfunc=udf(remspecialchar, StringType())
writetofilefunc=udf(writetofile)

insuredf3_allnonulls_enriched_df=insuredf3_allnonulls.withColumn("NetworkName",remspecialcharfunc(col("NetworkName")))
#insuredf3_allnonulls_enriched_df.show(20)

print(f" Step 25 to cleanse NetworkName column from insuredf3_allnonulls dataframe is completed ")
#26. Save the DF generated in step 25 in JSON format into HDFS with overwrite option.
#data = insuredf3_allnonulls_enriched_df
print(f"Call a generic function to save the dataframe insuredf3_allnonulls_enriched_df as json format into HDFS location")
writetofilefunc('json','/user/hduser/sparkhack2/insuredf3_allnonulls_enriched_df.json','','overwrite','insuredf3_allnonulls_enriched_df')

# 27. Save the DF generated in step 25 into CSV format with header name as per the DF and delimited by ~ into HDFS with overwrite option.

print(f"Call a generic function to save the dataframe insuredf3_allnonulls_enriched_df as CSV format into HDFS location")
writetofilefunc('csv','/user/hduser/sparkhack2/insuredf3_allnonulls_enriched_df.csv','~','overwrite','insuredf3_allnonulls_enriched_df')
# Note: Create a generic function namely writeToFile that should have 5 arguments passed as sparksession, filetype, location, delimiter, mode
# and call this function in the above step 26 and 27 to save the data rather than calling the write.csv and write.json directly.

#28. Save the DF generated in step 25 into hive external table and append the data without overwriting it.
print(f"save DF into Hive table default.insuredf3_allnonulls_enriched_df ")
insuredf3_allnonulls.write.mode("append").saveAsTable("default.insuredf3_allnonulls_enriched")

result = spark.sql("SELECT * FROM default.insuredf3_allnonulls_enriched")
result.show(10)

#4. Tale of handling RDDs, DFs and TempViews (20% Completion) – Total 75%
# Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF, register to use in Spark SQL, Transform, Aggregate, store in disk/DB

#Use RDD functions:
# 29. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates,
# this file contains 2 type of data one with 5 columns contains customer master info and other data with statecode and description of 2 columns.
path = "file:///home/hduser/Documents/hackathon_data_2023/custs_states.csv"
custstates = spark.sparkContext.textFile(path)

#30. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data and second RDD namely statesfilter should be only loaded with 2 columns data.

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

print("Customer RDD:")
custfilter.take(10)

print("\nState RDD:")
statesfilter.take(10)

#Use DSL functions:
#31. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF custstatesdf, this file contains 2 type of data one with 5 columns contains customer
# master info and other data with statecode and description of 2 columns.

custstatesdf = spark.read.csv(path)
custstatesdf.show(20)

#32. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data and second DF namely statesfilterdf should be only loaded with 2 columns data.
# Hint: Use filter/where DSL function to check isnull or isnotnull to achieve the above functionality then rename, change the type and drop columns in the above 2 DFs accordingly.
'''
custfilterdf = custstatesdf.filter("size(_c0) = 5")
statesfilterdf = custstatesdf.filter("size(_c0) = 2")
custfilterdf = custfilterdf.selectExpr("_c0 as custid", "_c1 as name", "_c2 as age", "_c3 as address", "_c4 as phone")
statesfilterdf = statesfilterdf.selectExpr("_c0 as statecode", "_c1 as statedesc")

'''

custfilterdf = custstatesdf.filter(col("_c0").isNotNull() & col("_c1").isNotNull() & col("_c2").isNotNull() & col("_c3").isNotNull() & col("_c4").isNotNull()) \
              .select(col("_c0").alias("custid"), col("_c1").alias("fname"),col("_c2").alias("lname"), col("_c3").alias("age"),col("_c4").alias("profession"))
custfilterdf.show()

statesfilterdf = custstatesdf.filter(col("_c2").isNull() & col("_c3").isNull()).select(col("_c0").alias("statecode"), col("_c1").alias("statedesc"))
statesfilterdf.show()

#Use SQL Queries:
# 33. Register the above step 32 two DFs as temporary views as custview and statesview.

custfilterdf.createOrReplaceTempView("custview")
spark.sql("select * from custview").show(5)

statesfilterdf.createOrReplaceTempView("statesview")
spark.sql("select * from statesview").show(5)

# 34. Register the DF generated in step 21.d as a tempview namely insureview
insuredf3_addcol.createOrReplaceTempView("insureview")
spark.sql("select * from insureview").show(5)

# 35. Import the package and refer the method created in step 23 in the name of remspecialcharudf using spark udf registration.

spark.udf.register("remspecialcharPySQLUDF",remspecialchar)

# 36. Write an SQL query with the below processing # Set the spark.sql.shuffle.partitions to 4
# a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname

insureview_sqludf = spark.sql("SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname FROM insureview")
insureview_sqludf.show(10)

# b. Add current date, current timestamp fields as curdt and curts.
insureview_sqludf_ts = spark.sql("SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname, current_date() as curdt, current_timestamp() as curts FROM insureview")
insureview_sqludf_ts.show(10)
# c. Extract the year and month from the businessdate field and get it as 2 new fields # called yr,mth respectively.

insureview_sqludf_ts_upd = spark.sql("""
                                      SELECT *, remspecialcharPySQLUDF(NetworkName) as cleannetworkname,
                                      current_date() as curdt,
                                      current_timestamp() as curts,
                                      year(businessdate) as yr,
                                      month(businessdate) as mth     
                                      FROM insureview
                                    """)
insureview_sqludf_ts_upd.show(10)

# d. Extract from the protocol either http/https from the NetworkURL column, if http then print http non secured if https then secured # else no protocol found then display noprotocol.
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

#e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column from statesview with age,profession
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

# 37 Store the above selected Dataframe in Parquet formats in a HDFS location as a single # file.
path = "hdfs:///user/hduser/sparkhack2/insureview_sqludf_ts_parquet"
insureview_sqludf_ts_final.coalesce(1).write.parquet(path, mode="overwrite")

print(f" Writing of dataframe as paraquet format is done into HDFS")

insureview_sqludf_ts_final.createOrReplaceTempView("insureview_upd")

# 38. Write an SQL query to identify average age, count group by statedesc, protocol, profession including a seqno column added which should have running sequence
# number partitioned based on protocol and ordered based on count descending and display the profession whose second highest count of a given state and protocol.
# For eg.
# Seqno,Avgage,count,statedesc,protocol,profession
# 1,48.4,10000, Alabama,http,Pilot
# 2,72.3,300, Colorado,http,Economist
# 1,48.4,3000, Atlanta,https,Health worker
# 2,72.3,2000, New Jersey,https,Economist
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
# 39. Store the DF generated in step 38 into MYSQL table insureaggregated.
professional_result_df.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="insureaggregated",mode="overwrite",properties={"driver": 'com.mysql.cj.jdbc.Driver'})

# 40. Test the code in REPL/PyCharm and save it as a .py file and try submit with driver  memory of 512M, number of executors as 4, executor memory as 1GB and executor
# cores with 2 cores.
# spark-submit --driver-memory 512M --num-executors 4 --executor-memory 1G --executor-cores 2 Wd30_Hackathon.py

shutil.make_archive("/home/hduser/Documents/hackathon_data_2023/hackathon1","zip",root_dir="/home/hduser/PycharmProjects/pythonProject")
# /home/hduser/PycharmProjects/pythonProject/Wd30_Hackathon.py
