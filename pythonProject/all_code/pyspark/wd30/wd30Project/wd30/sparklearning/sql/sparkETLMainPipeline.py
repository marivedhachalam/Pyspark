#Packaging, Deployment & Execution of the code
#shutil.make_archive("/home/hduser/project_folder","zip", root_dir="/home/hduser/PycharmProjects/wd30Project/")
#spark-submit --py-files /home/hduser/project_folder/wd30Project.zip --jars /home/hduser/install/mysql-connector-java.jar /home/hduser/project_folder/sparkETLMainPipeline.py file:///home/hduser/hive/data/custsmodified file:///home/hduser/hive/data/txns /home/hduser/connection.prop
import sys
#This sparkETLMainPipeline.py is a Modernized/Standardized code of spark_sql_ETL_ELT_2.py (Core Essence the ETL program spark_sql_ETL_ELT_2.py)
#inline function (scope of this function is within this module)

def reord_cols(df):#Inline Function
    return df.select("id","custprofession","custage","custlname","custfname")

def enrich(df):#Inline Function
    enrich_addcols_df6 = df.withColumn("curdt", current_date()).withColumn("loadts",current_timestamp())
    enrich_ren_df7 = enrich_addcols_df6.withColumnRenamed("srcsystem", "src")
    # Concat to combine/merge/melting the columns
    # try with withColumn (that add the derived/combined column in the last)
    enrich_combine_df8 = enrich_ren_df7.withColumn("nameprof",concat("custfname", lit(" is a "), "custprofession")).drop("custfname")
    # Splitting of Columns to derive custfname
    enrich_combine_split_df9 = enrich_combine_df8.withColumn("custfname", split("nameprof", ' ')[0])
    # Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
    enrich_combine_split_cast_reformat_df10 = enrich_combine_split_df9.withColumn("curdtstr", col("curdt").cast("string")).\
        withColumn("year", year(col("curdt"))).withColumn("curdtstr",concat(substring("curdtstr", 3, 2), lit("/"),substring("curdtstr", 6, 2))).\
        withColumn("dtfmt", date_format("curdt", 'yyyy/MM/dd hh:mm:ss'))
    return enrich_combine_split_cast_reformat_df10

def pre_wrangle(df):# Inline function
    return df.select("id", "custprofession", "custage", "src", "curdt")\
        .groupBy("custprofession") \
        .agg(avg("custage").alias("avgage")) \
        .where("avgage>49") \
        .orderBy("custprofession")

def prewrang_anal(df):# Inline function
    sample1=df.sample(.2,10)
    smry=df.summary()
    coorval=df.corr("custage","custage")
    covval=df.cov("custage","custage")
    freqval = df.freqItems(["custprofession", "agegroup"], .4)
    return sample1,smry,coorval,covval,freqval

def aggregate_data(df):
    return df.groupby("year", "agegroup", "custprofession").agg(max("curdt").alias("max_curdt"), min("curdt").alias("min_curdt"),
                                                       avg("custage").alias("avg_custage"),
                                                       mean("custage").alias("mean_age"),
                                                       countDistinct("custage").alias("distinct_cnt_age"))\
                                                       .orderBy("year", "agegroup", "custprofession", ascending=[False, True, False])

def standardize_cols(df):
    srcsys='Retail'
    #adding columns
    reord_added_df3=df.withColumn("srcsystem",lit(srcsys))
    #replacement of column(s)
    reord_added_replaced_df4=reord_added_df3.withColumn("custfname",col("custlname"))#preffered way if few columns requires drop
    # removal of columns
    chgnumcol_reord_df5=reord_added_replaced_df4.drop("custlname")#preffered way if few columns requires drop
    #achive replacement and removal using withColumnRenamed
    #chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumnRenamed("custage","age").show()#preffered way if few columns requires drop
    return chgnumcol_reord_df5

#1. I will start devlop the major functionalities for performing DE/ETL pipeline, Inside the below main method
#in a form of inline code, create & call inline functions or create & use/directly use the (existing) reusable functions
#java public static void main()
#scala def main(args:Array[String])
def main(arg):
    print("define spark session object (inline code)")
    spark = SparkSession.builder\
       .appName("Very Important SQL End to End App") \
       .config("spark.jars","/home/hduser/install/mysql-connector-java.jar")\
       .enableHiveSupport()\
       .getOrCreate()

    print("Set the logger level to error")
    spark.sparkContext.setLogLevel("ERROR")
    print("1. Data Munging")
    print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
    #I will first take some sample data or actual data and analyse about the columns, datatype, values, nulls, duplicates(low/high cardinality), format
    #statistical analysis - min/max/difference/mean(mid)/counts
    print("b. Combining Data + Schema Evolution/Merging (Structuring)")
    print("b.1. Combining Data- Reading from a path contains multiple pattern of files")
    print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
    print("b.3. Schema Merging (Structuring) - Schema Merging data with different structures (we know the structure of both datasets)")
    print("b.4. Schema Evolution (Structuring) - source data is evolving with different structure")
    print("c.1. Validation (active)- DeDuplication")
    #inline code
    custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                  StructField("custfname", StringType(), False),
                                  StructField("custlname", StringType(), True),
                                  StructField("custage", ShortType(), True),
                                  StructField("custprofession", StringType(), True)])

    #Let us clean and get the right data for further consideration
    #drop the unwanted/culprit data while creating the df
    #culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
    #custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)#inline code
    #metadata driven - {"activity":"read","typesrc":"csv","loc":"file:///home/hduser/..","dropmf":"No","strct":"file:///home/hduser/.."}
    #config driven - conf
    #py/java/....> json -> function calls -> reusable function
    custdf_clean=read_data('csv',spark,arg[1],custstructtype1,'dropMalformed')#reusable function calling
    custdf_clean=optimize_performance(spark,custdf_clean,4,True,True,2)#reusable function calling
    #{"datatype":"csv","srcdatainfo":"file:///home/hduser/hive/data/custsmodified","engine","bigquery"}
    custdf_clean.printSchema()
    custdf_clean.show(2,False)
    print("*******Dropping Duplicates of cust data**********")
    dedup_dropduplicates_df=deDup(custdf_clean,["custage"],[False],["id"])#reusable function calling
    dedup_dropduplicates_df.where("id=4000003").show(4)

    txnsstructtype2=StructType([StructField("txnid",IntegerType(),False),StructField("dt",StringType()),StructField("custid",IntegerType()),StructField("amt",DoubleType()),StructField("category",StringType()),StructField("product",StringType()),StructField("city",StringType()),StructField("state",StringType()),StructField("spendby",StringType())])
    txns=read_data('csv',spark,arg[2],txnsstructtype2,'dropMalformed')
    txns_clean_optimized = optimize_performance(spark, txns, 1, False, False, 1) #reusable function calling
    print("*******Dropping Duplicates of txns data**********")
    txns_dedup=deDup(txns_clean_optimized,["dt","amt"],[False,False],["txnid"])#reusable function calling
    txns_dedup.show(2)

    print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
    print("Replace (na.replace) the key with the respective values in the columns "
           "(another way of writing Case statement)")
    prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"prof not defined"}
    dedup_dropfillreplacena_clensed_scrubbed_df1=munge_data(dedup_dropduplicates_df,prof_dict,["id"],["custlname","custprofession"],["custprofession"],'any')
    dedup_dropfillreplacena_clensed_scrubbed_df1.show()

    print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
    reord_df2=reord_cols(dedup_dropfillreplacena_clensed_scrubbed_df1)#inline function creation & calling
    #reord_df2=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id", "custprofession", "custage", "custlname", "custfname")#inline code
    reord_df2.show(10,False)
    #Convert the below code as a inline function
    munged_df=standardize_cols(reord_df2)
    #Try to do this -> convert into inline function
    #reord_df2 -> chgnumcol_reord_df5
    print("********************data munging completed****************")

    #TRANSFORMATION PART#
    ###########Data processing or Curation or Transformation Starts here###########
    print("***************2. Data Enrichment (values)-> Add, Rename, combine(Concat), Split, Casting of Fields, Reformat, "
          "replacement of (values in the columns) - Makes your data rich and detailed *********************")
    munged_enriched_df=enrich(munged_df)
    munged_enriched_df.show(4)

    print("***************3. Data Customization & Processing (Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")
    print("Data Customization can be achived by using UDFs - User Defined Functions")
    #Step1: Create a Python function
    #Step2: Importing UDF spark library
    from pyspark.sql.functions import udf
    #Step3A: Converting the above function using UDF into user-defined function (DSL)
    age_custom_validation = udf(age_conversion)
    #Step4: New column deriviation called age group, in the above dataframe (Using DSL)
    custom_agegrp_munged_enriched_df = munged_enriched_df.withColumn("agegroup",age_custom_validation("custage"))
    custom_agegrp_munged_enriched_df.show(2)

    print("***************4. Core Data Processing/Transformation (Level1) (Pre Wrangling) Curation -> "
          "filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytics *********************")
    print("Transformation Functions -> select, filter, sort, group, aggregation, having, transformation/analytical function, distinct...")
    pre_wrangled_customized_munged_enriched_df=pre_wrangle(custom_agegrp_munged_enriched_df)#inline function
    print(pre_wrangled_customized_munged_enriched_df)#We can use thid DF to store in some target systems
    print("Filter rows and columns")
    filtered_nochildren_rowcol_df_for_further_wrangling1=fil(custom_agegrp_munged_enriched_df,"agegroup<>'Children'")\
        .select("id","custage","curdt","custfname","year","agegroup")#reusable function call and inline code also
    filtered_nochildren_rowcol_df_for_further_wrangling1.show(2)
    dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2=fil(custom_agegrp_munged_enriched_df,"agegroup<>'Children'")
    aggr_df=aggregate_data(dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2)

    print("Tell me the average age of the above customer is >35 (adding having)")
    aggr_filter_df=fil(aggr_df,"avg_custage>35")#having clause
    aggr_filter_df.show(2)

    print("Analytical Functionalities")
    #Data Random Sampling:
    #randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)
    sampledf,summarydf,corrval,covval,freqdf=prewrang_anal(custom_agegrp_munged_enriched_df)
    sampledf.show(2)
    summarydf.show(2)
    print(f"co-relation value of age is {corrval}")
    print(f"co-variance value of age is {covval}")
    freqdf.show(2)

    masked_df=mask_fields(custdf_clean, ["custlname", "custfname"], md5)

    print("***************5. Core Data Curation/Processing/Transformation (Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")
    denormalizeddf=custdf_clean.alias("c").join(txns_dedup.alias("t"),on=[col("c.id")==col("t.custid")],how="inner")
    denormalizeddf.show(2)
    rno_txns3 = denormalizeddf.select("*", row_number().over(Window.orderBy("dt")).alias("sno"))
    rno_txns3.show(2)

    #Usecase: Convert the below inline code to either inline/reusable functions
    print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
    print("Random Sample DF to File System")
    sampledf.write.mode("overwrite").csv("/user/hduser/randomsample1_for_consumption3")
    print("Denormalized DF to File System")
    denormalizeddf.write.mode("overwrite").json("/user/hduser/leftjoined_aggr2")
    print("Cleansed DF to Hive Table")
    custdf_clean.write.mode("overwrite").partitionBy("custprofession").saveAsTable("default.cust_prof_part_tbl")
    print("Masked DF to File System")
    masked_df.write.mode("overwrite").csv("file:///home/hduser/masked_cust_data")
    print("Aggregated DF to File System")
    writeRDBMSData(aggr_filter_df, arg[3], 'custdb', 'cust_age_aggr','overwrite')
    #aggr_filter_df.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="cust_age_aggr",
    #           mode="overwrite", properties={"driver": 'com.mysql.jdbc.Driver'})
    print("Spark App1 Completed Successfully")

if __name__=="__main__":
    if(len(sys.argv)==4):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        from pyspark.sql.types import *
        from pyspark.sql.window import *
        from wd30.sparklearning.sql.reusable_functions import *
        main(sys.argv)
    else:
        print("No enough argument to continue running this program")
        exit(1)