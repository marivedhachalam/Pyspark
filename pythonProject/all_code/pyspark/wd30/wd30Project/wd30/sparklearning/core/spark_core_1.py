if 1==1:
      print("Spark core Programming Learning (RDD transformations & Actions - not much important but few optimizations like "
            "caching, persisting, broadcasting, partitioning are all Very much important not for RDDs but for SQL)")
      print("1. How to create RDDs")
      print("2. How to transform RDDs")
      print("3. How to produce the result of RDDs in the console or in the filesystems")
      print("4. Architecture of Spark + Cluster Management of Spark + Spark Environment Terminologies + How to Optimize the RDDs and applicable for SQL also (important)")

      #first we have enter towards the context of spark core or sql programming using/adapting/adopting some spark libraries/framework
      #import pyspark.sql.session
      from pyspark.sql.session import SparkSession
      spark=SparkSession.builder.master("local[2]").appName("WD30 Spark Core Prog learning").enableHiveSupport().getOrCreate()
      #spark session object has already spark core context called sparkContext, sql context called sqlContext and hiveContext
      #a=10
      #b=a
      sc = spark.sparkContext #we are not instantiating rather just referring/renaming
      sc.setLogLevel("ERROR")
      #spark=SparkSession.builder.appName("project name and modulename").enableHiveSupport().getOrCreate()
      #pyspark.rdd.py class RDD find all functions
      print("1. How to create RDDs")
      #a. from different file sources, from another rdd, from memory, programmatically
      file_rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
      print(f"count of rows in lfs file {file_rdd1.count()}")
      hadoop_rdd=sc.textFile("hdfs:///user/hduser/empdata.txt")
      #Resilient Distributed Dataset/lazy evaluation/lazy execution/immutable/core spark abstraction/every spark program has some operations/functions (transformation/action) on RDD
      #Dataset can come from anywhere materialized into memory when action functions are called
      # Distribution can be achived with the partitions
      #Resilient - fault tolerance/recreation of the lost data we can get when the data loaded in the memory is lost (gracefully -Garbage Collector/gracelessly )
      # We can get the resiliency achieved with the help of Lineage (relation)
      # DAG will hold all lineages of a given program in spark in a form of RDD, Transformation, Actions, Lineages
      print(f"count of initial rows in hdfs source file {hadoop_rdd.count()}")
      print(hadoop_rdd.collect())
      print(hadoop_rdd.getNumPartitions())
      #['ArunKumar,chennai,33,2016-09-20,100000', 'Lara,chennai,55,2016-09-21,10000', 'vasudevan,banglore,43,2016-09-23,90000', 'irfan,chennai,33,2019-02-20,20000', 'basith,CHENNAI,29,2019-04-22']
      #[(1,2,3),(4,5,6)]
      #list(str)
      #Rather than using spark filter transformation, if i just write python program, how do i write
      lst1=hadoop_rdd.collect()
      #['ArunKumar,chennai,33,2016-09-20,100000', 'Lara,chennai,55,2016-09-21,10000', 'vasudevan,banglore,43,2016-09-23,90000', 'irfan,chennai,33,2019-02-20,20000', 'basith,CHENNAI,29,2019-04-22']
      for i in lst1:
       if 'chennai' in i.lower():
         print(i)

      #b. create an rdd from another rdd (since the source rdd is immutable, so we have to transform)
      #
      lam_filter_condition=lambda i:'chennai' in i.lower()

      def def_filter_condition(i):#dont prefer
            return 'chennai' in i.lower()
      #Here, filter is basically performing the for loop and Lambda/Normal function is performing the if condition, and we are combining filter, lambda together to perform the filter transformation
      filter_direct_rdd=hadoop_rdd.filter(lambda i:'chennai' in i.lower())#filter will run a for loop and if condition
      filter_lam_rdd=hadoop_rdd.filter(lam_filter_condition)#filter will run a for loop and if condition
      filter_def_rdd=hadoop_rdd.filter(def_filter_condition)#filter will run a for loop and if condition
      print(filter_direct_rdd.collect())
      print(filter_lam_rdd.collect())
      print(filter_def_rdd.collect())

      #c. RDDs can be created from memory
      file_rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")#cleaned by the GC once filter_direct_rdd is materialized into memory
      filter_direct_rdd=file_rdd1.filter(lambda i:'chennai' in i.lower())#filter will run a for loop and if condition
      #['ArunKumar,chennai,33,2016-09-20,100000', 'irfan,chennai,37,2019-02-20,10000', 'irfan,chennai,37,2019-02-20,10000', 'basith,CHENNAI,29,2019-04-22,20000']
      filter_direct_rdd.cache()#dont allow GC to clean the data from the memory, so the child rdds will get the data from the memory
      select_dt_amt_rdd=filter_direct_rdd.map(lambda i:(i.split(",")[3],int(i.split(",")[4])))
      #after getting 4,5 columns from filter rdd, the filter_direct_rdd will be cleaned by the GC
      select_city_amt_rdd=filter_direct_rdd.map(lambda rows_string:rows_string.split(",")).map(lambda cols_list:tuple((cols_list[1],int(cols_list[4]))))
      #again select_city_amt_rdd needs filter_direct_rdd to get 2,5 columns from filter rdd, hence filter_direct_rdd will be recreated inturn recreates file_rdd1 also
      #which is a costly effort., hence I am getting it from memory
      #[['ArunKumar,chennai,33,2016-09-20,100000'], ['irfan,chennai,37,2019-02-20,10000'], ['irfan,chennai,37,2019-02-20,10000'], ['basith,CHENNAI,29,2019-04-22,20000']]
      print(select_dt_amt_rdd.collect())#action called collect is performed
      print(select_city_amt_rdd.collect())
      filter_direct_rdd.unpersist()#execute only if further lines of code we are going to have in this application which doesn't require the filter_direct_rdd data
      print("some more lines of code i have in this application which doesn't require the filter_direct_rdd reference")

      #d. create RDD programmatically
      lst=list(range(0,40))
      range_rdd=sc.parallelize(lst)
      print(range_rdd.getNumPartitions())#total number of partitions
      print(range_rdd.glom().collect())#collect the individual partition data
      lst = [10000, 20000, 30000, 15000]
      bonus = 1000
      list(map(lambda sal: sal + bonus, lst))
      rdd1 = sc.parallelize(lst)
      rdd1.collect()
      rdd1.glom().collect()

      print("2. How to transform RDDs (Different transformation functions (which we don't use in real world directly)")
      #Map - iterate at the row level -(passive transformation - if the number of output is equal to the number of input passed)
      # is equivalent to select statement in db or for loop in python
      print(range_rdd.map(lambda x:x+10).collect())

      bonus=1000
      lam_convert_rowstring_collist=lambda row_string:row_string.split(",")
      lam_convert_rowstring_collist('ArunKumar,chennai,33,2016-09-20,100000')
      def lam_convert_rowstring_collist(row_string):
       return row_string.split(",")

      rows = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      #['ArunKumar,chennai,33,2016-09-20,100000', 'vasudevan,banglore,43,2016-09-23,90000', 'irfan,chennai,37,2019-02-20,10000', 'irfan,chennai,37,2019-02-20,10000', 'basith,CHENNAI,29,2019-04-22,20000']
      splitted_rows_into_columns=rows.map(lam_convert_rowstring_collist)
      print(splitted_rows_into_columns)
      #[['ArunKumar', 'chennai', '33', '2016-09-20', '100000'], ['vasudevan', 'banglore', '43', '2016-09-23', '90000'], ['irfan', 'chennai', '37', '2019-02-20', '10000'], ['irfan', 'chennai', '37', '2019-02-20', '10000'], ['basith', 'CHENNAI', '29', '2019-04-22', '20000']]
      #amt_col_int_type_rdd=splitted_rows_into_columns.map(lambda onerowcols:list([onerowcols[0],int(onerowcols[4])+bonus]))
      amt_col_int_type_rdd=splitted_rows_into_columns.map(lambda onerowcols:(onerowcols[0],int(onerowcols[4])+bonus))
      print("Final bonus applied salary is ")
      print(amt_col_int_type_rdd.collect())
      sc.textFile("file:/home/hduser/sparkdata/empdata.txt").map(lambda strrow: strrow.split(",")).map(
            lambda collist: collist[1].upper()).collect()

      #Filter (active transformation - if the number of output is greater or lesser than the number of input passed)
      #Filter will loop as like map and apply filter/where clause on the given data
      #rows = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      #splitted_rows_into_columns=rows.map(lam_convert_rowstring_collist)
      #filter only chennai data out of it regardless of case sensitivity
      rows_str = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      # ['ArunKumar,chennai,33,2016-09-20,100000', 'vasudevan,banglore,43,2016-09-23,90000', 'irfan,chennai,37,2019-02-20,10000', 'irfan,chennai,37,2019-02-20,10000', 'basith,CHENNAI,29,2019-04-22,20000']
      splitted_rows_str_into_columns_lst = rows_str.map(lambda row_string:row_string.split(","))
      filter_rdd=splitted_rows_str_into_columns_lst.filter(lambda x:x[1]=='chennai')
      filter_rdd.collect()

      #which is more optimistic?
      splitted_rows_str_into_columns_lst.filter(lambda x: x[1] == 'chennai').map(lambda x: x[1]).collect()#this is more optimistic
      splitted_rows_str_into_columns_lst.map(lambda x: x[1]).filter(lambda x: x == 'chennai').collect()

      #flatmap transformation (active transformation) (iterate at the row and column level)-
      #flatten the given data by running 2 levels of loop,
      # first loop to convert the list[str] (list of rows) to list[list[str]] (list of columns)
      #second loop to iterate (list of columns) and take the str out of the given column list
      rows_str=sc.textFile("file:///home/hduser/mrdata/courses.log")
      '''
      hadoop spark hadoop spark kafka datascience
      spark hadoop spark datascience
      informatica java aws gcp
      gcp aws azure spark
      gcp pyspark hadoop hadoop
      '''
      splitted_rows_str_into_columns_lst_columns_flattened = rows_str.flatMap(lambda row_string: row_string.split(" "))
      print(splitted_rows_str_into_columns_lst_columns_flattened.collect())

      for i in ['hadoop spark hadoop spark kafka datascience', 'spark hadoop spark datascience',
                'informatica java aws gcp', 'gcp aws azure spark', 'gcp pyspark hadoop hadoop']:
            row_cols=i.split(" ")
            print(i.split(" "))#[[hadoop, spark, hadoop ,spark ,kafka ,datascience]]
            for j in row_cols:
                  print(j)

#To convert unstructured data to structured, we use flatmap

      #distinct transformation (active transformation)
      rows_str.flatMap(lambda row_string: row_string.split(" ")).distinct().collect()

      #union (passive transformation) - union will do union all between 2 rdds (including duplicates)
      rows_str1 = sc.textFile("file:///home/hduser/mrdata/courses1.log")
      rows_str.union(rows_str1).count()
      #to avoid duplicates or to make union behave like simple union, we have to use distinct
      rows_str.union(rows_str1).distinct().count()

      #zip - passive transformation -
      rows_str2 = sc.textFile("file:///home/hduser/mrdata/stud_enquiry.csv")
      #1, Anand
      #2, Harish
      #3, Vaishali
      rows_str3 = sc.textFile("file:///home/hduser/mrdata/stud_city.csv")
      #chennai
      #mumbai
      #hyderabad
      rows_str3.coalesce(1).zip(rows_str3.coalesce(1)).collect()
      #zip has two thumb rules
      #rule 1. Can only zip RDDs with same number of elements in each partition
      rows_str2.glom().collect()
      #[['1,Anand', '2,Harish'], ['3,Vaishali']]
      rows_str3.glom().collect()
      #[['chennai', 'mumbai'], ['hyderabad', 'kolkatta']]
      #rule 2. Both rdds should have same number of partitions to execute the zip
      print(rows_str2.getNumPartitions())
      print(rows_str3.coalesce(1).getNumPartitions())

      #zipWithIndex - useful for creating running number or sequence number for a given rdd, using which we can identify or filter some dta set
'''
cid,city,device,amt
1,Chennai,Mobile,1000
2,Chennai,Laptop,3000
3,Hyd,mobile,4000
4,Chennai,mobile,2000
5,Mumbai,Laptop,4000
6,Mumbai,Mobile,1500
7,Kolkata,Laptop,5000
8,Chennai,Headset,9000
9,Chennai,Laptop,22222
'''
#interview question - how to eleminate header in the given dataset
rdd1_remove_header=rdd1.zipWithIndex().filter(lambda x:x[1]>0)#.map(lambda x:x[0])
#rdd2_distinct_city=rdd1_remove_header.map(lambda x:x.split(",")).map(lambda x:x[1]).distinct()
#rdd2_distinct_city.collect()
#['Chennai', 'Hyd', 'Mumbai', 'Kolkata']

#reduceByKey is a (paired RDD) transformation (active transformation) - help apply reducer in the spark core application
#remove the header 'cid,city,device,amt' from the above dataset
rdd1=sc.textFile("file:///home/hduser/cust_noheader.txt")
rdd1.map(lambda x:x.split(",")).map(lambda x:((x[1],x[2]),int(x[3]))).reduceByKey(lambda x,y:x+y).collect()

#interview question - how to do word count using spark core program/ how to process some unstructured data and convert to structured
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda a,i:a+i)
print(rdd1.collect())
print(sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).countByValue())
print(sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).countByKey())

print("Actions in Spark")

#any function applied on the rdd that return result ( then it is an action) in a form of value or collection type
#Every spark application with have atleast one action to trigger the DAG and lineages
#Actions can bring the result to the client/driver for analysis/dev/testing/learning
# or for decision making/programming logics in an application
#or the action output can stored in a storage layer also
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("INFO")#ERROR/WARN/INFO - DEFAULT ERROR
print("upto spark context creation")
rdd1=sc.textFile("hdfs:///user/hduser/empdata.csv")
rdd1.cache()
rdd2=rdd1.map(lambda x:x.split(","))
print(rdd2.count())
print(rdd1.collect())
print(rdd2.collect())
#print(rdd1.count())
#collect - action used to collect the data from rdd partition to the client node (driver node)
#must be used with caution, because the collect may return all the data from the cluster to the client
rdd1=sc.textFile("hdfs:///user/hduser/empdata.csv")
local_driver_python_collection_variable=rdd1.collect()#not preferrable
print(len(local_driver_python_collection_variable))
#count
print(rdd1.count())#preferrable - we should avoid bringing raw data directly to client and process data instead we can process data in distributed for and then bring resulted data to client for consolidation.
#sum,max,min
sal_rdd=rdd1.map(lambda x:int(x.split(",")[4]))
print(sal_rdd.sum())#preferrable because consolidating and collecting
print(sal_rdd.max())#preferrable because consolidating and collecting
pythonvalue=sal_rdd.collect()
print(sum(pythonvalue))#not preferrable because collecting and consolidating
print(max(pythonvalue))#not preferrable
#first
#first is an action that bring the very first row in the given rdd in a form of value equivalent to row_num in sql
python_int_value=sal_rdd.first()
#take is an action that takes first few values in the given rdd  in a form of list (collection) equivalent to limit in sql
python_lst=sal_rdd.take(2)
#interview question: difference between first and take?
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdataheader.txt")
print(rdd1.count())
header=rdd1.first()
rdd2=rdd1.filter(lambda x:x!=header)
print(rdd2.count())

header_lst=rdd1.take(1)
rdd2=rdd1.filter(lambda x:x!=header_lst)
print(rdd2.count())

#top
#top is an action that takes top few values in the given rdd in a form of list (collection) equivalent to limit in sql
python_lst2=sal_rdd.top(3)
#reduce
python_value3=sal_rdd.reduce(lambda x,y:y if x>y else x)#reduce is a reducer action used to write custom functions
#lookup
rdd1=sc.textFile("hdfs:///user/hduser/empdata.csv")
rddsplit=rdd1.map(lambda x:x.split(","))
max_sal=rddsplit.map(lambda x:int(x[4])).max()
sal_name_rdd=rddsplit.map(lambda x:(int(x[4]),x[0]))
print(sal_name_rdd.collect())
#[(100000, 'ArunKumar'), (90000, 'vasudevan'), (10000, 'irfan'), (100000, 'delhi babu'), (20000, 'basith')]
print(sal_name_rdd.lookup(max_sal))
#['ArunKumar', 'delhi babu']

#saveAsTextFile - we can store the result into the filesystem, hence avoiding collect()
rdd1=sc.textFile("hdfs:///user/hduser/empdata.csv")
rddsplit=rdd1.map(lambda x:x.split(","))
sal_name_rdd=rddsplit.map(lambda x:(x[0],int(x[4])))
sal_name_rdd.saveAsTextFile("hdfs:///user/hduser/emp_sal/")

#Interview questions
#If we use saveAsTextFile, does the data in the executor will be copied to the driver
# and sent back to the datanode to save in the HDFS?
#no, the rdd partitions in the executor will be directly writterned into the respective datanodes location.
#the data copy to the driver will only happen if i write some actions that required for some adhoc analysis
# eg. count(),sum(),collect()...

############################SPARK CORE LEARNING IS COMPLETED#################################
############################ BELOW CONCEPTS ARE COMMON FOR SPARK CORE/SQL/STREAMING #################################
print("Very very important concept, must know stuffs")
print("4******. How to (primarily or fundamentally) optimize an spark RDD application or very importantly SQL application (important)")
#How to optimize the below spark application?
#requirement: identify the errors and warning occured in the namenode server
#print the number of errors and warnings
#show some sample of errors and warnings
#memory optimization - how to use the in-memory for optimizing the spark applications
#cache/persist/unpersist/StorageLevel's
log_analysis_rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log")
splitrdd2=log_analysis_rdd1.map(lambda x:x.split(" "))#200mb of data approx
split_filter_rdd3=splitrdd2.filter(lambda x:len(x)>=3)#150mb of data approx
#caching of the err_filterrdd3 and warn_filterrdd4 is more efficient, interms of reduced usage of memory and reduced iteration of transformations
#split_filter_rdd3.cache()#retain the cache memory (150mb) with almost most of the data extracted from the filesystem
err_filterrdd3=split_filter_rdd3.filter(lambda x:x[2]=='ERROR')#filter only 1mb data from 150mb of data
#2gb of err_filterrdd3
err_filterrdd3.cache() #(don't do unpersist after consuming this cache by the below action) retain the cache memory with only error data of 1mb
err_cnt=err_filterrdd3.count()#get the data from the filesystem onwards
#err_filterrdd3.unpersist()
print(err_cnt)
print(err_filterrdd3.take(2))#get the data from the 1mb cache
#get the data from split_filter_rdd3 (150)-> apply filter(error) (1mb) -> 2 rows
#get the data from err_filterrdd3 apply filter(error) (1mb) -> take(2 rows)
if err_cnt<1000:#get the data from split_filter_rdd3 (150)-> apply filter(error) (1mb) -> 1 row
      error_log=err_filterrdd3.collect()#get the data from split_filter_rdd3 (150)-> apply filter(error) (1mb) -> 100 error rows
      #if the volume of data returned from an rdd is finite, then use collect.
      print("sending the error as a mail to the admin's")
err_filterrdd3.unpersist() #gracefully ask GC pls clean the memory of 1mb
#LRU Eviction algorithm is applied to clean the least recently used memory space, if more
#memory is needed by the subsequent programs.
warn_filterrdd4=split_filter_rdd3.filter(lambda x:x[2]=='WARN')
warn_filterrdd4.cache() ##retain the cache memory with only warn data of 10mb
warn_cnt=warn_filterrdd4.count()#get the data from the filesystem onwards
print(warn_cnt)
print(warn_filterrdd4.take(2))
if warn_cnt<10000:#get the data from split_filter_rdd3 (150)-> apply filter(error) (1mb) -> 1 row
      warn_log=warn_filterrdd4.collect()#get the data from split_filter_rdd3 (150)-> apply filter(error) (1mb) -> 100 error rows
      #if the volume of data returned from an rdd is finite, then use collect.
      print("sending the error as a mail to the admin's")

#unpersist will run automatically when the application is finished
#print(log_analysis_rdd1.count())

#How many types of data caching or temorary persistance of the RDD data is available?
#JVM/Executor/Container in general comprise of 2 major compartments of memory (ON-HEAP (major eg. 90%) & OFF-HEAP (minor eg. 10%))
#on-heap holds all variable rdd partitions of base and transformed for eg. cache will hold the data here
#off-heap holds all static configurations, code, tasks, part of lineage, environment variables etc.,

#We have options in spark to make use only memory or memory with replica or mem&disk only or mem&disk with replica or mem with serde
#or off heap memory etc.,

#Interview Question - what difference between cache & persist:
# In general both are same (if we don't leverage persist with more options)
#cache will retain by default the data only in memory and persist will provide multiple options to retain the rdd data in
# only memory or memory with replica or disk only or mem&disk only or mem&disk with replica or mem with serde
#or off heap memory etc.,

#Interview question : How to identify the number of elements in a given partition in an rdd by writing python program?
#answer1: I will go through the ui
#answer2:below code
pythonvar=list(range(1,101))
rdd1=sc.parallelize(pythonvar,8)
for i in rdd1.glom().collect():
 print(len(i))

from pyspark.storagelevel import StorageLevel #memory_only,disk_only,memory_and_disk....
warn_filterrdd4.cache()#equivalent to cache
warn_filterrdd4.persist() #equivalent to cache
warn_filterrdd4.persist(StorageLevel.MEMORY_ONLY)
#equivalent to cache (used in general by default)
warn_filterrdd4.persist(StorageLevel.MEMORY_ONLY_2)
#use only memory with 2 replica
# 1. prerequisite - if the source data volume is small to medium and if it fits in the memory with 2 replicas
# 2. in a business critical applications, if we need both performance (memory with speculative execution) and fault tolerance
warn_filterrdd4.persist(StorageLevel.DISK_ONLY) #equivalent to cache
#use only disk with 1 replica
# 1. prerequisite - if the source data volume is high and if it can't fits in the memory even with 1 replica
# 2. For batch applications (that run in peak hours), non business critical applications (10x faster execution is good), if we need more fault tolerance and use less memory resource
# 3. For replica 2 and 3, if we need fault tolerance in a higher degree (2/3) we go with replica 2 or 3 with medium performance 10X
# 4. increasing the number of replicas will increase data locality inturn help managing resources efficiently and performance also
warn_filterrdd4.persist(StorageLevel.DISK_ONLY_2)#serialized
warn_filterrdd4.persist(StorageLevel.DISK_ONLY_3)#serialized
warn_filterrdd4.persist(StorageLevel.MEMORY_AND_DISK)#serialized
warn_filterrdd4.persist(StorageLevel.MEMORY_AND_DISK_2)#serialized
#use both memory (upto the volume that can fit in memory (tachyon)) and disk (balance data cached in the disk) in a serialized (native spark java serializer) fashion
#1. prerequisite - If the source data is huge or variable in size, which cannot fits into the memory always and if we need medium to high performance.
#2. To avoid contacting the source system once again for collecting and process the remaining partial data. Instead, it can process it from the already collected data in the disk.
# 3. For replica 2, if we need fault tolerance  with medium to high performance (10 to 100x performance)
# 4. Storage occupied will be less but CPU overhead time taken for deserialization is more

#warn_filterrdd4.persist(StorageLevel.MEMORY_AND_DISK_DESER) #de serialized data
#use both memory (upto the volume that can fit in memory (tachyon)) and disk (balance data cached in the disk) in a serialized (native spark serializer) fashion
#1. prerequisite - If the source data is huge or variable in size, which cannot fits into the memory always and if we need medium to high performance.
#2. To avoid contacting the source system once again for collecting and process the remaining partial data. Instead, it can process it from the already collected data in the disk.
# 4. Storage occupied will be more (because deserialized) but time taken for directly accessing the deserialized data is less (no CPU overhead for deserialization)
warn_filterrdd4.persist(StorageLevel.OFF_HEAP) #

#partitioning (Important)
#Partitioning is the horizontal division of data
# (HDFS -Blocks, mapreduce - input splits, Sqoop -mappers, hive - partitioning/bucketing, yarn - containers, spark - partitions, python - list(list1,list2,list3))
#RDDs are distributed because of partitioning
#the degree of parallelism can be increased/decreased with the help of partitioning (coalesce/repartition)
#We can create or redefine partitions at any point of time in a given program
#Number of partitions determines the number of tasks in a given job

#32/128 respective for lfs/hdfs default no. of partitions is 2
#>32mb or >128mb respective for lfs/hdfs partitions will be calculated as totalsize/32mb or totalsize/128mb repectively
#programatic - total number of cores by default or overload with your desired partition numbers

#Controlling partitions at the RDD creation time:
#by default 2 partitions will be applied as per the code writterned by the framework developers if the size of data is not a single partition worth
rdd1=sc.textFile("file:///home/hduser/cust.txt")
print(rdd1.getNumPartitions())
#2
#we can override the default 2 partition with the minimum partition as an argument
rdd1=sc.textFile("file:///home/hduser/cust.txt",3)
rdd1.getNumPartitions()
#3

#by default for (LFS other non distributed FS) 32 mb per partition will be applied if the size of data is more than a single partition worth of 32mb
rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-datanode-localhost.localdomain.log",4)
rdd1.getNumPartitions()
#7
# we can override with the minimum partition as an argument if the size of data is more than a single partition worth of 32mb
rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-datanode-localhost.localdomain.log",8)
rdd1.getNumPartitions()
#8

#RDD Creation on a Distributed FS like HDFS:
#similar to LFS -  by default 2 partitions will be applied as per the code writterned by the framework developers if the size of data is not a single partition worth
rdd1=sc.textFile("/user/hduser/cust.txt")
print(rdd1.getNumPartitions())
#2 for both hadoop and linux if the file size is less than 1 block worth of data

#similar to LFS - we can override the default 2 partition with the minimum partition as an argument
rdd1=sc.textFile("/user/hduser/cust.txt",3)
rdd1.getNumPartitions()
#3

#by default for (HDFS) 128 mb per partition will be applied if the size of data is more than a single partition worth of 128MB
rdd1=sc.textFile("/user/hduser/hadoop-hduser-datanode-localhost.localdomain.log")
rdd1.getNumPartitions()
#2
rdd1=sc.textFile("/user/hduser/hadoop-hduser-datanode-localhost.localdomain.log",4)
rdd1.getNumPartitions()
#4 Minimum

# we can override with the minimum partition as an argument if the size of data is more than a single partition worth of 32mb
rdd1=sc.textFile("/user/hduser/hadoop-hduser-datanode-localhost.localdomain.log",8)
rdd1.getNumPartitions()
#8

#Programatic creation of rdds with default and overloaded partitions:
python_range=range(1,101)
rdd1=sc.parallelize()#It will consider the number of cores defined
rdd1.getNumPartitions()
#4
rdd1=sc.parallelize(range(1,101),10)#here the number of partitions can be passed as an argument

#Controlling partitions after RDD is created:
#I launched a spark job with 2 executors with 2 cores of processor each
#rdd1=sc.textFile("/user/hduser/hadoop-hduser-datanode-localhost.localdomain.log")
#this creates 2 partition by default, i am not leveraging all cores the processors allocated for the executors
rdd1=sc.textFile("/user/hduser/hadoop-hduser-datanode-localhost.localdomain.log",4)#228mb of data is loaded with 4 partitions
if rdd1.count()<800000:
      rdd11 = rdd1.coalesce(3)#reduce the number of partitions
else:
      rdd11 = rdd1.repartition(6)#increase the number of partitions

print(rdd11.getNumPartitions())
print(rdd11.count())
warnrdd2=rdd11.filter(lambda x:'WARN' in x)#228mb of data is processed with 4 partitions/4 filter tasks in parallel
print(warnrdd2.getNumPartitions())
print(warnrdd2.count())
warnrdd3=warnrdd2.coalesce(2)#repartition(2)
print(warnrdd3.getNumPartitions())
ziperrorrdd3=warnrdd3.zipWithIndex()
print(warnrdd3.getNumPartitions())
ziperrorrdd3.coalesce(1).saveAsTextFile("/user/hduser/errorlog")
#Interview question1: If you are writing the output of an rdd or a dataframe or a spark temp view, the output in the fs or cloud
#or db is generated with more number of files/partitions, how do you make it to one or fewer files? coalesce
#Interview question2: How do we dynamically handle the number of partitions based on the volume of data (initially or in the mid of the program)
# If the volume of data after applying some transformations is reduced, then use coalesce to reduce the number of partitions accordingly

fmrdd=rdd1.flatMap(lambda x:x.split(' '))#data grown from 0.8 million to 11 million, so its time for increasing the number of paritions
fmrdd_repart=fmrdd.repartition(20).map(lambda x:x.upper())
fmrdd_repart.count()#this count is going to run 11 million times

#Interview Question? Difference between repartition and coalesce
#1. Repartition or coalesce will call the coalesce function in the background with or without shuffling respectively
#eg. look at the /usr/locall/spark/python/pyspark/rdd.py -> class RDD -> function coalesce, reparitition
#2. Repartition is used to increase the partition and coalesce is used to reduce the partition - but can be used vice versa also (not suggested)
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_4k",4)
for i in rdd1.glom().collect():
 print(len(i))
rdd2 = rdd1.coalesce(2)
rdd2=rdd1.repartition(8)
rdd2=rdd1.coalesce(8,True)#increasing number of part from 4 to 8 - not recommended, because repartition is ther eto use
rdd2=rdd1.repartition(2)#reducing number of part from 4 to 2 - not recommended, because shuffling will happen
#3. Repartition will shuffle in background, coalesce avoids shuffling mostly
rdd2=rdd1.coalesce(2)#use coalesce, because it avoids shuffling
rdd2=rdd1.repartition(2)#reducing number of part from 4 to 2 - not recommended, because shuffling will happen
#4. Repartition equal number of elements across partitions (Round Robin Distribution),
# coalesce number of elements in partitions may vary (Random/Range Distribution) (leads to data skewing - uneven distribution of workload)
rdd11=sc.parallelize(range(1,101),4)
rdd11.repartition(10).glom().collect()#equal sliced partitions
for i in rdd11.repartition(10).coalesce(4).glom().collect():
      print(len(i))
'''
for (i <- rdd11.repartition(10).coalesce(4).glom().collect()){
     | println(i.length)}
'''
rdd11=sc.parallelize(range(1,10001))
for i in rdd11.repartition(10).glom().collect():
 print(len(i))
for i in rdd11.repartition(10).coalesce(3).glom().collect():
 print(len(i))

#5. Interview question: When do we use coalesce or repartition (refer the top part of the partitioning concepts discussed)
#a. We use coalesce if we find the volume of data is lesser than one partition worth (rdd with 8 partitions, each partitionw with only 4 mb),
#then coalesce to lesser number
# or if the functions (sql/dsl/df/rdd transformation) applied results lesser volume of data, then coalesce to lesser partition
#b. We use repartion if we find the volume of data is more than one partition worth (rdd with 2 partitions, each partition with 200 mb),
#then repartition to higher number (consider the executor numbers and cores per executor also)
# or if the functions (sql/dsl/df/rdd transformation) applied results in more volume of data, then repartition to higher number of partition

#broadcasting
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
driver_bonus=1000
rdd2=rdd1.map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[4])+driver_bonus))
#every time for every iteration of map, it will read the data from the driver
rdd2.collect()#not optimistic, because the data copied on demand for every iteration of the mapper
broadcasted_driver_bonus=sc.broadcast(driver_bonus)
rdd2=rdd1.map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[4])+broadcasted_driver_bonus.value))
rdd2.collect()#more optimistic, because the broadcast of the data happens only once per node
#every time for every iteration of map, it will read the data from the driver

#Checkpointing (spark streaming) - Use checkpoint if the source data is variable in nature.
#Interview Question: Difference between checkpoint and caching/persisting or getConfigFile()
#Checkpointing is the persistance of the rdd data into filesystem (local/hdfs) by TRUNCATING the LINEAGE
#does checkpoint == persist(StorageLevel.DiskOnly) ? NOT exactly the same
#checkpoint (doesn't not maintain the lineage of the parent rdds) <> persist (retains the lineage  of the parent rdds)
#checkpoint is more good for fault tolerance, reliability & performance
# whereas persist(Disk_only) is good for reliability and performance
# whereas persist(memory_only) is good for performance
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
rdd1.persist(StorageLevel.DISK_ONLY)#if the data persisted is lost for some reason, i can still get the data from the parent rdd using lineage
rdd1.count()
#Materialize the data from empdata.txt in memory in rdd1, then persist the data into disk storage, then count will be calculated from the memory
rdd1.count()
#read the data from the persisted disk storage (reliable), then count will be calculated from the disk
#If the rdd1.persisted disk data is lost for some reason, then ( USING THE LINEAGE) the rdd will Materialize the data from empdata.txt in memory in rdd1,
# then persist the data into disk and count will be applied on the memory data
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
rdd2=rdd1.map(lambda x:x.split(","))
#100 transformations
sc.setCheckpointDir("hdfs:///user/hduser/ckptdir12")#reliable (HDFS) or non-reliable(local) storage
#if the base rdd is variable in nature, because of streaming source data, then go with checkpoint
#if the data checkpointed is lost for some reason, i can't get the data from the parent rdd using lineage (bcz lineage is deleted after checkpoint happend)
rdd2.checkpoint()
#rdd2.persist(StorageLevel.DISK_ONLY)
rdd2.cache()#If this cache is lost, my source will be checkpoint directory and not the rdd1 directory
rdd2.count()
#The data will be materialized in the memory and into the checkpoint location also parallely,
# then count will execute on the memory data and then the rdd1 data in memory will be cleaned by the GC
# and finally the DAG (lineage) in the driver for the rdd2 creation will be deleted
rdd2.count()#subsequent actions depends on the checkpointed data only and apply the action without falling back on the parent rdd since lineage is deleted.
print(rdd2.getCheckpointFile())