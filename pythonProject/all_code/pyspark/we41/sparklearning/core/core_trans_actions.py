print("Writing spark Application to understand spark core programming")
#***1. Creating spark session object inturn it will create spark context object - important
#2. Creating RDDs using multiple ways # we don't write coding in spark using this legacy methodology,
# but in interview or in some rare custom situation, we may need this learning - less important
#3. Applying different transformations - less important in terms of what this functions will do (but important on how it works architecturally)
#4. Applying different actions - less important in terms of what this functions will do (but important on how it works architecturally)
#***5. Applying some Architecture (cluster manager) understanding + performance optimizations - very very important (core, sql, streaming)
#1. cache,persisting,storagelevels (DISK_ONLY,...),unpersist,
#2. partitioning coalesce/repartition (core/sql/hive/streaming)
#3. checkpointing
#4. broadcasting

#***1. Creating spark session object inturn it will create spark context object
from pyspark.sql import SparkSession
#spark=SparkSession.builder.master("local[*]").appName("Spark core learning WE41").getOrCreate()
spark=SparkSession.builder.master("local[*]").getOrCreate()
#spark session object has the function related to core, sql (sql/hive)
sc=spark.sparkContext#just renaming the spark.sparkContext as sc for simplicity
sc.setLogLevel("ERROR")#We can set the logger level to INFO/WARN/ERROR to see the detail information of every step or warning if any or only error if any
#2. Creating RDDs using multiple ways
#Create rdd from different sources (FS sources)
hadooplines= sc.textFile("hdfs://127.0.0.1:54310/user/hduser/empdata.txt")
linesrdd = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
hadoopfilenamerdd1=sc.wholeTextFiles("hdfs:///user/hduser/sampledata/*.txt")

#Create rdd from another RDD
#Eg: lets create another rdd with only the amount column derived from the linesrdd
print(linesrdd.collect())
#convert the string to list[string]
#'ArunKumar,chennai,33,2016-09-20,100000'.split(',')
#['ArunKumar','chennai','33','2016-09-20','100000']
lam_return_list_of_str_particular_col_str_int=lambda row:int(row.split(',')[4])
#def function (If the function is going to be used accross the team)
def fun_return_list_of_str_particular_col_str_int(row):
    return int(row.split(',')[4])
#lambda function (preferrable if this is a just anonymous function)

anotherrdd=linesrdd.map(lam_return_list_of_str_particular_col_str_int)#preferable
anotherrdd1=linesrdd.map(fun_return_list_of_str_particular_col_str_int)
print(anotherrdd.collect())
print(anotherrdd1.collect())

#Eg2: lets add a surcharge of 100 rs for all amount
surcharge=100
#lam_apply_bonus=lambda amt:amt-surcharge
anothersurchargerdd=anotherrdd.map(lambda amt:amt-surcharge)#some special functionalities ? - HOF, closure
print(anothersurchargerdd.collect())
#map()

#maprdd2=lines.map()
#Create rdd from memory

lines=sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
lines.cache()#I will retain the data in the memory once after the first materialization happens (no GC will be applied) until explicitly the program deletes it or until the application is completd
anotherrddfrommemory=lines.map(lambda row:int(row.split(',')[4]))
anotherrddfrommemory2=lines.map(lambda row:int(row.split(',')[2]))#creating rdd from memory
#anotherrddfrommemory.cache()
anotherrddfrommemory.count()#first time the data from disk loaded to lines rdd then to anotherrddfrommemory
anotherrddfrommemory.sum()#second time onwards the data from lines rdd memory will be consumed by anotherrddfrommemory
anotherrddfrommemory.collect()#second time onwards the data from lines rdd memory will be consumed by anotherrddfrommemory
print("some further lines of code we are going to write, which is not depending on any of the above rdds (lines,anotherrddfrommemory)")
lines.unpersist()#define this explicitly, if we have some more line of code below to execute
#or dont define unpersist() explicitly, if we don't have any more line of code in this application

##Create rdd programmatically
custinfo=["1,irfan","2,inceptez","3,vinoth","4,ram"]
progrdd1=sc.parallelize(custinfo,4)
progrdd2=sc.parallelize(range(0,100))
progrdd2.map(lambda x:x%2).collect()
progrdd2.filter(lambda x:x%2==1).collect()


#3. Applying different transformation (a function that return another rdd) functions
#lambda function, HOF, closures, defined function (positional, named, default)
#map - like a select statement in RDBMS or for loop in python prog
#passive transformation - if the number of elements returned is equal the number of the elements passed to a function/transformation
maprdd=progrdd2.map(lambda x:x/2)#passive transformation
maprdd.collect()

lst=list(range(0,100))
rdd1=sc.parallelize(lst)
rdd1.map(lambda x:x+10).collect()

#how to learn any transformation in spark core?
filerdd1=sc.textFile("file:///home/hduser/cust.txt")
print(filerdd1.collect())
#['1,Chennai,Mobile,1000', '2,Chennai,Laptop,3000', '3,Hyd,mobile,4000', '4,Chennai,mobile,2000', '5,Mumbai,Laptop,4000', '6,Mumbai,Mobile,1500', '7,Kolkata,Laptop,5000', '8,Chennai,Headset,9000', '9,Chennai,Laptop,22222']
mapsplitrdd2=filerdd1.map(lambda row_string:row_string.split(","))
print(mapsplitrdd2.collect())
#[['1', 'Chennai', 'Mobile', '1000'], ['2', 'Chennai', 'Laptop', '3000'], ['3', 'Hyd', 'mobile', '4000'], ['4', 'Chennai', 'mobile', '2000'], ['5', 'Mumbai', 'Laptop', '4000'], ['6', 'Mumbai', 'Mobile', '1500'], ['7', 'Kolkata', 'Laptop', '5000'], ['8', 'Chennai', 'Headset', '9000'], ['9', 'Chennai', 'Laptop', '22222']]
mapamtstrrdd3=mapsplitrdd2.map(lambda row_collist_string:row_collist_string[3])
print(mapamtstrrdd3.collect())
#['1000', '3000', '4000', '2000', '4000', '1500', '5000', '9000', '22222']
mapamtintrdd4=mapamtstrrdd3.map(lambda row_col_string:int(row_col_string))
print(mapamtintrdd4.collect())
#[1000, 3000, 4000, 2000, 4000, 1500, 5000, 9000, 22222]

#convert the above code in one line (preferred way for better performance)
filerdd1=sc.textFile("file:///home/hduser/cust.txt").map(lambda row_string:row_string.split(","))
filerdd1.cache()
amt_int_rdd2=filerdd1.map(lambda x:int(x[3]))
city_rdd3=filerdd1.map(lambda x:x[1])
print(amt_int_rdd2.sum())
print(city_rdd3.collect())

#filter - like a select statement with where clause in RDBMS or for loop with if condition in python prog
#active transformation - if the number of elements returned is equal or lesser or greater than the number of the elements passed to a function/transformation
filterrdd=progrdd2.filter(lambda x:x%2==0)#active transformation
filterrdd.collect()
for i in filterrdd.collect():
 if i%2==0:
  print(i)

#interview question- How we do cleanup of the bad data in our file, before we transform the data
filerdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt").map(lambda row_string:row_string.split(","))
#IndexError: list index out of range
print(filerdd1.filter(lambda row_list:len(row_list)>=5).map(lambda x:int(x[4])).max())


#flatMap - HOF that can take another function(lambda) as an argument and it will do map then flattern of the mapped data
#uses 2 loops - 1 for mapping and 2nd one for flattening

#eg. we go for a team build
'''un-structured
ram kalyani singaraj surya
aravind rajkumar

structured
project_members
***************
ram 
kalyani 
singaraj 
surya
aravind 
rajkumar 
'''
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log")
print(rdd1.collect())
#['hadoop spark hadoop spark kafka datascience', 'spark hadoop spark datascience', 'informatica java aws gcp', 'gcp aws azure spark', 'gcp pyspark hadoop hadoop']
print(rdd1.flatMap(lambda x:x.split(" ")).collect())
#['hadoop', 'spark', 'hadoop', 'spark', 'kafka', 'datascience', 'spark', 'hadoop', 'spark', 'datascience', 'informatica', 'java', 'aws', 'gcp', 'gcp', 'aws', 'azure', 'spark', 'gcp', 'pyspark', 'hadoop', 'hadoop']

#Interview question: Any rare cases you used spark core in your project
#answer is no by most of the people, which is acceptable
#if you want to answer, say no or say rarely in some cases where we did some cloud/server/web log analysis in one of the module of the project
dn_log_rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-datanode-localhost.localdomain.log").map(lambda x:x.split(" "))
print(dn_log_rdd1.filter(lambda x:len(x)>=3).filter(lambda x:x[2]=='ERROR').count())
#159
dn_log_str_rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-datanode-localhost.localdomain.log")
print(dn_log_str_rdd1.filter(lambda x:'EXCEPTION' in x.upper()).count())

#Interview question : write a spark core program to do a word count?
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log")
print(rdd1.collect())
#['hadoop spark hadoop spark kafka datascience', 'spark hadoop spark datascience', 'informatica java aws gcp', 'gcp aws azure spark', 'gcp pyspark hadoop hadoop']
print(rdd1.flatMap(lambda x:x.split(" ")).collect())
#['hadoop', 'spark', 'hadoop', 'spark', 'kafka', 'datascience', 'spark', 'hadoop', 'spark', 'datascience', 'informatica', 'java', 'aws', 'gcp', 'gcp', 'aws', 'azure', 'spark', 'gcp', 'pyspark', 'hadoop', 'hadoop']
print(rdd1.flatMap(lambda x:x.split(" ")).distinct().collect())
print(rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).collect())
print(rdd1.flatMap(lambda x:x.split(" ")).countByValue())
#flatmap transformation: row[string] -> row[col[string]] -> col[string]
#This function will perform mapping and the flattening on the mapped data
#what type of transformation?active
emptylst=[]
for i in rdd1.collect():#row[string]
 splitrdd=i.split(" ")#row of string to row of column[string]
 for j in splitrdd:#iterate on the column[string]
  emptylst.append(j)#only consider the string to convert to column[string]
print(emptylst)

#union transformation (set operators) - merge the data (column wise) regardless of any key column
#union has some thumb rules -
#1. the number of columns between rdd1 and rdd2 should be same (for pyspark 3x version not applicable)
#2. the datatype of columns between rdd1 and rdd2 should be same (for pyspark 3x version not applicable)
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")#5 rows
rdd2=sc.textFile("file:///home/hduser/sparkdata/empdata1.txt")#4 rows
hive_table_with_rowformatdel_datatype_cols=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")\
 .map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],int(x[2]),x[3],int(x[4])))
rdd3=rdd1.union(rdd2)#9 rows
rdd3.collect()#union all (with duplicates) of multiple rdds with duplicates
rdd1.union(rdd2).distinct().collect()#union of multiple rdds with duplicates
filrdd1=rdd1.map(lambda x:x.split(",")).filter(lambda x:x[1]=='chennai').map(lambda x:(x[0],x[1],int(x[2]),x[3],int(x[4])))#converting list to tuple to perform distinct operation
filrdd2=rdd2.map(lambda x:x.split(",")).filter(lambda x:int(x[4])>10000).map(lambda x:(x[0],x[1],int(x[2]),x[3],0))#in pyspark it is handled
filrdd1.union(filrdd2).collect()#union of multiple rdds with duplicates
filrdd1.union(filrdd2).distinct().collect()#union of multiple rdds without duplicates
'''
cat empdata1.txt
ArunKumar,33,2016-09-20,50000
surya,43,2016-09-23,5000
swetha,37,2019-02-20,10000
senthil,37,2019-02-20,20000
'''
filrdd2=rdd2.map(lambda x:x.split(",")).map(lambda x:(x[0],'NA',int(x[1]),x[2],int(x[3])))
filrdd1.union(filrdd2).collect()


#zip - Joins two RDDs by combining the i-th of either partition with each other.
#if the dataset across 2 different rdds has to be joined without any common keys available.
#file1:
#1,irfan,41
#2,xyz,9
#file2:
#chennai,tn,M
#banglore,ka,U
#file3:
#1,irfan,41,chennai,tn,M
#2,inceptez,9,banglore,ka,U
#1. thumb rules - the number of partitions between 2 rdds must be same
#ValueError: Can only zip with RDD which has the same number of partitions
#2. the number of element in each partitions must be same
'''
>>> file1rdd.glom().collect()
[['1,irfan,41'], ['2,xyz,9']]
>>> file2rdd.glom().collect()
[['chennai,tn,M', 'banglore,ka,U'], []]
'''
#Can only zip RDDs with same number of elements in each partition
file1rdd=sc.textFile("file:///home/hduser/sparkdata/file1")
file2rdd=sc.textFile("file:///home/hduser/sparkdata/file2")
print(file1rdd.coalesce(1).zip(file2rdd.coalesce(1)).collect())

#Interview question? In a given file I have a header, how to remove it?
file1rdd=sc.textFile("file:///home/hduser/sparkdata/empdataheader.txt")
file1rdd.zipWithIndex().filter(lambda x:x[1]!=0).map(lambda x:x[0]).collect()

header=file1rdd.first()#action function used to identify the first element of a given rdd
file1rdd.filter(lambda x:x!=header).collect()

#paired rdd transformations -eg. join, reduceByKey, groupByKey, aggregateByKey
# an rdd with the key and value pair is a paired rdd
#if certain transformations, only work on those paired rdd then it is paired rdd transformations
file1rdd=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
for i in file1rdd.collect():
 print(i)#prints 5 rows with 1 string column each row

for i in file1rdd.map(lambda x:x.split(",")).collect():
 print(i)#prints 5 rows with 5 string columns each row
splitrdd2=file1rdd.map(lambda x:x.split(","))
#all the below are paired rdds
print(splitrdd2.map(lambda x:(x[1],int(x[4]))).collect())

#[('chennai', 100000), ('banglore', 90000), ('chennai', 10000), ('chennai', 10000), ('Delhi', 20000)]
print(splitrdd2.map(lambda x:(x[1],(int(x[2]),int(x[4])))).collect())
#[('chennai', (33, 100000)), ('banglore', (43, 90000)), ('chennai', (37, 10000)), ('chennai', (37, 10000)), ('Delhi', (29, 20000))]
print(splitrdd2.map(lambda x:((x[1],x[3]),(int(x[2]),int(x[4])))).collect())
#[(('chennai', '2016-09-20'), (33, 100000)), (('banglore', '2016-09-23'), (43, 90000)), (('chennai', '2019-02-20'), (37, 10000)), (('chennai', '2019-02-20'), (37, 10000)), (('Delhi', '2019-04-22'), (29, 20000))]

#reduceByKey is a paired rdd function, used for applying aggregation or reduction based on a key
print(splitrdd2.map(lambda x:(x[1],int(x[4]))).reduceByKey(lambda x,y:x+y).collect())

#[('chennai', 1), ('banglore', 1), ('chennai', 1), ('chennai', 1), ('Delhi', 1)]
#chennai,[3]

#join transformation - join operation row by row using the key column can be achieved using paired rdd (an rdd with key and the value pair)
#file1:
#1,irfan,41
#2,xyz,9
#file2:
#1,chennai,tn,M
#2,banglore,ka,U

rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
rdd2=sc.textFile("file:///home/hduser/sparkdata/empdata1.txt")
filrdd1=rdd1.map(lambda x:x.split(",")).filter(lambda x:x[1]=='chennai').map(lambda x:(x[0],(x[1],x[2],x[3],x[4])))
filrdd2=rdd2.map(lambda x:x.split(",")).filter(lambda x:int(x[4])>10000).map(lambda x:(x[0],(x[1],x[2],x[3],x[4])))
print(filrdd1.join(filrdd2).collect())#inner
print(filrdd1.leftOuterJoin(filrdd1).collect())#self
print(filrdd1.leftOuterJoin(filrdd2).collect())#left
print(filrdd1.rightOuterJoin(filrdd2).collect())#right
print(filrdd1.fullOuterJoin(filrdd2).collect())#full

#[('ArunKumar', (('chennai', '33', '2016-09-20', '100000'), ('chennai', '33', '2016-09-20', '100000'))), ('delhi babu', (('chennai', '37', '2019-02-20', '10000'), None)), ('irfan', (('chennai', '37', '2019-02-20', '10000'), None))]
#[('delhi babu', (('chennai', '37', '2019-02-20', '10000'), None)), ('ArunKumar', (('chennai', '33', '2016-09-20', '100000'), ('chennai', '33', '2016-09-20', '100000'))), ('irfan', (('chennai', '37', '2019-02-20', '10000'), None)), ('surya', (None, ('banglore', '43', '2016-09-23', '90000')))]

#Action Function:
# Actions are the functions that returns the result in a form of mere values or collection types.
# Action triggers the spark DAG to schedule (DAGScheduler & TaskScheduler) the work in a form of tasks
# Every spark program need to have atleast one action
#collect action , returns collection type
hadooplines= sc.textFile("hdfs://127.0.0.1:54310/user/hduser/empdata.txt")
maprdd=hadooplines.map(lambda x:x.split(","))
print(maprdd.collect()) #action--> trigger point for executing the RDD transformation
filterrdd=maprdd.filter(lambda x:int(x[4])>10000)#the filterrdd will not be materialized in memory at all
#only the DAG got created upto here
print(maprdd.collect())
#DAG will be executed only after I perform an action

#count action returns the total number of elements in an rdd, returns value type
print(maprdd.count())
#take action returns the first few elements as a sample , returns collection type
print(maprdd.take(3))
#first action returns the first element, returns value type
print(maprdd.first())

#reduce action returns the reduced/aggregated result of the given rdd (single column value), returns value type
value1=filerdd1.map(lambda x:int(x[4])).reduce(lambda x,y:x+y)
print(value1)

#count by value action - used to count the value occurances that returns dictionary type
value1=filerdd1.map(lambda x:x[1]).countByValue()
sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).countByValue()
print(filerdd1.map(lambda x:(int(x[2]),1)).countByKey())
print(filerdd1.map(lambda x:int(x[2])).countByValue())
#count by key action

#top action to take top values of the given rdd
print(filerdd1.map(lambda x:int(x[2])).top(3))

#Paired rdd functions are reduceByKey (T), countByKey (A), join (T), lookup (A)
pairedrdd1=filerdd1.map(lambda x:(x[1],int(x[2])))
#[('chennai', 33), ('banglore', 43), ('chennai', 37), ('chennai', 37), ('Delhi', 29)]
lkpoutput=pairedrdd1.lookup("chennai")
print(min(lkpoutput))

#save a text file action
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")
#print(rdd1.count())
filterrdd2=rdd1.map(lambda x:x.split(",")).filter(lambda x:x[7]=='California').map(lambda x:(int(x[0]),x[1],int(x[2]),float(x[3])))
#print(filterrdd2.count())
filterrdd2.saveAsTextFile("/user/hduser/californiadata12")

#interview question: how to find the number of elements in a given partition of an rdd using python program
#go and see in the ui to find the counnt of elements in each partitions
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1",4)
local_var=rdd1.glom().collect()
for i in local_var:
 print(len(i))


#1. After completing the above spark application in windows Pycharm repl, i will ship the code from windows pc to Dev/NP Edge node
#local mode, mention the master("local[*]") in the code itself (Dev/Non Prod cluster)
#run the code line by line or block by block
#pyspark

#run the code as a batch in local mode initially (everything will run in the edge node only)
#spark-submit sparkapp2.py

#before productionizing (Dev/NonProd cluster 10 nodes), we can run in client mode (remove master(local[*])) with yarn cluster manager to test, develop, debug, performance optimization
#spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2 sparkapp2.py
#We will remove unwanted print, collect, count and other actions used for just degbuging or testing

#after productionizing (Prod cluster), we can schedule it to run in cluster mode with yarn cluster manager (70%-80%)
#spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-memory 1g --executor-cores 2 sparkapp2.py

#Interview Question: After productionizing a spark job (Prod cluster), it is not performing well or some data issues are occuring, how do you fix it?
#solution is- start the pyspark repl in the production (Prod cluster) edge node with the same spark-submit configuration in client mode,
# for test, debug, performance optimization in production directly (23/06/18 09:53:54 - 23/06/18 09:54:42)
#spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2 sparkapp2.py
#pyspark --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2

#[hduser@localhost ~]$ cat spark_app1.py
from pyspark.sql import SparkSession
#spark=SparkSession.builder.master("local[*]").appName("Spark core learning WE41").getOrCreate()
spark=SparkSession.builder.appName("we41 application").getOrCreate()
#spark session object has the function related to core, sql (sql/hive)
sc=spark.sparkContext#just renaming the spark.sparkContext as sc for simplicity
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_big1")
#print(rdd1.count())
filterrdd2=rdd1.map(lambda x:x.split(",")).filter(lambda x:len(x)>=8).filter(lambda x:x[7]=='California').map(lambda x:(int(x[0]),x[1],int(x[2]),float(x[3])))
#print(filterrdd2.count())
filterrdd2.coalesce(16).saveAsTextFile("/user/hduser/californiadata14")

#rdd2=rdd1.map(lambda x:x.split(",")).filter(lambda x:len(x)==7).map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],'NA'))
#rdd2=rdd2.map(lambda x:x.split(",")).filter(lambda x:len(x)==6).map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],'NA','NA'))

#If no hadoop cluster is available, after productioninizg, we can schedule it to run in client mode with standalone cluster manager (20%-30%)
#spark-submit --master spark://localhost:7077 --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2 sparkapp2.py
#pyspark --master spark://localhost:7077 --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2

#NARROW/WIDE DEPENDENT TRANSFORMATIONS + SHUFFLING + STAGES
rdd1=sc.textFile("file:///home/hduser/hive/data/txns_4k",4)
rdd2=rdd1.map(lambda x:x.split(","))#narrow
rdd3=rdd2.filter(lambda x:x[7]=='California')#filter1 is narrow to map1, filter2 is narrow to map2 ....
rdd4=rdd3.map(lambda x:(x[4],float(x[3])))#map1 is narrow to filter1 and so on..
rdd5=rdd4.reduceByKey(lambda x,y:x+y)
#reducebykey is wide dependent on map1,map2,map3,map4 to shuffle/copy all required key/value from all map task output
#hence shuffle occurs to copy data from map task output partitions1,2,3,4 to reducebykey partition1,2,3,4
#rather than saying rbk dependent on map task1,2,3,4 we can say rbk1,2,3,4 stage is waiting for stage (map1,2,3,4 tasks)
#stages are sequencially depending to each other
#tasks runs inside the stages are parallel


#5. performance optimizations
#Caching
#Persist
#StorageLevels
#Partitioning
#Broadcasting
#Checkpoint

#Log Analysis Example: Calculate howmany errors and warning occured in our namenode using spark
#memory optimization - using cache/persist/unpersist/StorageLevel
rdd1=sc.textFile("file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log")
rdd2=rdd1.map(lambda x:x.split(" "))
rdd3=rdd2.filter(lambda x:len(x)>=3)
rdd3.cache()#(hits 6 times the fs for every action) retain in the memory (dont allow GC to clean after consumption) almost 200 mb of data in the memory
warnrdd=rdd3.filter(lambda x:x[2]=='WARN')#filter data in rdd3 memory from 200mb to 10mb
errorrdd=rdd3.filter(lambda x:x[2]=='ERROR')#filter data in rdd3 memory from 200mb to 1mb
#first action count, data will come from FS to rdd3
#second action take/saveastextfile will read the data from memory of rdd3
warnrdd.cache()#gc will not clean this warnrdd from the memory
#executor memory only occupies 10mb (benefit1) + I am doing filter once for all and performing action multiple times on the cache (benefit2)
print(warnrdd.count())
errorrdd.cache()#executor memory occupies 1mb
#OR
errorrdd.persist()#If we use persist without any arguments passed, then persist and cache are same and it can be used interchangably
print(errorrdd.count())
rdd3.unpersist()
print(warnrdd.take(2))
warnrdd.saveAsTextFile("/user/hduser/warnrdd1")
warnrdd.unpersist()
#if we dont use this rdd further and we have further lines of code that uses memory area, then unpersist it will fully
#unpersist asap when the rdd is consumed
print(errorrdd.take(2))
errorrdd.saveAsTextFile("/user/hduser/errorrdd1")
#errorrdd.unpersist() #No need to unpersist the errordd because the application will be terminated and release all resources
#last line of my spark application
#cache(), persist(),persist(StorageLevel.MEMORY_ONLY) - all these 3 are same (mem only with serialization)
#IP -> DISK1 -> RAM/DISK2 -> RAM -> CPU REGISTER -> DISK/MONITOR/PRINT
#persist() function will give more options for caching
#what is the difference between cache and persist?
#1. In general both are same (if we don't leverage persist with more options)
#cache will retain by default the data only in memory and persist will provide multiple options to retain the rdd data in
# only memory or memory with replica or disk only or mem&disk only or mem&disk with replica or mem with de serialized
#or off heap memory etc.,
from pyspark.storagelevel import StorageLevel #memory_only,disk_only,memory_and_disk....
warnrdd.cache()#equivalent to cache
warnrdd.persist() #equivalent to cache
warnrdd.persist(StorageLevel.MEMORY_ONLY)
#equivalent to cache (used in general by default)
warnrdd.persist(StorageLevel.MEMORY_ONLY_2)
#use only memory with 2 replica
# 1. prerequisite - if the source data volume is small to medium and if it fits in the memory with 2 replicas
# 2. in a business critical applications, if we need both performance (memory with speculative execution) and fault tolerance
warnrdd.persist(StorageLevel.DISK_ONLY) #equivalent to cache IN DISK ONLY
#use only disk with 1 replica
# 1. prerequisite - if the source data volume is VERY high and if it can't fits in the memory even with 1 replica
# 2. For batch applications (that run in non peak hours), non business critical applications (10x faster execution is good),
# if we need more fault tolerance and use less memory resource
warnrdd.persist(StorageLevel.DISK_ONLY_2)#serialized,
warnrdd.persist(StorageLevel.DISK_ONLY_3)#serialized
# 3. For replica 2 and 3, if we need fault tolerance in a higher degree (2/3) we go with replica 2 or 3 with medium performance 10X
# 4. increasing the number of replicas will increase data locality inturn help managing resources efficiently and performance also
warnrdd.persist(StorageLevel.MEMORY_AND_DISK)#serialized
warnrdd.persist(StorageLevel.MEMORY_AND_DISK_2)#serialized
#use both memory (upto the volume that can fit in memory (tachyon tool)) and disk (balance data cached in the disk) in a serialized (native spark java serializer) fashion
#1. prerequisite - If the source data is huge or variable in size, which cannot fits into the memory always and if we need medium to high performance.
#2. To avoid contacting the source system once again for collecting and process the remaining partial data.
# Instead, it can process it from the already collected data in the disk (serialized).
# 3. For replica 2, if we need fault tolerance/reliability with medium to high performance (10 to 100x performance)

# For memory_only, 2 replica, disk_only, 2/3 replica, mem_and_disk, 2 replica, we have the stored in serialized format by default
# 4. Storage occupied will be less but CPU overhead time taken for deserialization is more (memory/disk is happy to store serialized data, but cpu is not happy)

#warn_filterrdd4.persist(StorageLevel.MEMORY_AND_DISK_DESER) #de serialized data
#use both memory (upto the volume that can fit in memory (tachyon db)) and disk (balance data cached in the disk) in a deserialized (native spark serializer will not apply) fashion
#1. prerequisite - If the source data is huge or variable in size, which cannot fits into the memory always and if we need medium to high performance.
#2. To avoid contacting the source system once again for collecting and process the remaining partial data. Instead, it can process it from the already collected data in the disk.
# 4. Storage occupied will be more (because deserialized) but time taken for directly accessing the deserialized data is less for CPU (no CPU overhead for deserialization)
warnrdd.persist(StorageLevel.OFF_HEAP) #to leverage the complete memory capacity of our executors, we can use off heap also (preview mode)

#partitioning (Important)
#Partitioning is the horizontal division of data (we can delegate the work more efficiently)
# (HDFS -Blocks, mapreduce - input splits, Sqoop -mappers, hive - partitioning/bucketing, yarn - containers, spark - partitions,
# python - list(list1,list2,list3))
#[1,2,3,4,5,6] ->sum(6 times) -> [[1,2],[3,4],[5,6]] -> sum(3 times parallely)-> sum(1 time) ([3,7,11]) -> 21
#RDDs are distributed because of partitioning
#the degree of parallelism can be increased/decreased with the help of partitioning (coalesce/repartition respectively)
#We can create or redefine partitions at any point of time in a given spark program
#Number of partitions determines the number of parallel tasks in a given job

#Interview question : How to identify the number of elements in a given partition in an rdd by writing python program?
#answer1: I will go through the ui
#answer2:below code
for i in rdd1.glom().collect():
 print(len(i))

#32/128 respective for lfs/hdfs typical partition size, by default minimum no. of partitions is 2 if the size of the data is not min 32mb/128mb respectively for lfs/hdfs
#>32mb or >128mb respective for lfs/hdfs partitions will be calculated as totalsize/32mb or totalsize/128mb repectively
#programatic - total number of cores by default or overload with your desired partition numbers

#Controlling partitions at the RDD creation time:
#rdd1=sc.textFile("")
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
#dynamically increase or decrease the number of partitions based the size of the data

#Interview question2: How do we dynamically handle the number of partitions based on the volume of data (at all levels Extraction/Transformation/Load)
# (initially or in the mid of the program before/after some transformations are applied or at the time of storing the output)
# If the volume of data after applying some transformations is reduced, then use coalesce to reduce the number of partitions accordingly
# If the volume of data after applying some transformations is increased, then use repartition to increase the number of partitions accordingly
#initially when do we change/define the number of partitions?
#1. possibilities of defining number of partitions in the extraction stage itself
rdd1=sc.textFile("file:///usr/local/hadoop/logs/yarn-hduser-nodemanager-localhost.localdomain.log",8)
print(rdd1.getNumPartitions())
rdd1_cnt=rdd1.count()
if rdd1_cnt < 800000:
    rdd2_repart=rdd1.coalesce(4)
else:
    rdd2_repart = rdd1.repartition(10)

print(rdd2_repart.getNumPartitions())#248mb of data is loaded with 10 partitions (valid)

#2. possibilities of defining number of partitions in the transformation stage
rdd3_filter=rdd2_repart.filter(lambda x:'WARN' in x)#after filtering 10mb of data is loaded with 10 partitions (not preferable)
print(rdd3_filter.count())#27667
print(rdd3_filter.getNumPartitions())#10
rdd4_repart=rdd3_filter.coalesce(2)#reducing the number of partition from 10 to 2, since the volume of data data is reduced
print(rdd4_repart.getNumPartitions())#2

rdd3_flattened=rdd2_repart.flatMap(lambda x:x.split(' '))#after flatmap 248mb of data (of 17 million) is loaded with 10 partitions (not preferable)
print(rdd3_flattened.count())#17077443
print(rdd3_flattened.getNumPartitions())#10
rdd5_repart=rdd3_flattened.repartition(20)#increasing the number of partition from 10 to 20, since the volume of data data is increased from .9 million to 17 million
rdd5_map=rdd5_repart.map(lambda x:x.upper())

#3. possibilities of defining number of partitions in the load stage
#Interview question1: If you are writing the output of an rdd or a dataframe or a spark temp view, the output in the fs or cloud
#or db is generated with more number of small files/partitions, how do you make it to one or fewer files of large in size? coalesce
rdd4_repart.coalesce(1).saveAsTextFile("/user/hduser/filtered_rdd")#HDFS expects  large sized files (5.4mb) of small in numbers (1 file) , rather storing as 2.7mb into 2 files
rdd5_map.coalesce(2).saveAsTextFile("/user/hduser/flatmap_rdd")#HDFS expects large sized files (120mb) of small in numbers (2 files), rather storing as 10mb into 20 files


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
#(1st min answer) coalesce will be used for reducing the number of partitions and repartition is used for increasing the number of partition
rdd3.getNumPartitions()
#4
rdd3.coalesce(2).getNumPartitions()
#2
rdd3.repartition(8).getNumPartitions()
#8

#but can be used vice versa (not suggested)
rdd3.coalesce(8,True).getNumPartitions()#not suggested to increase the number of partitions
#8
rdd3.repartition(2).getNumPartitions()#not suggested to decrease the number of partitions
#2

#(2nd answer) both repartition and coalesce will call coalesce in the background.
# repartition calls coalesce(numpart,shuffle=True) where as coalesce will calls coalesce(numpart,shuffle=False)
rdd3.coalesce(8,True).getNumPartitions()#reparition
rdd3.coalesce(8,False).getNumPartitions()#coalesce
#(3rd answer) Coalese use random or range partitioning where as repartition uses round robin partitioning
#(4th answer) Coalese use random or range partitioning the distribution of data across the partition will be inequally
# where as repartition uses round robin partitioning where the data across the partition will be equally distributed
rdd3.repartition(4).glom().collect()
'''
for (i <- rdd1.repartition(4).glom.collect()) {
     | println(i.length)}
25
25
25
25

'''

'''
for (i <- rdd1.repartition(8).coalesce(3).glom.collect()) {
     | println(i.length)}
26
36
38

'''

#a. We use coalesce if we find the volume of data is lesser than one partition worth (rdd with 8 partitions, each partitionw with only 4 mb),
#then coalesce to lesser number
# or if the functions (sql/dsl/df/rdd transformation) applied results lesser volume of data, then coalesce to lesser partition
#b. We use repartion if we find the volume of data is more than one partition worth (rdd with 2 partitions, each partition with 200 mb),
#then repartition to higher number (consider the executor numbers and cores per executor also)
# or if the functions (sql/dsl/df/rdd transformation) applied results in more volume of data, then repartition to higher number of partition

#broadcasting - concept of broadcasting the local value from driver to the executor nodes once for all
rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt").map(lambda x:int(x.split(",")[4]))
local_bonus=1000
#every time for every iteration of map, it will read the data from the driver
#not optimistic, because the data copied on demand for every iteration of the mapper
print(rdd1.map(lambda x:x+local_bonus).collect())
#[101000, 91000, 11000, 101000, 21000]

#more optimistic, because the broadcast of the data happens only once per node
#every time for every iteration of map, it will read the data from the local node rather than the driver if we do broadcasting
broadcast_bonus=sc.broadcast(local_bonus)
print(broadcast_bonus.value)
#1000
print(rdd1.map(lambda x:x+broadcast_bonus.value).collect())
#[101000, 91000, 11000, 101000, 21000]

#Checkpointing (spark streaming) - Use checkpoint if the source data is variable/streaming in nature.
#Interview Question: Difference between checkpoint and caching/persisting or getConfigFile()
#Checkpointing is the persistance of the rdd data into filesystem (local/hdfs) by TRUNCATING the LINEAGE
#does checkpoint == persist(StorageLevel.DiskOnly) ? NOT exactly the same
#checkpoint (doesn't not maintain the lineage of the parent rdds) <> persist (retains the lineage of the parent rdds)
#checkpoint is more good for fault tolerance, reliability & performance (if the data is streaming)
# whereas persist(Disk_only) is good for reliability and performance (if the data is batch)
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
#reliable (HDFS) or non-reliable(local) storage
#if the base rdd is variable in nature, because of streaming source data, then go with checkpoint
#if the data checkpointed is lost for some reason, i can't get the data from the parent rdd using lineage (bcz lineage is deleted after checkpoint happend)
rdd2.persist(StorageLevel.DISK_ONLY)
#If this disk is lost, I can get the data from the original source if i do persist
sc.setCheckpointDir("hdfs:///user/hduser/checkpointrdd3")
rdd2.checkpoint()
rdd2.count()#read from original source for the first time
#The data will be materialized in the memory and into the checkpoint location also parallely,
# then count will execute on the memory data and then the rdd1 data in memory will be cleaned by the GC
# and finally the DAG (lineage) in the driver for the rdd2 creation will be deleted
rdd2.count()#read from checkpoint dir
#subsequent actions depends on the checkpointed data only and apply the action without falling back on the parent rdd since lineage is deleted.

#irfan(source) -> jaison (receive memory) -> write to the memory(delete GC)/memory(cache)/notebook(persist)/notebook(checkpoint) disconnect from irfan's class -> going forward refer notebook



