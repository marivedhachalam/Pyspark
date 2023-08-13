print("this is just a python program to apply bonus for salaries")
sal_lst=[10000,20000,30000,15000]
bonus=1000
upd_sal_lst=[]
for i in upd_sal_lst:
    upd_sal_lst.append(i+bonus)
print(upd_sal_lst)

lam_func=lambda sal:sal+bonus#anonymous function
def func1(sal):#don't create regular function, because this is not going to be used accross the application
    return sal+bonus #closure
print(list(map(func1,sal_lst))) #higher order function

print("Convert this python program to Spark (Core) Program")
#to write a spark program we have to follow certain steps:
#1. import the respective spark libraries into this program environment
from pyspark.sql.session import SparkSession
#2. Create a spark session object to start invoke rdd/sql functions
#spark=SparkSession()
#/usr/local/spark/python/pyspark/sql/session.py
sparkSessionObject=SparkSession.builder\
    .appName("WE41 spark app")\
    .getOrCreate()
#The entry point to programming Spark with the Dataset and DataFrame API.
# A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
#tables, execute SQL over tables, cache tables, and read parquet files. To create a :class:`SparkSession`, use the following builder pattern:
#print(spark)
#sparkSessionObject=SparkSession.builder.master("local[2]").appName("WE41 spark app").enableHiveSupport().getOrCreate()
print(sparkSessionObject)
#When sparksession object is created, I will instantiate sparkContext, SQLContext and HiveContext(optional) together
#3. Use sparkSession.sparkContext object to create RDDs
spark=sparkSessionObject
#from pyspark.context import SparkContext
abcd=sparkSessionObject.sparkContext#as a part of spark core learning - we are going to use spark context for now to write rdd programs
#Main entry point for Spark functionality. A SparkContext represents the connection to a Spark cluster, and can be used to create :class:`RDD` and
#broadcast variables on that cluster.
surcharge=100
rdd1=sparkSessionObject.sparkContext.textFile("file:///home/hduser/cust.txt").map(lambda x:x.split(",")).map(lambda x:x[0])#spark core - RDD way of writing code
#rdd1=sc.parallelize(sal_lst)#defining an rdd (lazy evaluator/lazy executor)

#4. Start write RDD transformations and actions to achive the above results
#rdd2=rdd1.map(lam_func)#transformation on the rdd because rdds are immutable
print(rdd1.collect())#action to trigger the transformation to get executed and to see the result of a given rdd

#5. I don't wanted to operate RDDs, I want to simple write SQL queries
#df1=sparkSessionObject.read.csv("file:///home/hduser/cust.txt")
#df1.select("_c0").show()#spark sql - DSL way of writing code

#Interview question? I give a linux file with 4 columns, perform an etl to store the 4 col data to 2 col data of first and 3 column to be considered
#df1=spark.read.csv("file:///home/hduser/cust.txt").select("_c0","_c2")
# df1.write.csv("hdfs:///user/hduser/outdata/")
# df1.write.json("hdfs:///user/hduser/jsonoutdata/")
#what is this spark means?
#it is a spark session object which is an instance of SparkSession class which inturn instantiate sparkContext, SQLContext, HiveContext
#how do you get it created?
#spark.builder.getOrCreate()
