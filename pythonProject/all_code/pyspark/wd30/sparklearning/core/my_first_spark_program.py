print("I am just a python program")
#import libraries
#instantiate the SparkSession class in a form of spark session object
#rdd=sc.textFile("file:///home/hduser/cust.txt")
#SparkSession(class)-> Builder(class)->master, getOrCreate,appName,enableHiveSupport
#spark=SparkSession(args) #sparkSession object
print("Lets convert the python program environment to spark program ready environment")
#prerequisite:
# 1. install pyspark software in the pycharm IDE (pip install pyspark==3.1.1)
# 2. reference the SparkSession class from the below pkg.subpkg - from pkg.subpkg.module import class
# 3. Create a spark session object in the name of abcd or spark by instantiating SparkSession class using builder object and some special functions
# like master, appName,enableHiveSupport, getOrCreate, config ...
# 4. Start write any ETL/ETL/Data pipeline in spark using spark core/sql/hive
from pyspark.sql.session import SparkSession
#My Aim is to create an object for SparkSession class?
#SparkSession -> Builder class
#builder_object=Builder() class instantiated
#builder.master/appName/enableHiveSupport/getOrCreate
#spark=finally SparkSession class will be instantiated with all the options applied using the above functions
spark=SparkSession.builder.master("local").appName("wd30 spark learning app").enableHiveSupport().getOrCreate()
#above spark session object supports spark core (sparkContext), sql (sqlContext) and spark hive (hiveContext) programming also
#/usr/local/spark/python/pyspark/sql/session.py
#SparkSession - The entry point to programming Spark with the Dataset and DataFrame API.
# A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as tables,
# execute SQL over tables, cache tables, and read parquet files. To create a :class:`SparkSession`, use the following builder pattern
#master() - Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
# to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster or yarn or mesos or kuberneties
#appName() - Sets a name for the application, which will be shown in the Spark web UI
#enableHiveSupport() - Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive SerDes, and Hive user-defined functions
#getOrCreate() - Gets an existing :class:`SparkSession` or, if there is no existing one, creates a new one based on the options set in this builder.
#i am creating entry point to access spark application/prog
print(spark)
spark1=SparkSession.builder.master("local").appName("wd30 spark learning app").enableHiveSupport().getOrCreate()
print(spark1)
#spark=SparkSession.builder.getOrCreate()
#schema migration using spark SQL
df1=spark.read.csv("file:///home/hduser/cust.txt").toDF("custid","city","product","amt")
df1.select("city").show()
df1.write.mode("overwrite").json("file:///home/hduser/jsonout/")
#ETL/ELT programs (data pipeline)

print("1. How to Work in Spark Core Programming - Direct RDD operation using transformation and action functions (not much important, but should know)")
#for attending some interviews - we need spark core programming
#spark Session object included spark context object also, hence we can start write spark core programs also
sc=spark.sparkContext
#sc=bonnet/engine (feel the heat of the engine)
#spark=driver seat of a car (lavish options music, ac, cruise)
print("Possible ways of creating RDDs")
rdd1=sc.textFile("file:///home/hduser/cust.txt")
rdd2=rdd1.filter(lambda x:x==x)
print(rdd2.collect())