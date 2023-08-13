import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
#from wd30.sparklearning.streaming import testApp2
class SparkETL_ELT_Unit_Test(unittest.TestCase):
# test function to test equality of two value
    def test_cols(self):
        print("define spark session object (inline code)")
        spark = SparkSession.builder \
            .appName("Very Important SQL End to End App") \
            .config("spark.jars", "/home/hduser/install/mysql-connector-java.jar") \
            .enableHiveSupport() \
            .getOrCreate()

        print("Set the logger level to error")
        spark.sparkContext.setLogLevel("ERROR")
        custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                      StructField("custfname", StringType(), False),
                                      StructField("custlname", StringType(), True),
                                      StructField("custage", ShortType(), True),
                                      StructField("custprofession", StringType(), True)])
        custdf_clean = spark.read.csv("file:///home/hduser/hive/data/custsmodified", mode='dropmalformed',schema=custstructtype1)
        scols=custdf_clean.columns
        sum
        tcols=custdf_clean.withColumn("a",lit('a')).columns
        scnt=custdf_clean.count()
        dedupcnt=custdf_clean.dropDuplicates().count()
        nopilotDF=custdf_clean.where("custprofession<>'Pilo'").select("custprofession").distinct()
        nopilotlst=list(nopilotDF.rdd.map(lambda x: x.custprofession).collect())
        # Unit test1: Both DFs should not have same columns
        with self.subTest():
            self.assertNotEqual(scols, tcols, "First value and second value are equal, hence unit test1 fails")
        # Unit test2: Data should not be duplicate
        with self.subTest():
            self.assertEqual(scnt, dedupcnt, "First DF count and second DF are not equal, hence unit test2 fails")
        # Unit test3: Pilot data should not present
        with self.subTest():
            self.assertNotIn('Pilot',nopilotlst,"pilot data is present")