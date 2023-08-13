#requirement: write a STANDARD spark sql/DF application to read the comma del data from linux fs and convert to orc format and store into hdfs
#1. reference all related libraries or packages or modules or framework
import sys
from pyspark.sql.session import SparkSession

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

print("1 hello is from the module")#uncontrolled way we are running this
def main(args):#main method with/without input arguments but without any return values
    print("3 hello is from the main program")#controlled way we are running this
    #2. Create the spark session object
    spark=SparkSession.builder.getOrCreate()
    #spark -> sparkContext, sqlContext, hiveContext
    #3. Create DF and store the df in the required format
    df1=spark.read.option("header","true").option("inferschema","true").csv(args[1])
    df1.show()
    #df1.write.mode("overwrite").orc("/user/hduser/orcdata/")
#name='irfan'
if (__name__=="__main__"):#I am the starting point of this entire code
    print("2 Calling the main program")
    print(sys.argv)#["name of this module",arg1,arg2....]
    if (len(sys.argv)>=2):
        main(sys.argv)#call the main method with arguments (uncontrolled fashion)
    else:
        print("pls pass the necessary arguments to run this program eg: modulename.py file:///home/hduser/sparkdata/empdata_header.txt")
        sys.exit(100)

#Code can be writterned by 2 type of people?
#1. type1 manufacturer-> the framework developers (contributors/commiters) ASF
#how the hierarchy (standard) of programming - pkg-subpkg-module-class-function/methods-base code
#2. type2 consumer-> the core developers - consumer
#how the hierarchy (standard) of programming - pkg-subpkg-module-main method(control/arguments/configuration)-coding(leveraging base code)