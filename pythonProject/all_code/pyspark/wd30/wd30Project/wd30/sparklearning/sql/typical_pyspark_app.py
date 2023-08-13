#from pkg.subpkg.module import class
#1.importing of required libraries
import sys
from pyspark.sql.session import SparkSession
print("outside i am keeping libraries to invoke")
#2. create the main method and write all your code inside the main method
def main(args):
# 1. purpose of keeping our code in main method is to run only the code that we have developed
# and rest of all code developed as a library or framework (spark) just has to be referenced and not run
# 2. main method will help us make an application executable/callable in a controlled fashion (from line number 16)
# 3. for passing parameters/arguments main method place a vital role
    spark=SparkSession.builder.getOrCreate()
    df1=spark.read.csv(args[1])
    df1.show()
    print("inside i am running some code")

#3. We will call and execute the main program (from where the execution of the module will be started)
if (__name__=="__main__"):
    print(sys.argv)
    if (len(sys.argv)==2):
        main(sys.argv)
    else:
        print("pass the necessary arguments")
        sys.exit(100)
    #main("file:///home/hduser/sparkdata/empdata1.txt")