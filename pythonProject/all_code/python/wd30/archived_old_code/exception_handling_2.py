

#(pre-defined/system-defined) , (user-defined / user raise exception)
#statement level exception or block level exception
#try-except-else-finally

print("Example1: pre defined - statement level exception")
print("control will go to the exception then continue run the next set of statements in the next block")
try:
    num1=int(input("enter number1 to multiply"))
except ValueError as detailsOfError:
#    print(f"ValueError: {detailsOfError}")
#    exit(1)
    #ValueError: invalid literal for int() with base 10: 'ten'
    print(f"User you made some errors: {detailsOfError}")
    print(f"provide the right value eg:10 or 20, running a default multiplication program, as per the eg. given below")
    num1 = 10
    num2=20
    print(f"product of 2 numbers is {num1 * num2}")
    #exit(1)




try:
    num2=int(input("enter number2 to multiply"))#if anything is wrong here, then the control will be moved to except block
    print("If anything goes wrong in the above line of the code, then I will not be printed")
    print(f"product of 2 numbers is {num1*num2}")
except ValueError as detailsOfError:
    print(f"User you made some errors: {detailsOfError}")
    print(f"provide the right value eg:10 or 20, running a default multiplication program")
    num2 = 10
    print(f"product of 2 numbers is {num1 * num2}")

#(pre-defined/system-defined) , (user-defined / user raise exception)
#statement level exception or block level exception
print("Example2: pre defined - Block level exception,if any one statement inside the block went wrong, the total")
#control will go to the exception without executing the rest of lines of code
import json
try:#block level exception
    #num1=int(input("enter number1 to multiply"))
    file1 = input()
    file2=open(file1)
    jsonfile=json.load(file2)
    print("No exception occured")
    print(jsonfile)
except ValueError as detailsOfError:
    print(f"Give the right value : {detailsOfError}")
except FileNotFoundError as detailsOfError:
    print(f"filenot found: {detailsOfError} , opening default file")
    #file2=open("/home/hduser/sparkdata/file2.json")
    #jsonfile=json.load(file2)
    #print(jsonfile)
    #print(f"provide the right value eg:10 or 20, running a default multiplication program")
    exit(10)
else:#else case for exception, ie if no exception occurs then else block will be executed
    print("Json is parsed and no exception occured")
finally:#either exception with exit or no exception occured, finally block will be called
    print("mainly used to close the files or db connections or cleaning up of resources used")
    print("whether exception occured or not, finally will be executed")
#/home/hduser/sparkdata/file2.json

print("I will not be printed if exit is used in the exception handler block,if exception occurs")

'''try:
    num2=int(input("enter number2 to multiply"))#if anything is wrong here, then the control will be moved to except block
    print("If anything goes wrong in the above line of the code, then I will not be printed")
    print(f"product of 2 numbers is {10*num2}")
except ValueError as detailsOfError:
    print(f"User you made some errors: {detailsOfError}")
    print(f"provide the right value eg:10 or 20, running a default multiplication program")
    num1=10
    num2 = 10
    print(f"product of 2 numbers is {num1 * num2}")
'''

print("Example3 - user defined, user raised exceptions and graceful handling of exceptions")
#user defined exception (rarely used)
class InceptezCustomException(Exception):
    pass

try:#block level exception
    idx=int(input("enter numerator index"))
    den1 = int(input("enter a nonzero denominator number"))
    lst=[100,200,300,400]
    maxidx=len(lst) - 1
    if (maxidx>=idx):
        num1=lst[idx]
    else:
        num1 =lst[maxidx]
    print(num1)
    if den1 != 0:
        dividedresult=num1/den1
        print(dividedresult)
    else:
        dividedresult=num1/1
        print(f"default denominator of 1 is used, {dividedresult}")
    sumresult = num1 + dividedresult
    print(sumresult)

    if(den1==0):#user raised exception whenever needed
        print("finally failing the program since the denominator is zero")
        #raise InceptezCustomException
        raise ValueError

except ValueError as errmsg:
    print(f"value error: {errmsg}")
except NameError as  errmsg:
    print(f"provide the denominator error: {errmsg}")
    #dividedresult = num1 / den1
    #print(f"result {dividedresult}")
except ZeroDivisionError as  errmsg:
    print(f"provide the non zero denominator: {errmsg}")
except IndexError as  errmsg:
    print(f"provide the right index value: {errmsg}")
    print(len(lst)-1)
except InceptezCustomException as somemsg:
    print(f"custom error occured: {somemsg}")
    exit(1)
except Exception as somemsg:
    print(f"some error occured: {somemsg}")
    exit(1)

#try:#dividing the above block to statement level exception
#    sumresult = num1 + den1
#    print(sumresult)
#except Exception as somemsg:
#    print(f"some error occured: {somemsg}")


