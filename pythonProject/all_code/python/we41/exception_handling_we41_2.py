#else block in python can be used in if, while, except
#EXCEPTION HANDLING
#Understanding of Exception: what, exception handler blocks (try,except,else,finally)
#Exception handling methodologies: Block level or Statement level - raise exceptions
#Types of Exception: Predefined (TypeError, ValueError, KeyError,DivideByZero,IndexError,FileNotFound..) & Userdefined
#importance - Dataengineer less (already framework spark handled the exceptions)
#importance - python developer (application) medium (already framework spark handled the exceptions)
# Exception is a error occurrs when the program is executing at any stage for any
# reasons (data error, name/key/value error, type error, environment error, file not found, format error)
# and makes the flow of program terminated abruptly
# Exception Handler (graceful handling of program termination)
# help us handle or take the control from the abrupt termination of the program - is a construct/code that help us handle the state of exception
#Example:
#exception->exceptional cases, unexpected events, unusual..
#1. try - I am going to a store to buy something to my home (plan it perfectly to avoid any unexpected events)
    #take a vehicle
    # go to shop,
    # add different items in the cart,
    # pay the bill,
    # come back home
#2. except - any unexpected event occurs, handle it accordingly
    #exception1 - trip got cancelled because of some other priority work came
    #exception2 - vehicle is not starting, but i may use some other vehicle or go by walk or abort my journey
    #exception3 - shop is closed
    #exception4 - some products are not available...
    #exp5- card declined/not accepted or wallet is lost
    #exp6 - vehicle is not starting
    #exp7 - something went wrong which i didn't predicted (expect unexpected)
#3. else - (If except doesn't happened) If I am not getting any exception, what I have to do then?
    #ensure to clean, lock your vehicle when you leave the vehicle in the home
#4. (If 1 or 2  happened) If I am not getting any exception or I got some exception, what I have to do?
    #ensure to clean, lock your vehicle when you leave the vehicle in the home
    #plan for some other alternative journey
#1 and 2 are mandatory
#3 and 4 are not mandatory

#4 block of exception handler in python try,except,else,finally
'''shopname=int(input("enter the shop number"))#program aborted without any handler
print(shopname)
print("go to the shop...")
list_of_products_in_the_shop={"pencil":2,"oil":100}
list_to_purchase=["pencil","oil","fanta"]
shopping_cart=[]
for prod in list_to_purchase:
   shopping_cart.append(list_of_products_in_the_shop[prod])
   print(f"amout to pay finally {sum(shopping_cart)}")'''

print("BLOCK LEVEL EXCEPTION - If any lines/statements of the try block is failed (exception handler runs) \
and rest of the lines/statement of code will NOT BE execute")
print("Example1 - simple BLOCK level exception handling with few exception types")

try:
    shopname=int(input("enter the shop number"))#program aborted without any handler
    #any exception occured in the line 46 will take the control to line 68->72,73,74->85,86,87
    list_to_purchase=input("provide the list of items to purchase")
    list_to_purchase = list_to_purchase.split(",")
    print(list_to_purchase)
    print(shopname)
    print("go to the shop...")
    list_of_products_in_the_shop={"pencil":[2,24],"oil":[100,1],"notes":[50,12]}

    #list_to_purchase=["pencil","oil","fanta"]
    shopping_cart_price=[]

    #print(f"identify only the products in the shop and complete your purchase")
    #set(list_of_products_in_the_shop.keys()).intersection(set(list_to_purchase))
    list_of_items_matching_with_my_shopping_list=set(list_of_products_in_the_shop.keys()).intersection(set(list_to_purchase))
    for prod in list_of_items_matching_with_my_shopping_list:
       print(prod)
       shopping_cart_price.append(list_of_products_in_the_shop[prod][0])#anything goes wrong any other line of code will not be executed
       if list_of_products_in_the_shop[prod][1] <= 3:
           msg1="product expiry date is not meeting my criteria, hence aborting the shopping"
           raise Exception#if no exception occured of its own, we can raise any type of exception when needed
       print(f"product in the list to purchase {prod}")
    print(f"If any exception occurs in the above line, this statement will not be called - Insert an entry in the audit/log table with the given error")
    totalamt=sum(shopping_cart_price)
    promo=10
    finalamt=totalamt-promo
    print(f"amout to pay finally {finalamt}")

except KeyError as msg:
    #print(f"some exception occured ")
    print(f"Some of the products is not there in the shop, hence aborting my purchase {msg}")
    print(f"Insert an entry in the audit/log table with the given error {msg}")
except ValueError as msg:
    print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")
    print(f"Insert an entry in the audit/log table with the given error {msg}")
except Exception as msg:
    print(f"some exception occured {msg1}")

    '''except ValueError as msg:
        #print(f"some exception occured ")
        print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")'''
    #exit(1)
else:#if no error in the try block, execute the below line of code
    print("Insert into the log table (if no exception occured)- Shopping completed successfully, going back to home")
    print("closing the db connections or close the files openened ")
finally:#at any cost this block will run, either exception occured or not
    print("closing the db connections or files openened or deleting the temp files ")
    print("Insert into the log table (if either exception occured or not)- cleaning & locking my vehicle")

print("Example2 - complete BLOCK level exception handling with possible exception types")
try:#block level exception
    print("logic1 (stataments)")
    input_lst=input("Enter some list of elements of the students")
    input_lst=input_lst.split(",")
    input_idx=int(input("Enter the rank to print the student name"))
    print(f"The student name for the given rank {input_idx} is {input_lst[input_idx-1]}")
    print("logic2 (stataments)- didn't run the below statements when block level exception is used")
    def func1():
        name_not_defined=100#this variable can be only used inside the function, not outside
    #print(f"{name_not_defined}")
    input_numerator = int(input("Enter the numerator number amount total"))
    input_denominator=int(input("Enter the denominator number total customers"))
    print(f"The value of the average purchase rate is {input_numerator/input_denominator}")
    print("logic3 (stataments)- didn't run the below statements when block level exception is used")
    f1="/home/hduser/cust1123.txt"
    file1=open(f1)

except IndexError as msg:
    print(f"Given index {input_idx} is not available in the given list {input_lst} of items, max of index you can pass {len(input_lst)} - {msg}")
except NameError as msg:
    print(f"The variable used inside the program is not defined anywhere")
except ZeroDivisionError as msg:
    print(f"The denominator provided should be non zero")
except KeyError as msg:
    #print(f"some exception occured ")
    print(f" {msg}")
    print(f"Insert an entry in the audit/log table with the given error {msg}")
except ValueError as msg:
    print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")
except FileNotFoundError as msg:
    print(f"Given file doesn't exist {msg}")
except Exception as msg:
    print(f"some exception occured {msg}")

print("Example3: user defined exceptions and raise exception")
#Understanding of Exception: what, exception handler blocks (try,except,else,finally)
#Exception handling methodologies: Block level or Statement level - raise exceptions
#raise Exception#if no exception occured of its own, we can raise any type of exception when needed
#Types of Exception: Predefined (TypeError, ValueError, KeyError,DivideByZero,IndexError,FileNotFound..) & User defined

class DenominatorException(Exception):
    print("Denominator should not 1")
    #pass

print("STATEMENT LEVEL EXCEPTION - If any lines/statements of the try block is failed (exception handler runs) and rest of the lines/statement of code will BE execute")
try:#statement level exception
    print("logic1 (FIRST set of stataments)")
    input_lst=input("Enter some list of elements of the students")
    input_lst=input_lst.split(",")
    input_idx=int(input("Enter the rank to print the student name"))
    print(f"The student name for the given rank {input_idx} is {input_lst[input_idx-1]}")
except IndexError as msg:
    print(
        f"Given index {input_idx} is not available in the given list {input_lst} of items, max of index you can pass {len(input_lst)} - {msg}")
except NameError as msg:
    print(f"The variable used inside the program is not defined anywhere")
except ValueError as msg:
    print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")
except Exception as msg:
    print(f"some exception occured {msg}")

try:#statement level exception
    print("logic2 (stataments)- didn't run the below statements when block level exception is used")
    def func1():
        name_not_defined=100#this variable can be only used inside the function, not outside
    #print(f"{name_not_defined}")
    input_numerator = int(input("Enter the numerator number amount total"))
    input_denominator=int(input("Enter the denominator number total customers"))
    if (input_denominator==1):
        msg="denominator error"
        raise DenominatorException
    print(f"The value of the average purchase rate is {input_numerator/input_denominator}")

except NameError as msg:
    print(f"The variable used inside the program is not defined anywhere")
except ZeroDivisionError as msg:
    print(f"The denominator provided should be non zero")
    exit(2)#help us convert to block level exception if needed for certain exceptions alone
except ValueError as msg:
    print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")
    exit(1)
except DenominatorException as msg:
    print(f"enter the denominator <>1 -  {msg}")
except Exception as msg:
    print(f"some exception occured {msg}")

try:#statement level exception
    print("logic3 (stataments)- didn't run the below statements when block level exception is used")
    f1="/home/hduser/cust1.txt"
    file1=open(f1)
except FileNotFoundError as msg:
    print(f"Given file doesn't exist {msg}")
except Exception as msg:
    print(f"some exception occured {msg}")
    print("logic3 (stataments)- didn't run the below statements when block level exception is used")
