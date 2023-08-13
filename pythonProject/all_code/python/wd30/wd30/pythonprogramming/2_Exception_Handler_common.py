import os
import sys

# various exception combinations
# keywords: try, except, finally
print("************** 5. Exception handling **************" )

print( " ****** Statement level except Be cautious to Write in the below way to handle in one try block with one exception captured.. If any exception occured the try block will not exit out, but we can exit explicitly using exit")
try:
    print("Try Block1")
    num1 = int(input())
    str1=str(input())
    str1 + num1
    #print(num1+num1) #call the else block
except IndexError as index_err:
    print("Cannot print list item. Exception: {} , i will exit out if u enable exit(1)".format(index_err))
    #exit(1)
except Exception as i_store_the_errormessage:
    # without except block try block will throw error
    print("General Exception Block, error message is -> {} ".format(i_store_the_errormessage))
    #sys.exit(1)
else:
    print("I will be executing if no exception occurs in the try block - closing connections created in the try block")
finally:
    print("I will be executing at any cost, whether exception occured or not - closing all connections created in this code")

try:
    print("Try Block2")
    vowels = ['a', 'e', 'i', 'o', 'u']
    idx=int(input())
    if len(vowels)>idx:
     print(vowels[idx])
    else:
        print(vowels[len(vowels)-1])
except IndexError as index_err:
    print("Cannot print list item. Exception: {} , i will exit out the whole code without further blocks executed if u enable exit(1)".format(index_err))
    #exit(1)

try:
    print("Try Block3")
    size = os.path.getsize("1_python_core.py")
    print("{} Bytes".format(size))
except FileNotFoundError as file_err:
    print("Unable to retrieve size. Exception: {} ".format(file_err))

try:
    print("Try Block4")
    variable_one = "Hello Good morning"
    print(variable_two)
except NameError as name_err:
    print("Unable to retrieve variable. Exception: {} ".format(name_err))

try:
    print("Try Block5")
    print(10 / 1); #exception called automatically
    if 10/1:
        raise ZeroDivisionError #raise exception programatically
except ZeroDivisionError as operation_err:
    print("Raising custom/interpreter exception  {} ",format(operation_err))

finally:
    # finally block gets executed regardless of exception generation
    # optional block for try-except group
    print("Executing finally block at any cost");

try:
    print("Try Block6")
    user_input = int(input("enter a number "))
    print("received input: {}".format(user_input))
except ValueError as input_err:
    print("Unable to retrieve user input. {}".format(input_err))
    #sys.exit(1)

print( " ****** Block level exception - Write in the below way to handle in one try block with all exceptions captured.. If any exception occured the try block will exit out")
print("preferred way")
try:
    print("Try WHOLE block")
    num1 = 10
    #"Strongly typed" + num1
    print(num1+num1)

    vowels = ['a', 'e', 'i', 'o', 'u']
    print(vowels[4])

    variable_one = "Hello Good morning"
    print(variable_two)

    print(10 / 1);
    print(10/0);

    user_input = int(input("enter a number "))
    print("received input: {}".format(user_input))
except IndexError as index_err:
    print("Cannot print list item. Exception: {}".format(index_err))
except NameError as name_err:
    print("Unable to retrieve variable. Exception: {} ".format(name_err))
except ZeroDivisionError as operation_err:
    print(operation_err)
except ValueError as input_err:
    print("Unable to retrieve user input. {}".format(input_err))
    #sys.exit(1)
except Exception as general_error:
    # without except block try block will throw error
    print("General Exception Block. {} ".format(general_error))
    #exit(1)
else:
    print("I will be executing if no exception occurs in the try block")

finally:
    # finally block gets executed regardless of exception generation
    # optional block for try-except group
    print("Executing finally block at any cost");


# You can refer for various exceptions in python: https://docs.python.org/3/library/exceptions.html