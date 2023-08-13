print("Lets learn the funda of programming using Python")
print("A. Python is an indent based programming language")
print("enter the name to print")
#myname=input()
myname="irfan"
print("enter the number to decide 1 for print 0 for not printing")
#userinput=int(input())
userinput=1
if (userinput==1):
 print(myname)
 print("Good morning team")
 if (userinput==0):
  print("hello")
print("I am outside the above if conditions")

print("B. This is a commented line in Python")
#single line comment
'''
multi line comments
'''

"""
multi line comment
comment2
comment3
"""

print("C. Python treats single quotes the same as double quotes.")
myname="Inceptez technologies pvt. ltd."
myname='Inceptez technologies pvt. ltd.'
#single quote can't be used directly in this below case?
myname='Inceptez technologies\'s product'
myname="Inceptez technologies's product"
print(myname)
#double quotes can't be used directly in this below case of using multiple lines or if the content has double quotes
statement="""welcome "piyush" to inceptez technologies 
        lets learn python programming"""
print(statement)

print("D. Standard input options and output options of print statements")
hardCodeVariable="irfan"#standard input by hardcoding
print("pass the input")
#passTheRunTimeInput=input()#standard input using runtime variables
passTheRunTimeInput="hello" #Standard input using runtime variables
def convertToUpper(a):#standard input using parameters/arguments
    localVariable="Good morning "+ a.upper()
    print(localVariable) #standard output
    return localVariable #standard output

functionOutput=convertToUpper(hardCodeVariable)
print(functionOutput.lower())

functionOutput=convertToUpper(passTheRunTimeInput)
print(functionOutput.lower())

#Standard output using print statement- 4 possible ways (ONE OF THEM IS IMPORTANT)
name="Inceptez Tech"
years=9
print(name +" have crossed " + str(years) + " years of journey") # way1 of producing the std output using print
print(name,"has crossed",years," of journey") # way2 of producing the std output using print
print(f"{name} has crossed {years} years of journey") # ****** important way3 of producing the std output using print using named notation
print("{0} has crossed {1} years of journey".format(name,years)) # way4 of producing the std output using print
print("Inceptez Tech is a training center") #not a right way

print("E. ****Important***** Variables and properties of variables in python")
a=100#declaring/defining a variable 'a' with the 'assignment' of the value '100'
#variable is nothing but a value/set of values/another variable/ a reference created in a named placeholder
#varible is a value assigned to a name (name of the variable, assignment operator, value of a type

#1. naming convention & assignment types
#initupper/snakecase/camelcase/with underscores
myFullName=""
MyFullName=""
my_name=""

#variables are case sensitive
a=10
A=100
print(a)
print(A)
#can't start with numbers but with _ we can start a variable name
#1a=1000
_a=1000
a1=1000
#varible can have alpha and numerics and special char of only _
a_b_1_2=100

#differnt way of assigning values to a variable
fname="mohamed"
lname="irfan"
fname,lname="mohamed","irfan"
name=fname
fullname=fname,lname #tuple is equivalent to strut in hive
fullname1=[fname,lname] #list is equivalent to array in hive

#2. Properties/Characteristics -
#Dynamic inference
print('Both Python and Scala will be dynamically inferencing the types')
a=100
type(a)
#<class 'int'>
af=100.1
ai=int(100.1)
#<class 'int'>
asa="irfan"
type(asa)
#<class 'str'>
aa=("mohamed","irfan")#declaring/defining a variable 'aa' with the 'assignment' of the complex value ("mohamed","irfan")
                      #using dynamic inference the type is identified as tuple using the notaion ()
aa1=["mohamed","irfan"]
aa2=set(list({"mohamed","irfan"}))
aa3={"mohamed":"irfan"}
#Dynamically Typed
print('H. Python is Dynamically typed where as Scala is Statically typed')
#Scala Statically typed: once the variable is assigned with a value of a type,
# the datatype become static, we can't change the type later
'''
scala program
var a=100
a: Int = 100

scala> a=200
a: Int = 200

scala> a="two hundred"
<console>:25: error: type mismatch;
 found   : String("two hundred")
 required: Int
       a="two hundred"
'''
#Python Dynamically typed: once the variable is assigned with a value of a type,
# the datatype become dynamic and we can change the type later
'''
a=100
type(a)
<class 'int'>
a="irfan"
type(a)
<class 'str'>
'''


#Strongly Typed
print('Python Strongly typed where as Scala is Weakly Typed')
'''
>>> a="100"
>>> b=50
>>> type(a)
<class 'str'>
>>> type(b)
<class 'int'>
>>> a+b
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: must be str, not int
>>> int(a)+b
150
>>> a+str(b)
'10050'
'''

#3. data types or typecasting
#variablename:datatype=datatype(value)
#a:int=int(100.1)

#Datatypes: simple & complex
username="irfan"
loginstatus=username=="irfan"
if loginstatus:
 print("user authenticated")

a=10
#a=20.2
isinstance(a,int)
#To understand the type of a variable we use type()
print(type(a))
#To check/evaluate the given variable is of an expected type isinstance(a,int)
isInt=isinstance(a,int)
print(isInt)
if isinstance(a,int):
    print(float(a))
else:
    print(a)
#To convert the type of a variable we use datatype()
print(type(float(a)))

#4. static/dynamic - immutable/mutable
#string, int, float, bool, tuple are immutable
str1='irfan'
#str1[0]='a' #doesn't work
print(str1)
#list, set, dictionary are mutable
lst1=['irfan']
lst1[0]='mohamed' #works
print(lst1)
#5. input (assigning value directly/passing arguments/using input function)/output (print/assigning/return)
#already covered in the top
#line #40 - print("D. Standard input options and output options of print statements")

#6. The respective type of variables supports respective functions - not all the functions can be recalled
# and not all functions can be used always, so don't try to memorize the functions merely
# rather try to know how to use the functions by passing required mandatory/optional argument,
# getting the right datatype of result
# how to use the functions for the given requirement by using the name of the function, input/output values & types,
# go through the description
# the functions in our entire learning is all about applying/creating/using/managing/composing the functions
num1=100
str(num1)
print(num1.bit_count())
print(num1.bit_length())

num1=100
#str(num1)
#num1.bit_count()
#print(num1.bit_count())
print(num1.bit_length())
str1="inceptez-Tech"
str2="Inceptez-teCh"
print(str1.count('inceptez',0,6))
print(str1.count('inceptez'))
lst1=str1.split("-")#row format delimited fields terminated by
str1.index("e")
str1.count('e', str1.index("e"), len(str1))
print(str1.capitalize())
print(str1.upper())
print(str1.lower())

if str1.casefold()==str2.casefold():
    print("both the strings are same")

str1="inceptez"
str1.find('e')
str1.join(str2)
str1.isalnum()
str1.isalpha()
#str1.isdecimal()
str1.isnumeric()
str1.replace('i','r')
str1.strip("inc")

flt1=100.1
print(flt1.is_integer())
print(flt1.hex())
print(flt1.as_integer_ratio())

#Operators/Operands (assignment, arithmetic, logical, comparision/relataional, membership, identity, bitwise)
#Symbols/notations used for managing the operators
#+,-,*,/,=,>,<,<=,!=,==,&/and,|/or,!/not,+=,++,%,:
#Types of operators
#1. Assignment Operator: = or += or -=
a=100
b=200
#2. Arithmetic Operator : +-/*%
print(a+a)
#3. Relational/Comparison Operators: >,<,>=,<=,==,!=
print(a>b)
#4. Logical Operators: and &, or |, not !
print(a>b or b>a)

#5. membership/member operator
#in, not in
name="Inceptez"
if 'i'.casefold() in name.casefold():
    print(f"i is present in {name}")

#6. Identical or identity operator
print("xyz" is "xyz")

#fundamental building construct of any programming languages including python is the control structures
#control structure
#1. conditional structure (if)
#membership, identity operators (least bother)
#membership operators -> in / not in

#presedence of using the operators
print((10+2)*2)
print(2>3 and 1==1 or 1<2)
print((1==1 or 1>2) and 3>2)
#find the greatest of 2 numbers
#if
a=100
b=200
if a==b:
    print("line1 of code or block of code")
    print("line2 of code or block of code")
#if else
if a!=b:
    print("if line1 of code or block of code")
    print("if line2 of code or block of code")
    print("if line3 of code or block of code")
else:
    print("else line4 of code or block of code")
    print("else line5 of code or block of code")
#if elif
a=10
b=20
if a>b:
    print("if line1 of code or block of code")
    print("if line2 of code or block of code")
    print("if line3 of code or block of code")
elif a==b:
    print("elif line4 of code or block of code")
else:
    print("if all of the above if is false, then control will come to else")

#if elif else
a=10
b=10

#more optimistic is "if elif else"
if a>b:
    print("a is greater")
elif a<b:
    print("b is greater")
else:
    print("both are equal")

if a>b:
    print("a is greater")
elif a<b:
    print("b is greater")
elif a==b:
    print("both are equal")


if a>b:
    print("a is greater")
if a<b:
    print("b is greater")
if a==b:
    print("both are equal")

#if -> if elif ->if elif -> else
print("consider 3 factors when write the code - functionality, performance/cost, readability")
print("find the greatest of 3 numbers, and apply the a-max amount, b-600 discount, c-500max discount")
'''
get 3 inputs
check for the greater within the 3 inputs by using ab,ac,bc - a=10,b=20,c=30 -> 30 is greater
check for the equals within the 3 inputs by using ab,ac,bc - a=20,b=10,c=20 -> 20 is greater with a and b
else case all are equal
'''
a,b,c=input("Enter three numbers using , delimiter: ").split(",")
a,b,c=int(a),int(b),int(c)
if a>b:
    if a>c:
        print(f'a max is {a}')
    elif a<c:
        #print("we are here")
        print(f'c are max {c}')
    else:
        print(f'a and c are max is {c}')
elif b>a:
    if b>c:
        print(f'b max is {b}')
    elif b<c:
        #print("we are here")
        print(f'c is max {c}')
    else:
        print("we are here")
        print(f'b and c are maximum {c}')
else:
    if b>c:
        print(f'a and b are max is {b}')
    elif b<c:
        print(f'c max is {c}')
    else:
        print('a,b,c All numbers are equal')

# find greater of 3 numbers
print("enter values for a,b,c: ")
a = int(input())
b = int(input())
c = int(input())
print("values entered for a,b,c are: ", a, " ", b, " ", c)

if a > b and a > c:
    print(f"a is greater than b and c {a}")
elif b > a and b > c:
    print(f"b is greater than a and c {b}")
elif c > b and c > a:
    print(f"c is greater than a and b {c}")
elif a==b and (b!=c and a!=c):
    print(f"a and b are equal {a} {b}")
elif b==c and (c!=a and b!=a):
    print (f"b and c are equal {b} {c}")
elif a==c and (a!=b and c!=b):
    print(f"a and c are equal {a} {c}")
else:
    print("a b c are equal")

print("consider 3 factors when write the code - functionality, performance/cost, readability")
#find greater of 3 numbers
print ("enter values for a,b,c: ")
a,b,c=int(input()),int(input()),int(input())
print ("values entered for a,b,c are: ", a," ",b," ",c)

if a>b and a>c:
    print("a is greater than b and c")
elif b>a and b>c:
    print("b is greater than a and c")
elif c>b and c >a:
    print("c is greater than a and b")
elif a==b==c:
    print("a b c are equal")
elif a==b:
    print(f"a and b are equal {a}")
elif a==c:
    print(f"a and c are equal {a}")
else:
    print(f"b and c are equal {b}")

#Nested conditional structure

while True:
    print("please enter state")
    state=input()
    print("please enter city")
    city=input()
    print("please enter area")
    area=input()

    if state=="TN":
        if city is "Chennai":
            print(f"{city} belongs to state of {state} and state tax is 18% , city prof tax is 4%")
            if area in ["Guindy","Ambathur"]:
                print(f"{city} belongs to state of {state} and for the area {area} susidised property tax is 2% of the overall value")
            else:
                print(f"{city} belongs to state of {state} and for the area {area} property tax is 4% of the overall value")
        else:
            print(f"{city} belongs to state of {state} and state tax is 17% , city prof tax is 3%")
    elif state=="KE":
        if city=="Kollam":
            print(f"{city} belongs to state of {state} and state tax is 16% , city prof tax is 5%")


#2. looping Constructs
#There are 2 types of loops - conditional (while loop) and un conditional (for loop)
#If w have to perform iteration or repetitive operation on a collection of values or untill a condition is met or unconditionally
#emp=[1,'irfan',10000]
#calculate the bonus on the given salary in table a and store the bonus applied salary in another collection variable in table b
sal=[10000,20000,30000]
bonuspercent=10
bonussal=[]
#bonussal1=10000+(10000*(bonuspercent/100))
#print(bonussal1)
#bonussal2=20000+(20000*(bonuspercent/100))
#print(bonussal2)
for i in sal:#un conditional
    if i>10000:#conditional
        bonussal1=i+(i*(bonuspercent/100))
        print(f" bonus applied sal is {bonussal1}")
        bonussal.append(bonussal1)
print(bonussal)

maxIndexOfSalList=len(sal)
startingIndex=0
rangeofvalues=range(startingIndex,maxIndexOfSalList)

'''for i in rangeofvalues:#for loop obviously requires a iterble type to iterate, otherwise it won't work
    onesal=sal[i]
    if onesal>10000:#conditional
        bonussal1=onesal+(onesal*(bonuspercent/100))
        print(f" bonus applied sal is {bonussal1}")
        bonussal.append(bonussal1)
print(bonussal)
'''

#looping constructs comprised of 2 additional constructs such as break and continue
statelst=["TN","KE","GO","KA","PO"]
for i in statelst:
    if (i in ["GO","PO"]):
        print(f"state tax is NOT applicable for these states {i}")
        print(f"don't run 100 lines of code below to calculate the tax for GO and PO")
        continue#continue is important construct in looping, help to control the execution of the loop further
        print("continue will continue the next iteration without continue the further lines of code")
    print(f"state tax is applicable for these states {i}")
    print(f"executing the below 100 lines of code writtern further")

#Purpose of using Break construct in a program
print("break will break/terminate the loop if applied using some conditions")
for i in statelst:#exists in sql
 if i in ["GO","PO"]:
  print("minumum one union territory is present")
  break

for i in statelst:#in in sql
 if i in ["GO","PO"]:
  print(f"union territory is present {i}")
  #break

sal=[10000,30000,20000,15000,5000,50000]
bonuspercent=10
bonussal=[]
#below code will run on all the given salary
for i in sal:#un conditional
    print(f"salary taken is {i}")
    if i>20000:#conditional
        bonussal1=i+(i*(bonuspercent/100))
        print(f" bonus applied sal is {bonussal1}")
        bonussal.append(bonussal1)
print(bonussal)

#below code will have lesser iteration, since it will run only upto the condition is met >20000 and break out after the condition is not met <=20000
#relate with the hive bucketing and sorting example or spark or mapreduce
sal=[10000,30000,20000,15000,5000,50000]
sal=sorted(sal,reverse=True)
bonuspercent=10
bonussal=[]
for i in sal:#un conditional
    print(f"salary taken is {i}")
    if i>20000:#conditional
        bonussal1=i+(i*(bonuspercent/100))
        print(f" bonus applied sal is {bonussal1}")
        bonussal.append(bonussal1)
    if i<=20000:
        break

print(bonussal)

#lst2=map(lambda i:i+(i*(bonuspercent/100)),sal)
#print(f" bonus applied sal is {list(lst2)}")


#while loop - equally lesser important and used only if for loop can't be used
#challenges in while loop which makes it un conventional
#while loop require a condition to execute, if the condition is not going to meet at times, then infinite loop will be runing
#while loop requires multiple variables that has to be mananged using the code (not easy)
#real life scenario of using while loop?
#scheduler we crated with infinite while loop (do while loop - doesn't available in python)
#below is a normal while loop, which do a entry control
maxAttempts=3
attempt=1
systempwd='hduser'
sudouser='irfan1'
while attempt<=maxAttempts and sudouser=='irfan':#entry controlled loop
    userpwd=input("please enter the password")
    print(f"attempt number {attempt}")
    if (systempwd==userpwd):
        print("You are authenticated to execute the command or enter the bank site")
        break
    attempt=attempt+1

#Exit control looping concept - do while loop
maxAttempts=3
attempt=1
systempwd='hduser'
sudouser='irfan1'
while True:#entry controlled loop
    userpwd=input("please enter the password")
    usersudopwd=input("please enter the sudo password")
    print(f"attempt number {attempt}")
    if (sudouser==usersudopwd):
        print("you can use sudo command")
        if(systempwd==userpwd):
            print("do the createuser operation")
            break
    else:
        print("you can't use sudo command,incident will be reported")
        break
    if(attempt>maxAttempts):
        break
    else:
        attempt = attempt + 1


#Exit control looping concept - do while loop
maxAttempts=3
attempt=1
systempwd='hduser'
sudouser='irfan1'
while True:#entry controlled loop
    userpwd=input("please enter the password")
    usersudopwd=input("please enter the sudo password")
    print(f"attempt number {attempt}")
    if (sudouser==usersudopwd):
        print("you can use sudo command")
        if(systempwd==userpwd):
            print("do the createuser operation")
            break
    else:
        print("you can't use sudo command,incident will be reported")
        break
    if(attempt>=maxAttempts):
        break
    else:
        attempt = attempt + 1


#Complex data types or Collection Types:
import json
openfile=open("/home/hduser/sparkdata/file2.json",'r')
readjsonfile=json.load(openfile)
query=readjsonfile['select']+' where'+readjsonfile['where']
sources=readjsonfile["source"]
type(sources)
for i in sources:
 print(f"running the query {query} on the db of {i}")


#What is collection type?
#the organized/indexed-sequenced/named/iterable collection of homogeneous/hetrogeneous types of
# elements/items/keys/values which can support iterations
#if we have more than one dimension of value stored in a variable we call it as collection.
#Refer the pdf document for more details... from page 18 onwards..
#Why we need collection types?
#To manage complex dataset in a hirarchical structure stored, to process semi structure data, nested data, dynamic schemaful data..
#different notations we use?
# list[],dict/set{},tuple()
#different types of collection types?
#sequence/iterable (str,tuple,list...), mapping type(dictionary)
#category of collection types
#mutable/immutable

#functions applied?
#we are going to see in detail

#Collection Types: complex data types
#iterable or non-iterable - mutable/immutable

#python-hive
#list - Array (duplicated list)
#Dictionary - Map
#Tuple - Struct
#set - Array (de duplicated list)

print("List type in python (PRIORITY1)")
#Notation: []
#Accessed using ? index starting from 0 ending with len(list)-1 or just put -1
#Definition
#indexed-sequence collection of homogeneous elements/attributes/items/values/object
# it can be hetrogeneous too (but not suggested, because python is a stongly typed language)

print("list operations")
#select/access
#accessed using the index, loops, functions like map
lst=[1,2,10,100,200,40]
#map(lambda x:x,lst)
firstelement=lst[0]
for i in lst:
    print(i)

#sorting the list
sorted(lst)
sorted(lst,reverse=True)

#insert/update/delete
#append in the last
lst.append(1000)
#insert in the index position
lst.insert(3,2000)
lst2=[3000,4000]
lst.extend(lst2)

#update the list elements (mutable)
lst[3]=lst[3]+100

#delete the elements of the list using value
lst.remove(1000)
#delete the elements of the list using index
lst.pop(1)
#search for a value with in the given index range
lst.index(3000,0,5)
lst.remove(lst[lst.index(3000,0,5)])
pop(lst.index(3000,0,5))

#certain builtin functions available
sorted(lst)
max(lst)
len(lst)
tuple(lst)
#all the below are applying functions on a value/collection
len(lst) #common function is created commonly inside the builtins module, can be applied accross most of the types
lst.index()#class function(method) -> if the index function (denoting as method) is created inside the class of list (only applied for the list only) then we will be using list.index()
lst.__len__()#implicit functions may refer/use/call the common function len() defined inside the module
sum(lst)
tuple(lst)

print("2. Dictionary type in python")
#Notation: {}
#Access using: key
#Definition: Collection of key value pairs
#thumb rules:
#keys preferably of same type and value preferably be of same type
#keys should be unique, values can be non unique
#Json eg. {"name":"inceptez","age":9}
#Dict eg. {1:10000,2:20000,3:15000}
#{"irfan":10000,"bala":20000,"raj":15000}

print("dictionary operations")
# select/access pass the key and get the values
dict1 = {"irfan": 10000, "bala": 20000, "raj": 15000,"yamini":15500}
print(dict1["irfan"])  # to access the value of a key, pass the key (if the key is not present, keyerror exception will occur)
dict1.get("irfan") #to access the value of a key, pass the key in the get function (if the key is not present, no error will occur)
# to access all the keys
all_keys = dict1.keys()
print(list(all_keys))

all_upper_keys=[]
for i in dict1:
    all_upper_keys.append(i.upper())
print(all_upper_keys)

# to access all the values
dict1 = {"irfan": 10000, "bala": 20000, "raj": 15000,"yamini":15500}
print(list(dict1.values()))
#take only the round salary of the employees
all_round_values=[]
all_non_round_values=[]
for i in dict1.values():
    if i%1000==0:
        all_round_values.append(i)
    else:
        all_non_round_values.append(i)

print(all_round_values)
print(all_non_round_values)

# to access all the elements
dict1.items()
for i in dict1.items():
    print(i)

print(" append/insert into dictionary")
#update function will help us to insert else update using the key (merge or upsert operation)
dict1 = {"irfan": 10000, "bala": 20000, "raj": 15000,"yamini":15500}
print(dict1)
dict1.update({"piyush": 25000})#if the key is not present, insert
print(dict1)
# update the elements
dict1.update({"irfan": 12000})#if the key is present, update
print(dict1)
# delete the elements
dict1.pop("bala")#remove the item using the given key
print(dict1)
dict1.popitem()#remove the last item
print(dict1)

dict1.pop(list(dict1.keys())[0])#remove the first item
dict1.clear()
print(dict1)
dict1 = {"irfan": 10000, "bala": 20000, "raj": 15000,"yamini":15500}
print(dict1)
#other functions
dict3={"bagavathi",30000}
dict2={"bagavathi",20000}
dict3=dict2.copy()
dict1 = {"bala": 20000, "raj": 15000,"yamini":15500}
dict1.setdefault("irfan",50000)#insert irfan key with default value of 50000 if irfan not present, if present dont do anythingdict1.setdefault("irfan",50000)
dict1.update({"irfan":60000})#insert irfan key with value of 60000 if irfan not present, if irfan present then update value of irfan to 60000
dict1.fromkeys("irfan",10000)#help us convert the given pair of mere values to collection of key,value pairs

#eg: convert the given list of tuples (table of 2 columns and 3 rows) into dictionary
name_sal=[("irfan",10000),("karthik",20000),("aravind",30000)]#list of tuples
#name_sal=sc.textFile("file:///home/hduser/sparkdata/sampledata.csv").map(lambda x:x.split(",")).map(lambda x:(x[1],int(x[2]))).collect()
dict1={}
dict2=dict({})
for i in name_sal:
    dict2.update(dict1.fromkeys([i[0]], i[1]))

print(dict2)

print("3. Tuple type in python")
#Notation: ()
#Access using: index
#Definition: Indexed - sequenced Collection of hetrogeneous elements
#example : (1,'irfan',41)
#thumb rules:
#can have any type of elements
#tuples are immutable
tup=tuple((10000,20000,30000,20000,15000))
tup.__len__()
tup.index(20000,0,2)
tup.count(20000)
tup.index(20000,2,4)
print("tuple operations")
#select/access
x=tup[0] #using the index
print(x)
for i in tup:
    print(i)
tup=("a","b","c")
tup.__sizeof__()
#append
tup=("a","b","c")


#If we want to perform all list/set operations on a tuple, which is not directly supported, can be achieved using the conversion
tup=list(tup)
#same way we can achieve the rest of activities also
#insert in the index position
#update the  elements
#delete the elements using value
#delete the elements using index
tup=tuple(tup)

print("Set operations")
#Notation: {values}
#Access using: only iteration
#Definition: iterable collection of (deduplicated) elements which are un-ordered/without index
#example : {1,3,2}
#thumb rules: deduplicated, un-ordered, no index support
#set is iterable
#can have any type of elements (possibly go with same type)
#set is mutable
#set is mainly used for performing set operations (union/intersection/difference,adjoint,disjoint,symetricdifference)
st={10,30,20,10,40,25}
#select/access
for i in st:
    print(i)
#set will not support index
#st[0]
st=list(st)
print(st[0])
st=set(st)

#insert
st={10, 50, 20, 25, 60, 30}
print(st)
lst=[100,200,300]
st.add(100)#add an element
print(st)
st.update(lst)#add another iterble type with the elements not present in the set (do upsert/merge)
print(st)
#update the  elements
#can't, only insert new elements

#delete the elements using value
st.pop()
st.remove(100)#error occurs if no element present
st.discard(200)#no error occurs if no element present
#st.clear()

st={100,200,300,400,500}
st1={100,300,500,600}
st.union(st1)
st.difference(st1)#a-b and display remaining elements from a
st.difference_update(st1)#set a will be updated with the result of 'a-b and display remaining elements from a'
st={100,200,300,400,500}
st1={100,300,500,600}
st.symmetric_difference(st1)#a-b and display remaining elements from a and from b also ((a-b)+(b-a))
st.symmetric_difference(st1)#a-b and display remaining elements from a and from b also ((a-b)+(b-a))
st.symmetric_difference_update(st1)#set a will be updated with the result of 'a-b and display remaining elements from a and from b also ((a-b)+(b-a))
st={100,200,300,400,500}
st1={100,300,500,600}
st.intersection(st1)
st.intersection_update(st1)
st={100,200}
st1={100}
st.issuperset(st1)#if st has more than the related elements than st1
st.issubset(st1)#if st has less than the related elements than st1
st={50,300,600}
st1={100,200,200,400}
st.isdisjoint(st1)#If both set's doesn't have any commonality (opposite of intersection)

#type - tuple(int,string,list(dict(k:str,v:string/dict())))
complextup=(1,
            "Madison's Family",
            [{"gender":"male","Relation":"self","personalinfo":{"title":"Ms","name":"Madison"}},
             {"gender":"female","Relation":"spouse","personalinfo":{"title":"Mrs","name":"Elisa"}},
             {"gender":"female","Relation":"daughter","personalinfo":{"title":"Miss","name":"Hanna","hobby":"book reading"}},
             {"gender":"male","Relation":"son","personalinfo":{"title":"Master","name":"Dave","schooling":True}}])

print("id is ")
print("Family name is " )
print("gender of first list element is " )
print("relation of first list element is ")
print("personalinfo of the first list element is ")
print("personalinfo title of the first list element is " )
print("personalinfo name of the first list element is " )
print("personalinfo name of the second list element is " + complextup[2][0]['personalinfo']['name'])
print("personalinfo hobby of the third list element is " )
print("personalinfo schooling info of the fourth list element is ")

