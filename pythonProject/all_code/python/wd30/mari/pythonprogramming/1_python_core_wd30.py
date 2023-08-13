print("************** 1. Basics of Python Programing*************")

print("A. Python is an indent based programming language")
# Indentation can be using tab or spaces (usually 4 spaces),
# the levels of indent help to write blocks of code rather than using braces
a=10
if a>99:
    print("a is greater than 99")

#indent_func(100)
print("B. This is a commented line in Python")
# this is a commented line in Python
'''This is a
     multiline
     comment.'''

#Python treats single quotes the same as double quotes.
print("C. Python treats single quotes the same as double quotes.")
x='Inceptez'
y="'Technologies '"
z="Pvt.\nLtd."
print("welcome " + x + y+z)
greet="Welcome to " \
      "Python"
greet1="""Welcome to 
Pyspark"""
sql="""select * from customer
where city='chennai'"""
greet1='''Welcome to 
Pyspark'''
greet1
print(greet1)

print("D. Standard output options of print statements")
var1=" Years"
var2=" Inceptez "
totalsal,empid=10000+2000, 10

print("Total sal is ", totalsal, "for the emp id ", empid); #considered as different prints
print("Total sal is " + str(totalsal) + " for the emp id " + str(empid)) #convert to string to concatenate
#below 2 are the standards we follow
print("Total sal is {0} for the emp id {1}".format(totalsal,empid) ); #add the formatted positional variables/arguments in the respective placeholder placeholder
print(f" Total sal is {totalsal}  for the emp id {empid}") #add the named variables/arguments in the direct placeholder

print(greet)
print(greet1)

print("E. Data Types ")
print("F. Dynamic inference")
flt=10.2345
a,b,c,d,e=10,20,30,40,50
print(a)
print(b)
print("The type of variable having value" , flt, " is ", type(flt))
#print("The type of variable having value" + flt + " is " + type(flt)) this wont work in python (strongly typed)
# scala -> println("The type of variable having value" + flt + " is " + flt.getClass)
isins=isinstance(flt,float)
print(isins)

#strings are immutable
tmpstr = "Test"
a=10

#tmpstr[2]='r'
print ("strings are immutable (updation is not possible) " + tmpstr)
tmpstr="hello"
print ("strings are immutable (re assignment is possible)" + tmpstr)

print("G. String & Num operations using Built in functions")
strdata = "this IS test data"
#strdata.split()
#strdata=100
print("split into list for performing update, as string is immutable",strdata.split(" "))
print("Convert to Upper Case:",strdata.upper())
print("Check upper:",strdata.isupper())
print("Convert to lower Case:",strdata.lower())
print("Check lower:",strdata.islower())
print("Check numeric:",strdata.isnumeric())
print("Check float is a number or not: ",flt.is_integer())
print("get no of occurances:",strdata.count("i"))
print("Starts with :",strdata.startswith("t"))
print("Ends with :",strdata.endswith("a"))
flt="10.1"
print(flt.upper()) #possible
flt=10.1
print("Check float is a number or not: ",flt.is_integer())
#print(flt.upper()) #not possible
#int1.is_integer() #not possible

print(f"Length of the sequence {len(strdata)}" ) # not str conversion is needed
print("Length of the sequence " , len(strdata))
print("Length of the sequence {}".format(len(strdata)))
#len() and string.__len__()
print("Length of the sequence " + str(len(strdata)))
print("Length of the sequence " + str(strdata.__len__()))

'''The Python len() function can be interpreted as:
def len(s):
    return s.__len__()'''
#len() function returns the length of the object. This function internally calls __len__() function of the object.
# __len__() is what actually happening behind the scenes to calculate the length

#Variables & its properties
num1=100
num2=200
print(num1+num2)
#interpreter language
num1=100
num2="Two hundred"
#print(num1+num2)

print('H. Python is Dynamically typed where as Scala is Statically typed')
#Dynamically typed: If a variable is created with a specific data type, can be changed later
num1=100
num2=200
print(type(num1))
print(num1)
print(isinstance(num1,int))
num3=num1 + num2
num1="Inceptez" #dynamically typed
print(type(num1))
print(num1)

print('Python Strongly typed where as Scala is Weakly Typed')
#Strongly typed: Python allow us to operate between the variables of same datatype and doesn't allow to operate between different datatypes.
#num3=num1 + num2 #strongly typed

print('Both Python and Scala will be dynamically inferencing the types')
#Dynamic inference - based on the assigned value it will automatically decides/infer the type
#var x=100
xyz=100

#Static defined - by assigning a variable with a type and cast the value as per the type
tn:float=float(30)
type(tn)
#<class 'float'>

print("************** 2. Conditional Structures ************** ")
print("greatest of 3 numbers")
print("enter the input1")
a=int(input())
b=200
c=200

if (a == b == c):
    print("all are equal")
    print("one more print all are equal")
    if a > 100:
        print("a is greater than 100")
elif a > b or a > c:
    print("a is greater..a value is %d " % a)
elif b > a and b > c:
    print("b is greater")
else:
    print("c is greater")

print("************** 3. Looping Constructs ************** for/while/do-while/break/continue")
#loops in python

lst = [5,4,2,3,1]

for i in reversed(lst) : # We dont know the conditions upfront
    print(i)

# for ( i <- lst.reverse ) {println(i)}

#Nested Loops - A nested loop is a loop inside a loop.
colors = ["red", "big", "tasty"]
fruits = ["apple", "banana", "orange"]

for x in colors:
    for y in fruits:
        print(x, y)

recs3_cols2=[({1,2},'a'),({3,4},'b'),({4,5},'c')]
for i in recs3_cols2: #i holds ({1,2},'a') in iter1
 for j in i: #j holds {1,2} in iter1, then j will iterate on a
  for k in j: #k iterated on 1 and 2 then k will iterate on a
    print(k)


''' for (x <- colors)  #scala equvalent
        { 
         for (y <- fruits) 
             {
             println(x,y)
             }
        } '''

'''for x in colors, y in fruits: # not do able in python as like scala
        print(x,y)'''

# for (x <- colors, y <- fruits) {println(x,y)} #scala

# while loop . We know the conditions upfront
count = 0
while (count < 3):
    count = count + 1
    print("Hello Inceptez - I run with all iteration of while loop")
print("Loop completed (i run only once)")

#Using else statement with while loops: while loop executes the block until a condition is satisfied.
# When the condition becomes false, the statement immediately after the loop is executed.

# combining else with while
count = 5
while (count > 3):
    count = count - 1;
    print("Inside while loop with count ",format(count));
else:
    print("In Else Block, do something when the while loop completed")

#Single statement while block (purpose of semicolon)
count = 2
while (count == 2): count = count - 1; print("Hello1 Inceptez"); print("Hello2 Inceptez2")

while (count == 2):
 count = count - 1; print("Hello1 Inceptez"); print("Hello2 Inceptez2")

# With the break statement we can stop the loop even if the while condition is true:
# Do While is not available in Python and below code is the equivalent
# If we wanted to derive the value of i inside the while block
i = 1
while True: #infinate loop (scheduling), Do-While
    print(i)
    if i <= 2:
        break
    i -= 1
print("Do while example loop with break completed here")

#With the continue statement we can stop the current iteration, and continue with the next:
i = 0
while i < 8:
    #increment operators are not allowed in python i++ or i-- instead we can use i += 1
    i += 1
    if i == 5 or i == 6 : #Dont consider these values in the looping
        print("inside the if condition "+str(i))
        continue
        #break
    print(i) #dont print 5 alone or dont give bonus for employee 5 alone
print("loop with continue completed here")

print("************** 4. Collections ************** ")
# Scala Collections - Seq-Array/List, Tuple, Map, Set(mutable/immutable)
# Python Collections - List, Tuple, Dictionary, Set/frozenset
print("***** Mutability and immutability - Both Python and Scala are similar *****")
print("List (mutable) - []")
print("List Operations")
lst2 = ['a','e','x','o','u']
#The remove() method removes the first matching element (which is passed as an argument) from the list.
lst2.remove('x')
print(lst2)
lst2.insert(2,'i')
print(lst2)
#simply update since mutable rather than seperately remove and then insert
lst2[2]='I'
print(lst2)
lst2.reverse()
print(lst2)
lst2.clear()
print(lst2)
#lst2.__add__('x')
weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
lst = [5,4,2,3,1]
print(lst)
print("slicing a list")
print(lst[1:3])
print("Append lists")
lst=lst.__add__(weekdays)
'''for i in lst:
    print(i+10)'''

print(lst.count(5))
print(len(lst))
print(lst)
#single column - list
#single row - tuple
#all columns all rows - list(tuples)

print("Tuples (immutable) - () - similar to Scala")
#Syntactically python and scala are same
#semantically - tuples in scala is not iterable and the position of elements starts from 1, access using _position
#semantically - tuples in python is iterable and the position of elements starts from 0, access using [0]

tup = ('all your mark', 'set', 'go')
print("Tuple looping is possible in python and it proves tuple is added in sequence hierarchy in python")
for i in tup: #not possible in scala, but possible in python and it proves tuple is added in sequence hierarchy in python
    print(i);

print('tuple starts with zero index ' + tup[0])
print(len(tup))
print("tuple doesn't support any add/update/delete operation")
#tup[0]='get ready' #object does not support item assignment
print(tup)
print('Workaround is convert to list and do the operation')
tuplst=list(tup)
tuplst.remove('set')
print(tuplst)
tuplst.insert(1,'get set')
print(tuplst)
#simply update
tuplst[1]='get set1'
print(tuplst)
tup=tuple(tuplst)
print(tup)

print("Set (mutable) - {} ")
#by default sorts and remove duplicates
my_set = {5,1, 3, 1}
#print(my_set[1])
print(my_set)
print("set converted to list")
lst_my_set=list(my_set)
print(lst_my_set)
print(my_set.__len__());
print(my_set.intersection(my_set))
my_set.remove(3)
# remove & discard does the same thing. removes the element.
# difference is discard doesn't raise error while remove raise error if element doesn't exist in set
#my_set.discard(7)
print(my_set)
my_set.add(6)
print(my_set)
my_set2={2,3,4}
#my_set[3].update(2) #Set can't be updated with an element, rather we can update a set with another set
my_set.update(my_set2)
print(my_set)

print("Dictionaries (mutable) - {k:v, k:v}")
# When to go for tuple and when with dictionary - its equivalent to the tradeoff between csv and json / sql(mysql) vs nosql(hbase/es)
# Tuple - index/Positional access, storing fixed number of unbounded elements (type and the name are not bounded),
# eg. fixed fields records
# dictionary - key based access, variable number of bounded elements (type and name are bounded) eg. dynamic fields records
print("Dictionaries in python is Map in scala")
my_tup=('inceptez','tech',6)
my_dict = {'first_name': 'inceptez', 'last_name': 'tech','age': 6}
print(my_dict)
print(my_dict["last_name"]) #How dictionary name has come
print(my_dict['first_name'] + ' ' + my_dict['last_name'])
#Adding Items
my_dict["city"] = "chennai"
print(my_dict)
#Updating Items
my_dict["last_name"] = "technologies"
my_dict.update({"country":"India","city":"Hyderabad"})
print(my_dict)
#Removing an Item
# The __delitem__ delete value and dont returns anything
print(my_dict.__delitem__("age"))
print(my_dict)
#pop() delete value and returns deleted value.
print(my_dict.pop("city"))
print(my_dict)
print(my_dict.keys());

my_dict = {'first_name': 'inceptez', 'last_name': 'tech','age': 6}
print("looping dictionary")
print("Printing keys")
for i in my_dict.keys():
    print(i)
print("Printing values")
for i in my_dict.values():
    print(i)

print("converting my_dict to items type to access list of tuples for looping")
print(my_dict.items()) #items return as List(Tuples)

print("Printing keys and values")
for key,val in my_dict.items():
 print('keys','values')
 print(key,val)

print("nested list")
nestlst = ["some string", [1,2,3], ['x']]
#eg data - ["custid1",[page1,page2,page3]]
#0,explode(1)
# custid1,page1
# custid1,page2
# custid1,page3

nestlst1 = nestlst[1][2]
print(nestlst1)

print("Complex tuple1 - tuple(list,dictionary,tuple)")
print("Hive equivalent - struct(array<int>,map<a:string,b:String>,struct)")
tup11 = (['SWE','SSE'], {'Name': 'Irfan','Age':40}, (1,10000,'Chennai'))
tup12 = (['Trainee','SWE','Analyst'], {'Name': 'Mydeen','Age':30,'Hometown':'Madurai'}, (2,15000,'Chennai'))
print(tup11[0][1])#SSE
print(tup11[1]['Name'])#Irfan
print(tup11[2][2])#Chennai

#{"results":
# [{"gender":"male","name":{"title":"Mr","first":"Lucas","last":"Gallardo"},"location":{"street":{"number":49,"name":"Ronda de Toledo"},"city":"Bilbao","state":"Navarra","country":"Spain","postcode":72759,"coordinates":{"latitude":"-24.1241","longitude":"-42.9993"},"timezone":{"offset":"+5:45","description":"Kathmandu"}},"email":"lucas.gallardo@example.com","login":{"uuid":"a4c0f1db-35ef-43b9-9d23-b1da970f4876","username":"blackduck720","password":"spank","salt":"ov40nTLe","md5":"2d2a37e544c2423dabd30e45b04d01de","sha1":"2c6398b2cd82becb356a624b661978c9c0ae699c","sha256":"e26278657a9c3d9b17572dd14c75e402deebef7ff25600981c923a8e0b307759"},"dob":{"date":"1951-01-05T20:37:44.406Z","age":71},"registered":{"date":"2016-09-08T07:29:59.842Z","age":6},"phone":"954-092-544","cell":"671-763-895","id":{"name":"DNI","value":"97111064-H"},"picture":{"large":"https://randomuser.me/api/portraits/men/65.jpg","medium":"https://randomuser.me/api/portraits/med/men/65.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/men/65.jpg"},"nat":"ES"}
# ,{"gender":"female","name":{"title":"Mrs","first":"Elisa","last":"Gallardo"},"location":{"street":{"number":49,"name":"Ronda de Toledo"},"city":"Bilbao","state":"Navarra","country":"Spain","postcode":72759,"coordinates":{"latitude":"-24.1241","longitude":"-42.9993"},"timezone":{"offset":"+5:45","description":"Kathmandu"}},"email":"lucas.gallardo@example.com","login":{"uuid":"a4c0f1db-35ef-43b9-9d23-b1da970f4876","username":"blackduck720","password":"spank","salt":"ov40nTLe","md5":"2d2a37e544c2423dabd30e45b04d01de","sha1":"2c6398b2cd82becb356a624b661978c9c0ae699c","sha256":"e26278657a9c3d9b17572dd14c75e402deebef7ff25600981c923a8e0b307759"},"dob":{"date":"1951-01-05T20:37:44.406Z","age":71},"registered":{"date":"2016-09-08T07:29:59.842Z","age":6},"phone":"954-092-544","cell":"671-763-895","id":{"name":"DNI","value":"97111064-H"},"picture":{"large":"https://randomuser.me/api/portraits/men/65.jpg","medium":"https://randomuser.me/api/portraits/med/men/65.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/men/65.jpg"},"nat":"ES"}]
# ,"info":{"seed":"1934b3808d5cc6aa","results":1,"page":1,"version":"1.3"}}

print("Complex Tuple2 - tuple(familyid:int,familyname:str,familymembers:list[dict{k:v,k:v,k:dict{k:v,k:v}}])")
#complextup: Tuple[int, str, List[Union[Dict[str, Union[str, Dict[str, str]]], Dict[str, Union[str, Dict[str, str]]], Dict[str, Union[str, Dict[str, str]]], Dict[str, Union[str, Dict[str, Union[str, bool]]]]]]] = (1,...
complextup=(1,
            "Madison's Family",
            [{"gender":"male","Relation":"self","personalinfo":{"title":"Ms","name":"Madison"}},
             {"gender":"female","Relation":"spouse","personalinfo":{"title":"Mrs","name":"Elisa"}},
             {"gender":"female","Relation":"daughter","personalinfo":{"title":"Miss","name":"Hanna","hobby":"book reading"}},
             {"gender":"male","Relation":"son","personalinfo":{"title":"Master","name":"Dave","schooling":True}}])

print("id is "+str(complextup[0]))
print("Family name is " + str(complextup[1]))
print("gender of first list element is " + complextup[2][0]["gender"])
print("relation of first list element is " + str(complextup[2][0]["Relation"]))
print("personalinfo of the first list element is " + str(complextup[2][0]["personalinfo"]))
print("personalinfo title of the first list element is " + str(complextup[2][0]["personalinfo"]["title"]))
print("personalinfo name of the first list element is " + str(complextup[2][0]["personalinfo"]["name"]))
print("personalinfo name of the second list element is " + str(complextup[2][1]["personalinfo"]["name"]))
print("personalinfo hobby of the third list element is " + str(complextup[2][2]["personalinfo"]["hobby"]))
print("personalinfo schooling info of the fourth list element is " + str(complextup[2][3]["personalinfo"]["schooling"]))