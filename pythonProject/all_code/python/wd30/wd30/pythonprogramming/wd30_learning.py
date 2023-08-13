a=b=c=1
print(a,b,c)
a,b,c=1,2,3
print(a,b,c)
a=1;b=2;c=3
print(a,b,c)
a=1
b=2
c=3
print(a,b,c)

#python is an intend based prog
#comments in python
#variables in python with double/single/triple double/triple single
#standart output options using print...

#properties of variables and types
#dynamic inference
x=100
print(type(x))
y=10.1
print(type(y))
z=float(10)
print(type(z))
#static definition
zz:float=float(10)
print(type(zz))

#Strongly Typed
print(x+z)
#Statically Typed

#simple data types:

y=100.1
z="irfan"
a=True


#control structure
#conditional structure
#if
#if else
#if elif
#if elif else
#if if ..
#if else if

print("provide some number input")
#valuetocheck=int(input())
valuetocheck=1;value2tocheck=1;
if (valuetocheck>0):
    print("inside the first if block");
    print(f"input value is {valuetocheck}");
print(f"any cost, I am going to run outside the body of If");
#if else
valuetocheck=0
#valuetocheck=-1
if (valuetocheck>0):
    print("inside the second if block");
    print(f"input value is {valuetocheck}");
else:
    print("inside the second else block");
    print(f"input value is {valuetocheck}");
print(f"any cost, I am going to run outside the body of If or else");

#if elif
valuetocheck=0
#valuetocheck=-1
if (valuetocheck>0):
    print("inside the third if block1");
    print(f"input value is {valuetocheck}");
elif(valuetocheck==0):
    print("inside the third elif block2");
    print(f"input value is {valuetocheck}");
elif(valuetocheck==-1):
    print("inside the third elif block3");
    print(f"input value is {valuetocheck}");
print(f"any cost, I am going to run outside the body of If or else");
#if elif else
valuetocheck=-1
#valuetocheck=-1
if (valuetocheck>0):
    print("inside the fourth if block1");
    print(f"input value is {valuetocheck}");
elif(valuetocheck==0):
    print("inside the fourth elif block2");
    print(f"input value is {valuetocheck}");
elif(valuetocheck==-1):
    print("inside the fourth elif block3");
    print(f"input value is {valuetocheck}");
else:
    print("inside the fourth else block");
    print(f"input value is {valuetocheck}");
print(f"any cost, I am going to run outside the body of If or else");
print("\n\n")

print("Nested Conditions")
valuetocheck=2
value2tocheck=2
if (valuetocheck>0):
    print("inside the main first level Nested if block");
    print(f"input value is {valuetocheck}");
    if (valuetocheck == 1):
        print("inside the second level Nested if block");
        print(f"input value is {valuetocheck}");
    elif (valuetocheck == 2):
        print("inside the second level Nested elif block");
        print(f"input value is {valuetocheck}");
        if(value2tocheck==1):
            print("inside the third level Nested if block");
            print(f"input value is {valuetocheck}");
        elif(value2tocheck==2):
            print("inside the third level Nested elif block");
            print(f"input value is {valuetocheck}");
    else:
        print("inside the second level Nested else block");
        print(f"input value is {valuetocheck}");
else:
    print("inside the main first level else block");
    print(f"input value is {valuetocheck}");
print(f"any cost, I am going to run outside the body of If or else");

# find the greatest of 2 numbers
print("enter number1")
#num1=int(input())
num1=20
print("enter number2")
#num2=int(input())
num2=30
if (num1>=num2):
    print("number1 is greater or number1 is equal to number2")
else:
    print("number2 is greater ")

# find the greatest of 2 numbers
print("enter number1")
#num1=int(input())
num1=10
print("enter number2")
#num2=int(input())
num2=11
if (num1>num2):
    print("number1 is greater")
elif(num1<num2):
    print("number2 is greater ")
elif(num1==num2):
    print("both are same ")

# find the greatest of 2 numbers (optimal solution)
print("enter number1")
#num1=int(input())
num1=10
print("enter number2")
#num2=int(input())
num2=20
if (num1>num2):
    print("number1 is greater")
elif(num1<num2):
    print("number2 is greater ")
#instead of putting elif(num1==num2), we can directly apply else right?
else:
    print("both are same ")
# find the greatest of 3 numbers (think about using logical operator along with comparison operator
print("Starts greatest of three numbers")
#a = int(input()) #10
a=10
#b = int(input()) #20
b=30
#c = int(input()) #30
c=20
if (a>b) and (a>c):
    print ('a is greater number')
elif (b>a) and (b>c):
    print ('b is greater number')
elif (c>a) and (c>b):
    print ('c is greater number')
elif(a==b==c):
    print ('all are same numbers')
elif (a==c):
    print ('a and c have equal values')
elif (b==c):
    print ('b and c have equal values')
else:
    print ('a and b have equal values')

#looping constructs
#looping constructs used for performing iterations or repetitive operations conditionally/unconditionally
#types of looping (in python for and while including nested looping)
#in general for, while, do while loop (not available) including nested looping

#for loop (repeating operation)- used for iterting unconditionally (example select * from tablename)
#one thumb rule for for loop is we need iterable values to apply the loop in a form of sequences
#select statement is a literal example of for loop

sal_lst=list([10000,20000,15000,25000,30000])
festival_bonus=1000
#for bonus in festival_bonus:
#    print(bonus)

for sal in sal_lst:
    if(sal>10000):
        bonus_sal=sal+festival_bonus
        print(f"bonus is {festival_bonus} and salary is {sal} and bonus applied sal is {bonus_sal}")
#while loop (repeating operation until condition is succeeded)- used for iterting conditionally (example select * from tablename)

#while loop - Condition First then Iteration

sal_lst=list([10000,20000,15000,25000,30000])
festival_bonus=1000
#for bonus in festival_bonus:
#    print(bonus)
#simple looping on iterable values
for sal in sal_lst:
    #if(sal>10000):
        bonus_sal=sal+festival_bonus
        print(f"bonus is {festival_bonus} and salary is {sal} and bonus applied sal is {bonus_sal}")

#Nested for loop
#example1
lst1=['Anuja','Vaishali','Dhana']
lst2=['perk','munch','dairymilk']

lst1=['row1','row2','row3']
lst2=[('col1.1','col1.2'),('col2.1','col2.2'),('col3.1','col3.2')]
for row in lst1:
 for col in lst2:
  for cols in col:
   print(row,cols)

for i in lst1:
  print(f"good morning {i}")
  for j in lst2:
   print(f"{i} is gifted with {j}")

#example2
#HIVE TABLE -> tablename is lst, table contains 2 rows, column structure is int,string,array<int>
lst=[[1,"irfan",[10,20]],[2,"dhana",[20,40,60]]]
#writing a selelct statment to do explode on name of the customer
for i in lst:
  print(i[1])
  for j in i[2]:
   print(f"{i[1]} purchased for {j} dollars")

#eg. hive table with bucketed column without sorting
promoamt=[1000,2000,900,500,1200,300,200,100,50,10]
for gift in promoamt:
    if gift < 1000:
        print(f"promo is not applicable for {gift}")
        continue
    print(f"promo is applicable for {gift}")

#eg. hive table with bucketed column with sorting
promoamt=[2000,1200,1000,900,500,300,200,100,50,10]
for gift in promoamt:
    if gift < 1000:
        print(f"promo is not applicable for {gift}")
        break
    print(f"promo is applicable for {gift}")

#break and continue options
#continue unconditionally continue the loop, break conditionally end the loop
#break example: if we apply in/= clause in hive it will check for the all occurance and continue the loop
state_lst=["TN","KE","PO","KA"]
for states in state_lst:
    if (states=='PO' or states=='GO' or states=='KA'):
        print(f"state tax for {states} is not applicable")
        continue
    print(f"state tax for {states} is 18% GST")

#break example: if we apply exists clause in hive it will just check for the first occurance and break the loop
state_lst=["TN","KE","PO","KA","GO","KA"]
for i in state_lst:
    if (i=='PO' or i=='GO' or i=='KA'):
        print("there are union territories in our list")
        break
    print(f"non union territories are {i}")

#while loop (repeating operation until condition is succeeded)-
# used for iterting conditionally (example select * from tablename limit 10)
# while loop can be used for creating infinate loops, where as for loop can't
# while loop can run based on a condition, where as for loop runs on a list of elements that are iterable.

#Typical simple while loop example:
i=1
maxiter=5
while i<=maxiter:
 print(i)
 i=i+1

#Infinate loop
i=1
maxiter=5
while i<=maxiter:
 print(i)
 i=i-1


#while loop as an alternative for "for loop"
lst2=['perk','munch','dairymilk']
idx=0
lent=len(lst2)-1
while idx<=lent:
  print(lst2[idx])
  idx+=1

#for loop as an alternative for "while loop"
i=1
maxiter=5
for a in range(i,maxiter+1):
  print(a)

#Create an authentication mechanism to allow the user to login with a given password
totalretry=3
initialvalue=1
password='hduser'

while initialvalue<=totalretry:#entry controlled while loop
    print(f"Enter the password for {initialvalue} time")
    userenteredpwd=input()
    if (userenteredpwd==password):
        print("You are authenticated, Welcome to Inceptez Technologies")
        break
    initialvalue+=1
#Try write the same above example using for loop?



#do while loop - is not available in python, but available in all other languages
#do while loop is an exit control loop, but while loop in an entry control loop
'''
Scala do while, lets achieve the same in python?
do{
     | println("hello")
     | }while(1==2)
'''
#python don't have do while, but lets try to achive?
while True:
    print("hello")
    if(1==2):
        print("continuing the loop")
    else:
        print("breaking the loop")
        break

#realtime example of do while loop?
#creation of a user by another user without sudo privledge
username="notsudoer"
while True:
    print("enter the password for irfan")
    userenteredpwd = input()
    if (username=="sudoer" and userenteredpwd=="root"):
        print("adding the other user in our system")
        break
    elif(username!="sudoer"):
        print("irfan is not in the sudoers file.  This incident will be reported.")
        break

#same password example with do while in python
totalretry=3
initialvalue=1
password='hduser'

while True:#entry controlled while loop
    print(f"Enter the password for {initialvalue} time")
    userenteredpwd=input()
    if (userenteredpwd==password):
        print("You are authenticated, Welcome to Inceptez Technologies")
        break
    if(initialvalue>=totalretry):
        break
    initialvalue+=1

#retry any number of times? Purpose of infinate loop
password='hduser'
while True:
    print(f"Enter the password any number of times, if want to break explicitly use ctrl+c")
    userenteredpwd=input()
    if (userenteredpwd==password):
        print("You are authenticated, Welcome to Inceptez Technologies")
        break

print("not inside the loop")

#while loop - Condition First then Iteration

#Collection Types:
#Collection Types: complex data types
#iterable or non-iterable - mutable/immutable
#emp table with few columns, i wanted to take salary column and apply bonus to it, using python prog how to do?
#without using python, by using sql we can simply do insert into bonus select sal+bonus from emp;

print("List type in python")
#Notation: []
#Access using index we can access the elements, where as in scala the same.
#indexed sequenced collection of homogeneous elements
emptbl=[(1,'irfan',10000),(2,'harish',20000),(3,'akash',15000),(4,'berlin',25000)]
#emptbl=[[1,'irfan',10000],[2,'harish',20000],[3,'akash',15000],[4,'berlin',25000]]
#emptbl
#col1 col2  col3
#1  irfan   10000
#2  harish  20000
#3  akash   15000
emplst=list([])
for row in emptbl:
    print(row[2])
    emplst.append(row[2])

print(emplst)
#emplst=[10000,20000,15000]
bonus=1000
bonuslst=list([])
print(bonuslst)
for col in emplst:
    print(f"salary is {col}")
    print(f"bonus applied value {col} + {bonus}")
    bonuslst.append(col+bonus)

print(bonuslst)

#python-hive
#tuple-structure
#dictionary-map
#list-array (collect_list function)
#set-array (collect_set function)

#What is collection type?
#refer pdf page18
#Why we need collection types?
#think about the application of using collection types to parse the json configuration/rules/data structure
#different types of collection types?
#list, dictionary, tuple, set
#category of collection types
#sequence/mapping -> mutable/immutable
#functions applied?
#eg. sort, reverse/reversed ...
#Operations performed such as select/insert/update/delete
#collectiontype[index] to access
#colltype[index]=value to update
#colltype.pop()) to delete

emplst=[10000,20000,15000]
sortedrevemplst = []
for i in reversed(sorted(emplst)):
    sortedrevemplst.append(i)

emptuple=(10000,20000,"abc")
emplst=[10,30,20,40]
sortedrevemplst = []

for i in reversed(sorted(emplst)):
    sortedrevemplst.append(i)
print(sortedrevemplst)

lst=[10,30,20,40]
lst.sort(reverse=True)
print(lst)

print("list operations")
lst = [10, 20, 30, 40]
#select/access
print(lst[2])
#append in the last
lst.append(50)
print(lst)
#insert in the index position
lst.insert(2,25)
print(lst)
#update the list elements
lst[3]=lst[3]+5
print(lst)
#delete the elements of the list using value
lst.remove(40)
#delete the elements of the list using index
lst.pop(4)
print(lst)

lst=["chn","goa","mum","del"]
print(lst)
#search for a value with in the given index range
print(lst.index("goa",0,2))
print(lst.count("goa"))
lst.remove("goa")
print(lst)

#lst = [10, 20, 30, 40]
otherlst=[]
for i in reversed(lst):
    otherlst.append(i)

print(otherlst)
lst.clear()
print(lst)
lst.extend(lst)

print("2. Dictionary type in python")
#Notation: {}
#Access using the key or (by using the keys and values functions we can access the elements using index)
#Collection of key, value pair of homogeneous keys and homogeneous values with k,v is of hetrogeneous types
#eg. {1:"irfan",2:"inceptez"}
#dictionary is mutable
#dictionary is iterable
#keys are non duplicated, but values can have duplicates
dict1={1:"irfan",2:"inceptez",3:"irfan",3:"vignesh"}
print("dictionary operations")
#select/access
print(dict1[2])
dict1.keys()
dict1.values()
dict1.items()

#append/insert into dictionary
dict1.update({4:"vanmathi"})
#update the list elements
dict1.update({4:"saravanan"})

#delete the elements of the list using value
#dict1.clear()
dict1.pop(3)
dict1.popitem()
#delete the elements of the list using index


print("3. Tuple type in python")
#Notation: ()
#Access using index where as in scala tuples can be accessed by using name or position
#indexed sequenced collection of hetrogeneous elements
#tuples are immutable
tuple1=tuple((1,'irfan',41,100000))
print("tuple operations")
#select/access
#basic function supported
print(tuple1.index('irfan',0,4))
print(tuple1.count('irfan'))
print(tuple1[2])
#append in the last
tuple1=list(tuple1)
tuple1.append(50)
tuple1=tuple(tuple1)
print(tuple1)
#If we want to perform all list/set operations on a tuple, which is not directly supported, can be achieved using the conversion
#same way we can achieve the rest of activities also
#insert in the index position
lst=list(tuple1)
lst.insert(2,25)
print(lst)
#update the list elements
lst[3]=lst[3]+5
print(lst)
#delete the elements of the list using value
lst.remove(50)
#delete the elements of the list using index
lst.pop(4)
print(lst)
tuple1=tuple(lst)

print("4. Set type in python")
#Notation: {}
#Access using iteration/functions/converting to list/tuple to access using index
#iterable collection of unique elements
#sets are mutable
#select/access
#using iteration
st={1000,2000,3000,6000,4000,5000,1000,2000} #set will not accept duplicates
for i in st:
    print(i)
st2={1000,3000}
st.difference(st2)#using the functions we can modify the content of a set or select also

lstst=list(st)#convert set to list and access using index
print(lstst[0])

#insert/append
st.add(7000)
st3=st.copy()
st2={1000,8000}
st.update(st2)#inserted else updated (merge operation is happening)

#update the list elements
st.update(st2)#inserted else updated (merge operation is happening)
st.difference_update(st2)#using the functions we can update the content of a set using the result of the function applied
st.intersection_update(st2)#using the functions we can update the content of a set using the result of the function applied

#delete the elements of the list using value
st.pop()#pop out/delete the set from the left element
st.remove(6000)#delete a given element
st.discard()#delete a given element silently

#Set specific operations
st2={3000,7000}
st={2000, 3000,5000, 7000}
st2.issubset(st)#true
st.issuperset(st2)#true
st.isdisjoint(st2)#false
st2={1000,17000}
st.isdisjoint(st2)#true
st.union(st2)
st.symmetric_difference(st2)#the elements between 2 sets by removing the common elements