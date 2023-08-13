
#11. Write a program to find the greatest of 3 numbers
a=4
b=2
c=6
if (a == b == c):
    print("all are equal")
    print("one more print all are equal")
    if a > 100:
        print("a is greater than 100")
elif a > b and a > c:
    print("a value %d which is greater than b and c" % a)
elif b > a and b > c:
    print("b value %d which is greater than a and c" % b)
else:
    print("c value %d which is greater than a and b" % c)

#12. Write a single program to find the given number is even or whether it is negative
# and print the output as (the given number is even but not negative
# or the given number is not even but negative or the given number is neither negative nor even)

x=2
y=x%2
print(y)
if x<0:
    if y==0:
        print("The given number %d is negative and an even number" % x)
    else:
        print("The given number %d is negative and not an even number" % x)
elif x>0:
    if y==0:
        print("The given number %d is positive and an even number" % x)
    else:
        print("The given number %d is positive and not an even number" % x)
else:
    print("The given number %d is neither negative nor even number" % x)

#13. Write a nested if then else to print the course fees - check if student choosing bigdata, then fees is 25000,
# if student choosing spark then fees is 15000,
# if the student choosing datascience then check if machinelearning then 25000 or if deep learning then 45000 otherwise if both then 25000+25000.

print("The courses offered are the below,")
print("bigdata")
print("spark")
print("datascience")

#course=str(input("please choose the course to know fee details"))
course="spark"
if course=='bigdata':
    print("The fee for bigdata is 25000")
elif course=='spark':
    print("The fee for spark is 15000")
elif course=='datascience':
    print("There are two courses offered under datascience")
    print("machinelearning")
    print("deep learning")
    course=str(input("please choose the course to know fee details"))
    if course=='machinelearning':
        print("The fee for machinelearning is 25000")
    elif course=='deep learning' or 'deeplearning':
        print("The fee for deep learning is 25000")
    else:
        print("The fee is 25000+25000")
else:
    print("Invalid course entered")


#14. Check whether the given string is palindrome or not (try to use some function like reverse).
#For eg: x="madam" and y="madam", if x matches with y then print as "palindrome" else "not a  palindrome".

#string_entered=str(input('Enter a string to check if its palindrome '))
string_entered="madam"
rev_of_string=string_entered[::-1]

if string_entered==rev_of_string:
    print('its palindrome')
else:
    print('not a palindrome')

#15. Check whether the x=100 is an integer or string.
#(try to use some functions like str or upper function etc to execute this use case) or use isinstanceof(variablename,datatype) function.

def isNumber(x):
    for i in range(len(x)):
        if x[i].isdigit() != True:
            return False
    return True

#if __name__ == "__main__":

x = "100"

if isNumber(x):
   print("Integer")
else:
   print("String")

#Control Statements

#16. Write a program using for loop to print even numbers and odd numbers
# in the below range of data (generate using range function) [5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
# output should be with even as 6,8,10,12,14,16,18,20 and odd as 5,7,9,11,13,15,17,19.

def checkIfEven(x):
    y = x % 2
    if y == 0:
       return True
    else:
       return False

list = [5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
print(f'The full list is {list}')
even_list=[]
odd_list=[]
for i in range(len(list)):
#    print(f'Checking the number from list {list[i]}')
    if checkIfEven(list[i]):
        even_list.insert(i,list[i])
#       print(f'even list {even_list}')
    else:
        odd_list.insert(i,list[i])
#        print(f'odd list {odd_list}')

print(f'The EVEN list is {even_list}')
print(f'The ODD list is {odd_list}')


#17. Write a while loop to loop from 0 till 21 with the increment of 3,
#the result should be exactly 3,6,9,12,15,18 and store this result in a list
list=[]
x=0
while(True):
    x=x+3
    if (x<=21):
        list.append(x)
    else:
        break

print(f'The list is {list}')

#18. Write a for or while loop to print the cube of 4,# result should be 4*4*4=64
# (initiate some variable outside the loop with 4 and loop through 3 times to achieve the result)

cube=4
csize=1
x=1
while(True):
    if (x<4):
        csize=cube*csize
        x=x+1
    else:
        break
print(f'Cube of 4 is {csize}')

#19. Create a list as sal_lst=[10000,20000,30000,10000,15000],
# loop through the list and add 1000 bonus to the salary and store in another list sal_bonus_lst=[11000,21000,31000,11000,16000]
#then store the bonus applied salary in another list where sal>11000

sal_lst=[10000,20000,30000,10000,15000]
sal_bonus_lst=[]
sal_bonus_lst1=[]
bonus=1000
max_sal=11000
for i in range(len(sal_lst)):
    sal_bonus_lst.insert(i,sal_lst[i]+bonus)
    if sal_bonus_lst[i]>max_sal:
        sal_bonus_lst1.insert(i,sal_lst[i])

print(f'The salary list {sal_lst}')
print(f'Bonus applied salary list {sal_bonus_lst}')
print(f'Salary >11000 {sal_bonus_lst1}')


#20. Write a do while loop to print “Inceptez technologies” n number of times as per the input you get from the user.
#Minimum it has to be printed at least one time regardless of the user input.

print_me="Inceptez technologies"
x=0
#x=int(input("How many times woould you like to print me "))
if x==0:
    x=1
while(True):
    if (x>=1):
        print(print_me)
        x=x-1
    else:
        break

#21. From the given list of list of elements produce the following output using nested for loop lst1=[[10,20],[30,40,50],[60,70,80]],
# calculate the sum of all number,
# calculate the min value and
# the max value of all the elements in the lst1.

lst1=[[10,20],[30,40,50],[60,70,80]]
sum_of=0
maxx=0
minn=0
for i in lst1:
    for j in i:
        e1=i[0]
        sum_of=sum_of+j

max_val=(max(lst1, key=sum))
min_val=(min(lst1, key=sum))
print(f'max value of all the elements in the lst1 {max_val}')
print(f'min value of all the elements in the lst1 {min_val}')
print(f'sum of all the elements in the lst1 {sum_of}')
lst1.reverse()
print(f'reverse of lst1 {lst1}')
lst1.sort()
print(f'sort of lst1 {lst1}')

'''
for i in range(len(lst1)):
    print(f' i {lst1[i]}')
    for j in range(i):
        print(f'j  {j}')
        sum_of=j+sum_of
        print(f'Sum of list of list elements is {sum_of}')'''
#Create a looping construct to create 3 tables upto 10 values. Output should be like this…
#1 x 3 = 3
#2 x 3 = 6
#3 x 3 = 9
#....
#10 x 3 = 30

table_for = 3
upto = 10

for i in range(1,upto+1):
    print(f"{table_for} X {i} = {table_for * i}")


#23. Create a list with a range of 10 values starting from 2 to 11
#and prove mutability by updating the 3rd element with 100 and prove resizable properties by adding 100 in the 5th position.

list1=[*range(2,12,1)]
print(list1)
list1[2]=100
print(list1)
list1.insert(4,100)
print(list1)

#lst2.__add__('x')
weekdays = ['Monday', 'Tuesday', 'Tuesday', 'Thursday', 'Friday']
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

flt=10.0
isins=isinstance(flt,float)
print(isins)

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

print(f"Length of the sequence {len(strdata)}" ) # not str conversion is needed
print("Length of the sequence " , len(strdata))
print("Length of the sequence {}".format(len(strdata)))
#len() and string.__len__()
print("Length of the sequence " + str(len(strdata)))
print("Length of the sequence " + str(strdata.__len__()))

#Static defined - by assigning a variable with a type and cast the value as per the type
tn:float=float(30)
print(type(tn))

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
count = 2
#while (count == 2): count = count - 1; print("Hello1 Inceptez"); print("Hello2 Inceptez2")

while (count == 2):
    count = count - 1; print("Hello1 Inceptez"); print("Hello2 Inceptez2")

#24. Create a tuple of 2 fields eg. ("Inceptez","Technologies","Pvt","Ltd"),
# prove immutability and non resizable nature,
# access the 2nd and 4th fields and store in another tuple.

tup=("Inceptez","Technologies","Pvt","Ltd")
print(tup)
#tup1=tup(1)=tech
#print(tup1)
#tup2=tup.insert(2,"add")
#print(tup2)
tup3=()
tup4=()
tup3=(tup[1])
print(tup3)
tup4=(tup[3])
print(tup4)
lst = [5,4,2,3,1]

print("Set (mutable) - {} ")
#by default sorts and remove duplicates
my_set = {5,1, 3, 1}
my_set1 = {5,1}
#print(my_set[1])
print(my_set)
print("set converted to list")
#st_my_set =list(my_set)
#print(lst_my_set)
print(my_set.__len__())
print(my_set.intersection(my_set1))

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
