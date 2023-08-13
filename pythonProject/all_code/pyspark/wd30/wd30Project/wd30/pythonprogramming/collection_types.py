a="irfan"
b="IrFan"
if (a.casefold()==b.casefold()):
    print("both are same")
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

