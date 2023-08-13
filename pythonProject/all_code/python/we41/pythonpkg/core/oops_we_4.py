print("4. OOPS - Object Oriented Programming")
print("Fundamentals of FBP - Must know -Hierarchy of python programming (function.py) -> pkg,subpkg,module,function (different types of initial 5 functions)")
print("Fundamentals of OOPS - Must know -Hierarchy of python programming (colums.py)->pkg,subpkg,module, Class, Method/func, Object/instance, members, Constructor, self, init")
from chennaishowroom.guindy.showroom import Car #A class is a template or blueprint for creating objects at runtime.
obj1=Car()#Object is an instance of the class or Object is a constructed in memory by instantiating the class
#in the above line - obj1 is an object refers the class loaded in memory, () represents the instantiation/construction of the class in memory
#https://create.microsoft.com/en-us/templates/resumes-and-cover-letters (pkg/subpkg/..)-> social-media-marketing-resume-d68df550-2dce-4601-ac59-22b431043ee8(module) ->
# Modern hospitality resume (class)-> loaded file in memory (object)-> PROFILE/ACTIVITIES AND INTERESTS (member)...
obj1.ac(True,2)
import chennaishowroom.guindy.showroom
onroad_vinoth2_car_obj=chennaishowroom.guindy.showroom.Car()

#Features of OOPS - reusability, simplicity, performance, reduction in LOC..
#Simple Object oriented program example
class Car:
    def ac(self,power,level):
        return (power,level)
    def music(self,power,mode):
        return (power,mode)

class Finance:
    def emi(self,total,tenure):
        return total/tenure

#from chennaishowroom.guindy.showroom import Car
onroad_vinoth_car_obj=Car()
print(onroad_vinoth_car_obj.ac(True,3))

#from chennaishowroom.guindy.showroom import *
onroad_lavanya_finance_obj=Finance()
print(f"Taking finance {onroad_lavanya_finance_obj.emi(1000000,36)}")

onroad_lavanya_car_obj=Car()
print(onroad_lavanya_car_obj.music(True,'fm'))

print("************** 9. Classes, Functions & Methods **************")
#########Important stuffs to understand and know#########
#minimum what we are supposed to know
#1. Define a class
#2. Define member of the class (passing self arg)
#3. Import the pkg.subpkg.module.class
#4. instantiate the class by creating object
#5. access the members of the class using object.membername

#MINUMUM REFER THIS BELOW CODE TO UNDERSTAND THE FUNDAMENTAL OF OOPS in Python

#Eg of OOPs: I want to buy/bring onroad a car from a showroom in chennai in guindy and want to start and drive the car with ac and music system
#Company - chennai pkg->guindy subpkg->showroom module->car class->ac and music functions/methods
#Customer - refer the pkg.subpkg.module import car,finance then load the car class in memory as an object
#clean or a comprehensive program you should understand
class Company:
    def __init__(self,bonus):
        self.bonus1=bonus
    def sal_calc(self,sal):
        return sal+self.bonus1
obj1_jan=Company(5000)
obj1_feb=Company(3000)
print(obj1_jan.sal_calc(10000))
print(obj1_feb.sal_calc(10000))

'''
#Piyush
class - keyword to define a Class.
Company - class name with init upper case.
def - keyword to define a function.
_init_ - special method to initialize instance variables, called when class is instantiated. it helps to create parameterised constructor.
self - by default functions inside a class accept first args as self. the object of a class will be passed in self parameter.
bonus1 - instance variable.
obj1_jan/obj1_feb - different instances or objects of class Company.
Company(5000) - Company() constructor which accepts one args as bonus (because i used __init__ function with parameterized constructor)
obj1_jan.sal_calc(10000) - sal_calc() function/method is called using obj1_jan.sal_calc(10000) and 10000 passed as arg to sal.

#Singaraj
class Company:- class - Template or blueprint program instantiated at the runtime
	    def __init__(self,bonus): init- initiate the function - allow the variables or attributes to functions
	        self.bonus1=bonus - self is like an initial(ref to class)
	    def sal_calc(self,sal):
	        return sal+self.bonus1
	obj1_jan=Company(5000) -obj1 is an object- instances(memeber variable which got loaded in to the memory area along with the  class)
	obj1_feb=Company(3000)
	print(obj1_jan.sal_calc(10000))
	print(obj1_feb.sal_calc(10000))
	
#Naresh	
	class Company:                     #>>Class created in the name of Company. (class name in int.cap case)
    def __init__(self,bonus):      #>>__init__ is a function initiated when the class is loaded into memory
        self.bonus1=bonus          #>>self is used to call the class name
    def sal_calc(self,sal):        #>>self. is used mention bonus1 is member of the class
        return sal+self.bonus1
obj1_jan=Company(5000)             #>>Class is loaded into memory object of obj1_jan with bonus value of 5000
obj1_feb=Company(3000)             #>>Class is loaded into another memory object name of obj1_feb with bonus value of 3000
print(obj1_jan.sal_calc(10000))    #>>Class loaded in the memory object obj1_jan is called to print addition of sal and bonus arg mentioned in obj1_jan
print(obj1_feb.sal_calc(10000))    #>>Class loaded in the memory object obj1_feb is called to print addition of sal and bonus arg mentioned in obj1_feb
'''

#Hierarchy of programming in Python - package->subpkg->module.py->inline code/functions

#from pyspark.sql.session import SparkSession
#spark=SparkSession.builder.master("abc").getOrCreate()

from consumer.learning.sample_programs import AllAccounts
memory_space_object=AllAccounts()#Instance of the class (construction of the class program in memory for consumption)
print(memory_space_object.pf(10000))


def f1(num1):
    return num1+100
print(f1(200))
#from pyspark.sql import SparkSession
print("To learn about the fundamentals of OOPS - pkg,subpkg,module,Class, Method/func, Object/instance, Members,self")
class Father:#Template/blueprint program instantiated at the runtime by creating an object
    son=100#member variable of a class
    daughter=200
    def f1(self,num1):#member function
#self (object of Father class) is a keyword used to refer the object of the given class to access the members of the class,
# we use self. eg. self is like an initial (father) for a son
        return num1+self.son+self.daughter

obj1=Father()#obj1 is an object is a variable to refer the memory where class is constructed/loaded, () is a instance of the class Father
print(obj1.f1(1000))

print("To learn about the fundamentals of OOPS - Constructor, init")
#Classes in Python can be constructed using 3 methodologies -
print("Default Constructor - If you are not going to have an init function, automatically the member variables will be instantiated when class is referred")
class Father:#Default Constructor Class (automatically by default python will add init function in the background)
    son=100# static member variable/attribute of a class
    daughter=200
    def f1(self,num1):#member function
        return num1 + self.son + self.daughter
obj1=Father()
print(obj1.f1(10000))
print("Non Parameterized Constructor - If the class is instantiated, then the init function will initialize some static members of the class")
class Father:# (Non Parameterized Constructor - init function will be explicitly mentioned and initialized when class is initialized, without params)
    def __init__(self):#I don't pass any parameters when the class is initialized (Non Parameterized Constructor)
        choclate=5#local member
        self.daughter = 200+choclate #class member
        self.son=100+choclate# member variables will be instantiated (with the help of init function) when the class is instantiated
    def f1(self,num1):#member function
        return num1 + self.son + self.daughter#+self.choclate
obj1=Father()
print(obj1.f1(10000))

print("(Parameterized Constructor - init function will be explicitly mentioned and initialized when class is initialized)")
class Father:# (Parameterized Constructor (regularly used) - init function will be explicitly mentioned and initialized when class is initialized)
    def __init__(self,daugh,sonn):#I pass parameters when the class is initialized (Parameterized Constructor)
        choclate=5#local member
        self.daughter = daugh+choclate #class member
        self.son=sonn+choclate# member variables will be instantiated (with the help of init function) when the class is instantiated
    def f1(self,num1):#member function
        return num1 + self.son + self.daughter#+self.choclate
obj1=Father(200,100)
print(obj1.f1(10000))
obj2=Father(300,200)
print(obj2.f1(10000))
obj3=Father(400,500)
print(obj3.f1(10000))

class Telephone :
	def __init__(self, model,costperhour):
		self.model=model
		self.costperhour=costperhour
	def installationcost(self,hoursforinstallation):
		return hoursforinstallation*self.costperhour


agent1_rotary_installation_cost=Telephone('rotary phone',10)
agent1_tt_installation_cost=Telephone('touchtone phone',5)
agent1_router_installation_cost=Telephone('router',3)

charge_tothe_cust=agent1_rotary_installation_cost.installationcost(6)
print(charge_tothe_cust)

charge_tothe_cust=agent1_tt_installation_cost.installationcost(4)
print(charge_tothe_cust)

charge_tothe_cust=agent1_router_installation_cost.installationcost(10)
print(charge_tothe_cust)

####Important items are completed####

######Good to know items are started######
print("************** 10. Detailed OOPS Fundamentals & Features **************")
#Why we have to know this - To develop an App or understand a developed App/Framework with better awareness or to teach your kids
#1. Inheritance - Incurring some of the properties from the parent class
# Eg: Son inherits the property/money from his father/mother/parents/grandfather
#2. Polymorphism - Overloading/Overriding
#3. Encapsulation
#4. Abstraction

#1. Inheritance - Incurring some of the properties from the parent class
# Eg: Son inherits the property/money from his father/mother/parents/grandfather
#Single Inheritance
class Father:
    land=2400
class Son:
    house=1500

sonobj1=Son()
print(f"Son can construct a house of {Son.house} sqft")

# 10.1. Inheritance - Incurring some of the properties from the parent class
# benifits - reusability, reduced LOC, improved performance, modularity, better uniformity
# Eg: Son inherits the property/money from his father/mother/parents/grandfather
print("Single Inheritance - If a child class inherits from one parent class")

class Father:  # parent class or base class
    land = 2400

    def money(self):
        amount = 100000 + 10000
        return amount


class Son(Father):  # child class or derived class - son can inherit from the father
    # land=2400#needed? no
    house = 1500


sonobj1 = Son()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1.land} with the amount of {sonobj1.money()}")

print("Multipe Inheritance - If a child class inherits from more than one parent class")


class Father:  # parent class or base class
    land = 2400
    # timber='teak'


class Mother:  # parent class or base class
    def money(self):
        amount = 100000 + 10000
        return amount


class Son(Father, Mother):  # child class or derived class - son can inherit from the father
    # land=4000#needed? no
    house = 1500


sonobj1 = Son()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1.land} with the amount of {sonobj1.money()}")

print(
    "Multlevel Inheritance - (A class can inherits and can be inherited by some other calss) If a class inherits from another parent class that was inheriter by some other class")


class GFather:  # parent class or base class
    land = 2400


class Father(GFather):  # child class1
    def money(self):
        amount = 100000 + 10000
        return amount


class Son(Father):  # child class2  or derived class - son can inherit from the father
    # land=4000#needed? no
    house = 1500


sonobj1 = Son()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1.land} with the amount of {sonobj1.money()}")

print("Hierarchical Inheritance - If more than one child class inherits from one parent class")


class Father:  # child class1
    land = 2400

    def money(self):
        amount = 100000 + 10000
        return amount
class Son(Father):  # child class1  or derived class - son can inherit from the father
    # land=4000#needed? no
    house = 1500

class Daughter(Father):  # child class2  or derived class - daughter can inherit from the father
    # land=4000#needed? no
    house = 2000


sonobj1 = Son()
daughterobj1 = Daughter()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1.land} with the amount of {sonobj1.money()}")
print(
    f"Daughter can construct a house of {daughterobj1.house} sqft on a land of {daughterobj1.land} with the amount of {daughterobj1.money()}")

print("10.2 - Poly-morphism (many - forms/faces/shapes) (Overloading/Overriding)- "
      "If a person accepts different input the behaves accordingly ")
print(
    "Overloading - Function with same name but different in type and number of arguments - returns result differently")
# Eg: Len function
lst = ["Inceptez"]
str1 = "Inceptez"
print(len(lst))  # different input types with respective results
print(len(str1))  # different input types with respective results


# rdd1=sc.parallelize(range(1,10),2)#different number of arguments with respective results
# rdd1.getNumPartitions()
# rdd1=sc.parallelize(range(1,10))#different number of arguments with respective results
# rdd1.getNumPartitions()

# Can we achive overloading in Python directly? not directly,
# this can be achieved by arbitrary/arbitrary keyword/default arg function
class Abcd:
    def f1(self, *a):
        if len(a) == 1:
            return a[0] * a[0]
        else:
            return a[0] + a[1]


#    def f1(self,a,b): #not possible
#        return a + b
obj1 = Abcd()
print(obj1.f1(10))
print(obj1.f1(10, 10))
'''
scala> class abcd{
     | def f1(a:Int):Int={return a*a}
     | def f1(a:Int,b:Int):Int={return a+b}
     | }
defined class abcd

scala> val obj1=new abcd
obj1: abcd = abcd@411f53a0

scala> obj1.f1(10)
res9: Int = 100

scala> obj1.f1(10,10)
res10: Int = 20

'''
print(
    "Overriding - Function with same name across different classes has different functionalities, parent class function can override the child class function")


# In Python when you have two methods with the same name that each perform different tasks then it is overriding

class Father:  # child class1
    land = 2400

    def money(self):
        bank_amount = 100000 + 10000
        return bank_amount


class Son(Father):  # child class1
    # land=4000
    house = 1500

    def money(self):  # Over riding the function in the parent class
        hand_amount = 200000
        return hand_amount


sonobj1 = Son()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1.land} with the amount of {sonobj1.money()}")

print(
    "3.1 - Encapsulation - Protecting/Controlling the access for the functionalities or values inside/across the classes")


# car - ignition module - key, security system works (immobilizer)
# Access controls to the car - rent a car Uber
# public(passenger) - access upto the front seat,back seat
# protected(driver) - access upto the front driver seat + key + steering + devices
# private(manufacturer) - access to everything + immobilizer
# Banking -> lobby(public), officeroom (protected), locker room (private)
class Father:
    wood = "teak"  # public member (by default)
    _land = 2400  # protected member with the prefix of _
    __money = 10000  # private member with the prefix of __
    _money = __money - 9000  # private members can be only accessed by the other members of the same class
    # def __money(self): #private member with the prefix of __


#        hand_amount=200000
#        return hand_amount
#    money=__money()
class Son(Father):  # child class1
    house = 1500
    # money=__money


obj1 = Father()
print(obj1.wood)  # public can be accessed by anyone without any restriction
print(obj1._land)  # In general the protected member of a class should be only accessed by the sub/child/derived class,
# but python is weak in protecting the values being allowed to be used by everyone
# print(obj1.__money)#private can be only accessed by the same class members and can't be accessed by anyone else - even derived class or by creating a direct object

sonobj1 = Son()
print(
    f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1._land} with the amount of {sonobj1._money}")
# print(f"Son can construct a house of {sonobj1.house} sqft on a land of {sonobj1._land} with the amount of {sonobj1.money}")

print("4.1 - Abstraction - Hiding of functionality/forcing to use of the implementation of the functionalities")
# Car has power feature - keyentry or button start
# Abstraction - the features are hided/not implemented which is forced to implement later as per the requirement
# add abstraction feature toward this program to overcome the problem of not implementing the parent class functions by the child class
from abc import ABC, abstractmethod


class CommonFeatures(ABC):  # if i don't have the concept of abstraction, then there is a major flaw in this system?
    # land=2400
    @abstractmethod  # convert the ignition function to abstract method (which says all the child classes has to implement this method)
    def ignition(self):  # no implementation of features, because we need customization
        pass
    @abstractmethod  # convert/decorate the ignition method to abstract method (which says all the child classes has to implement this method)
    def engine(self):  # no implementation of features, because we need customization
        pass
    def music(self, mode, volume):
        if mode == 'fm':
            vol = 0
            return ("fm", vol + volume)
        else:
            vol = 0
            return ("mp3", vol + volume)


class Alto(CommonFeatures):
    def powersteering(self):
        return True
    def ignition(self):
        return 'keyentry'
    def engine(self):
        return 'k10'


class Ertiga(CommonFeatures):
    # Abstraction means - all unimplemented abstract methods of the parent class has to be implemented by the child classes
    def engine(self):# forceful implementation with the inclusion of abstraction
        return 'kappa'
    def powersteering(self):
        return True
    def cruise(self):
        return True
    def ignition(self):  # forceful implementation with the inclusion of abstraction
        return 'button'


altoobj1 = Alto()
print(altoobj1.ignition())
print(altoobj1.engine())
print(altoobj1.powersteering())
# print(altoobj1.music('fm',5))

ertigaobj1 = Ertiga()
print(ertigaobj1.ignition())
print(altoobj1.engine())
print(ertigaobj1.powersteering())
print(ertigaobj1.music('music', 6))


print("11. decorator is a concept of converting or decorating a given function using @decorator_function prefix")
def sal_bon_penality(sal,bonus):
 return sal+bonus
print("normal function")
print(sal_bon_penality(10000,1000))

def sal_bon_lop_decorator(lop):
 print("decorator function to LOP function")
 def lop(a,b):
  return a-b
 return lop

@sal_bon_lop_decorator
def sal_bon_penality(sal,bonus):
 return sal+bonus

print("decorated function does Loss of pay calculation")
print(sal_bon_penality(10000,1000))

print("12. different class methods (static method, instance method, class method)")
#instance method - Common type of methods we create in python, requires instantiation (object) of a class to access his members
class InsMetClass:
    def __init__(self):
        self.attr1=100
    def add(self,a,b):
        return a+b+self.attr1

obj1=InsMetClass()#this is needed
#InsMetClass.add(10,100,200)
print(obj1.add(10,20))

#Class method - special type of methods in python, no need of creating (object) instance of a class to access the members of the class
# needs @classmethod decorator
class ClassMetClass:
    attr1=100
    def sub(self,a,b):
        return a-b+self.attr1
    @classmethod
    def add(cls,a,b):#converted to class method, which doesn't require object instantiation
        return a+b+cls.attr1

#without creating the instance of the class, I want to access the class members, then use class method in python
print(ClassMetClass.add(10,20))
obj1=ClassMetClass()
print(obj1.add(10,20))

#Static method - special type of methods in python, no need of creating (object) instance of a class to access the members of the class
# needs @staticmethod decorator
#only difference between class and static method is - static method can't access the members of the class
class ClassMetClass:
    attr1=100
    def sub(self,a,b):
        return a-b+self.attr1
    @staticmethod
    def add(a,b):#converted to static/standalone method, which doesn't require object instantiation, but can't access the other membersof the clas
        return a+b #can't access the other members of the class

#without creating the instance of the class, I want to access the class members, then use class method in python
print(ClassMetClass.add(10,20))

#conclusion:
#Instance Method ->
# 1. No decorator is needed (default)
# *2. IM can be accessed by using the object of the class (IM method will be accessed only at the time when class is instantiated)
# 3. IM can access all the member variables of the class
#Class Method ->
# 1. decorator is needed (@classmethod)
# *2. CM can be accessed by using the class directly (no need to create an instance of the class, CM can be accesed directly)
# **3. CM can access all the member of the class
#Static Method ->
# 1. decorator is needed (@staticmethod)
# *2. SM can be accessed by using the class directly (no need to create an instance of the class, CM can be accesed directly)
# **3. SM cannot access member of the class because it is standalone/static