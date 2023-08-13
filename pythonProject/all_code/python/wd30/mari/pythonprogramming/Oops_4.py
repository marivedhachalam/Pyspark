#python inline programs & few functions
#gcs://json_rules.json -> raw rules table (bq1) -> parse -> step parsed bq2 -> final SQL bq3 -> sql (BQ) (orchestrated airflow (cloud composer))
# Bigquery SQLs(SPs) ->SQL execution -> loading data -> BQ raw data (bq4) -> ETL -> detail, summary, kpi, unified, rel ((orchestrated airflow (cloud composer)))-> DScience
# /bigquery sqls/cloud composer(airflow)

print("If I just want to stay with FBP and don't want to go with Object Oriented Programming Concepts:")
from automobile.sales.four_wheelers import emi
#class_tenure=12
#def emi(amt):
#    return amt / class_tenure
print(emi(10000))

print("Object Oriented Programming Concepts:")
#Different OOPS related Functions
#overloaded/overrided/instance methods/static methods/class methods
print("Few important items to learn in OOPS")
#Hierarchy of programming in Python ->
#project=>package=>sub-package=>modules=>functions (FBP)
from automobile.sales.four_wheelers import EmiSchemes
#print(VehicleTypes().suv("x6","red"))
#what is an object - Instance or instantiation or initialization of a class
print("creating an object of the class EmiSchemes")
obj1=EmiSchemes(12)
print(obj1.emi(10000))#the members of the classes can be used only by instantiating the class



class Calc:
    def add(self,x,y):
        return x+y

from wd30.pythonprogramming.scratch import *
obj1=Calc()
print(obj1)
print(obj1.add(100,200))

#project=>package=>sub-package=>modules=>classes=>functions (OOPS-FBP)

print("************** 9. Classes, Functions & Methods **************")
#minimum what we are supposed to know
#1. Define a class
#2. Define member of the class (passing self arg)
#3. Import the pkg.subpkg.module.class
#4. instantiate the class by creating object
#5. access the members of the class using object.membername

#MINUMUM REFER THIS BELOW CODE TO UNDERSTAND THE FUNDAMENTAL OF OOPS in Python
class ClassName:
    class_attr=10#default constructor
    def __init__(self,class_attr_param):#non-parameterized or parameterized constructor
        self.class_attr = class_attr_param
    def funct(self,arg1):#instance method
        return arg1+self.class_attr
    @classmethod#decorator
    def funct2(cls,arg1):#instance method
        return arg1+cls.class_attr
    @staticmethod#decorator
    def funct3(arg1):#static method
        return arg1#+class_attr
#from wd30.pythonprogramming.scratch import ClassName
obj1=ClassName(10)#object to refer the instance of the class
print(obj1.class_attr)
print(obj1.funct(1000))
obj2=ClassName(100)#object to refer the instance of the class
print(obj2.class_attr)
print(obj2.funct(1000))
obj3=ClassName(200)#object to refer the instance of the class
print(obj3.class_attr)
print(obj3.funct(1000))
print(obj3.funct2(1000))

ClassName.funct2(1000)

#minimum what we are supposed to know is completed here

# OOP facilitates to have properties & behaviour bundled into an object
# create own objects that has its own methods (behaviour) & attributes (properties)
# Example: object person with properties like a name, age and behaviors such as walking, talking
# class is blueprint for objects. Multiple objects can be created with a class
# creating object from class is called instantiation
# function inside a class is called as methods
class PyClass:
    classattrib = 'Inceptez';  # Class Attribute
    #instance methods
    def mergetext(self, x, y): #I will call the function as a method if the function is a member of a class, and invoked through instance of the class
        # The self first parameter is (mandatory parameter) is a reference to the current instance of the class, and is used to access attributes/methods that belongs to the class.
        return self.classattrib + x + y

obj1 = PyClass(); #creating instance of the class and storing in a object requires () to instantiate
print(PyClass.classattrib) # Without instantitating the class, we can access the class attributes, but not the members
#print(PyClass.mergetext(" tech "," pvt ltd ")) #methods/members/instance methods can't be accessed without instantiating the class
print(PyClass().mergetext(" tech "," pvt ltd ")) #Create an object seperately or inline using () notation.
print(obj1.mergetext(" tech "," pvt ltd ")) # Create an object  seperately and reuse rather than creating objects every time.
print("class attribute is : {}".format(obj1.classattrib));
print("calling concat mergetext - " + str(obj1.mergetext(" Technologies", " Private LTD")))

print("Few items to just know to say that I too know OOPS (c++, java, .net) since they are low level programming lang")
#Concept1 of OOPS: CONSTRUCTORS
#constructor is a methodology used for constructing object from the class
#In python we have 3 types of constructors -> Default constructor, Non Parameterized (Primary Constructor), (Parameterized) Auxilary Constructor
class Def_Constructor_Sal_Bon:#default consturctor (create a class to construct with default value)
    bonus=1000#default constructor variable
    def sal_calc(self,sal):
        return sal+self.bonus

obj1=Def_Constructor_Sal_Bon()#default consturctor
print(obj1.sal_calc(10000))

class Parameter_Constructor_Sal_Bon:#parameterized consturctor (Create a class and construct the object by passing params to the class)
    def __init__(self,bonus,incentive):
        self.bonus1=bonus
        self.incentive1=incentive
    #default constructor variable
    def sal_calc(self,sal):
        return sal+self.bonus1+self.incentive1

#marketing dept
marketing_obj1=Parameter_Constructor_Sal_Bon(2000,1000)#parameterized consturctor
print(marketing_obj1.sal_calc(10000))
print(marketing_obj1.sal_calc(20000))

#sales dept
sales_obj1=Parameter_Constructor_Sal_Bon(3000,2000)#parameterized consturctor
print(sales_obj1.sal_calc(10000))
print(sales_obj1.sal_calc(20000))


class NonParam_Constructor_Sal_Bon:#Non Parameterized consturctor (create a class to construct with non parameters value)
    def __init__(self):
        self.bonus=1000#default constructor variable
    def sal_calc(self,sal):
        return sal+self.bonus

#marketing dept
marketing_obj1=NonParam_Constructor_Sal_Bon()#parameterized consturctor
print(marketing_obj1.sal_calc(10000))
print(marketing_obj1.sal_calc(20000))


print("Different types of class methods/functions")

#just going with FBP, yet to add the OOPS in this program
#adding OOPS feature (class,objects) also in this FBP
#Using this program we can learn about - pkg,subpkg,module,class,members,objects, self keyword, constructors(3 types)
class_tenure=12
def emi(amt):
    return amt / class_tenure

class VehicleTypes:#Default constructor #template or a blue print program will be instantiated or initialized when we create an object
    features=['abs','led lights']#Member Variable of a Class (class variable)
    def suv(self,model,color):#Memeber function of the Class
        colors=['blue','red','black','white']
        if model=='w6':
            self.features.append("reverse camera")
            #print(x)
        elif model=='w8':
            self.features.append("reverse camera")
            self.features.append("auto ignition")
        else:
            self.features.append("reverse camera")
            self.features.append("auto ignition")
            self.features.append("sun roof")
            self.features.append("cruise mode")
        return (model,self.features,colors.count(color)>=1)

    def sedan(self):
        pass

    def hb(self):
        pass

class Accessories:#non parameterized constructor #template or a blue print program will be instantiated or initialized when we create an object
    def __init__(self):
        local_init_var=100
        self.floor_mat_type1=1000+local_init_var #Member Variable of a Class (class variable)
        self.floor_mat_type2 = 1500
        self.viper1 = 1000
    def basic_accessories(self,type):
        local_func_variable=1000#this is not a member of the class, rather its the member of the basic_accessories function
        if type=="mat1":
            return self.floor_mat_type1
        elif type=="mat2":
            return self.floor_mat_type2
        else:
            return self.viper1

class EmiSchemes:
    def __init__(self,tenure):#parameterized constructor
        self.class_tenure=tenure
    def emi(self,amt):#instance method (most of the time)
        return amt/class_tenure
    def down_payment(self,amt):
        return amt-(amt*.5)

print("Different types of class methods/functions")
print("1.Instance method")
class EmiSchemesInstanceMethod:#Regularly used class method in python
    def __init__(self,tenure):#parameterized constructor
        self.class_tenure=tenure
    def emi(self,amt):#instance method
        return amt/self.class_tenure
    def down_payment(siva,amt):
        return amt-(amt*.5)
print("i used siva rather than self")
obj1_instance=EmiSchemesInstanceMethod(36)
print(obj1_instance.emi(100000))#instance method

print("Decorators in Python")
def decorator_function(some_func):
    print("I will decorate the functions passed to me for example a function will be converted to classmethod")
    def some_func(a):
        return a*a
    return some_func

@decorator_function
def add(x):
    print("add two numbers return the result")
    return x

add(10)


print("2.Class method")
class EmiSchemesClassMethod:  # Regularly used class functions in python
    class_tenure = 36
    @classmethod #decorator (I am decorating or adding more functionality to my function)
    def emi(cls, amt):#becomes class method and not a instance method
        return amt / cls.class_tenure
    def down_payment(self, amt):#instance method
        return amt - (amt * .5)

#obj1_instance = EmiSchemesInstanceMethod(36)#not creating an instance of this class, since it is a class method
print(f"Class method {EmiSchemesClassMethod.emi(100000)}")  #Class method (without creating an instance of the class (object), we can still access the method (class)

obj1_instance = EmiSchemesClassMethod()
print(f"Instance method, called without instantiating the class {obj1_instance.down_payment(100000)}")


print("3.Static method")
class EmiSchemesStaticMethod:  # Regularly used class functions in python
    def __init__(self):
        self.class_tenure = 36
    @staticmethod #decorator (I am decorating or adding more functionality to my function)
    def emi(amt):#becomes class method and not a instance method
        #print(self.class_tenure)
        return amt
    def down_payment(self, amt):#instance method
        print(self.class_tenure)
        return amt - (amt * .5)

obj1_instance = EmiSchemesStaticMethod()
print(f"instance method {obj1_instance.down_payment(100000)}")
print(f"Class method {EmiSchemesStaticMethod.emi(100000)}")

#conclusion:
#Instance Method ->
# 1. No decorator is needed (default)
# *2. IM can be accessed by using the object of the class (This method will be accessed only at the time when class is instantiated)
# 3. IM can access all the member variables of the class
#Class Method ->
# 1. decorator is needed (@classmethod)
# *2. CM can be accessed by using the class directly (no need to create an instance of the class, CM can be accesed directly)
# **3. CM can access all the member variables of the class
#Static Method ->
# 1. decorator is needed (@staticmethod)
# *2. SM can be accessed by using the class directly (no need to create an instance of the class, CM can be accesed directly)
# **3. SM cannot access member variables of the class because it is standalone/static

print("************** 10. Detailed OOPS Fundamentals & Features **************")
# data abstraction
# encapsulation
# inheritance
# polymorphism (overloading/overriding)
print("10.1 - Inheritance")
#Single Inheritance - if a child class access a single parent class
#base class
class Father:
    land=2400
    def func1(self):
        print(f"This function is in parent class {self.land}")

# Derived class
class Son(Father):#Inheriting the property of the parent
    house=2000
    def func2(self):
        print(f"Parent purchased the land sqft of {self.land} house constructed by the son with the sqft of {self.house}")

obj1=Son()
obj1.func2()

#Multiple Inheritance - If a child class access from multiple parent classes
class Father:
    land=2400
    def func1(self):
        print("This function is in parent class "+str(self.land))

class Mother:
    amount=10000000
    def func2(self):
        print("This function is in parent class "+str(self.amount))

# Derived class
class Son(Father,Mother):#Inheriting the property of multiple classes
    house=2000
    def func3(self):
        amount = self.amount+100000#overriding
        print(f"Father purchased the land sqft of {self.land} ,mother given me the amount {amount} to Son to construct the house of sqft of {self.house}")

obj1=Son()
obj1.func3()

#Multi Level Inheritance - If a child class access from multiple parent classes
class GrandFather:
    land=2400
    def func1(self):
        print("This function is in parent class "+str(self.land))

class Father(GrandFather):#Single inheritance
    house=1000
    def func2(self):
        print(f"Father constructed house of sqft of {self.house} on the land of the Grandfather {self.land}")

# Derived class
class Son(Father):#Multi Level inheritance
    def func3(self):
        extend_house = self.house+1000
        print(f"Grandfather purchased the land sqft of {self.land} , father constructed house {self.house},  Son extended the house of sqft of {extend_house}")

obj1=Son()
obj1.func3()