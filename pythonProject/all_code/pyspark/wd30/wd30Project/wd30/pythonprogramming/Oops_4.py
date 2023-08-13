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
    return x+x

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

#Hierarchical Inheritance - If we have multiple derived/child classs access from one parent classes

class Father:
    land=1000
class Son(Father):
    house=500
class Daughter(Father):
    house=500

obj1=Son()
print(f"{obj1.house} sqft house constructed by son in father land of {obj1.land}")
obj2=Daughter()
print(f"{obj2.house} sqft house constructed by daughter in father land of {obj2.land}")

# Multiple+Hierarchical Inheritance (Diamond methodology)- If we have multiple derived/child classs access from one parent classes
class Father:#A
    land=1000
class Son(Father):#B->A(hierarchical)
    house1=500
class Daughter(Father):#C->A(hierarchical)
    house2=500
class GrandSons(Son,Daughter):#D->B,C(Multiple)
    gson1usage=500
    gson2usage=500

obj1=GrandSons()
print(f"Usage by grandson1 {obj1.gson1usage} of {obj1.house1} sqft house constructed by father in grandfathers land of {obj1.land}")
obj2=GrandSons()
print(f"Usage by grandson2 {obj2.gson2usage} of {obj2.house2} sqft house constructed by mothers in grandfathers land of {obj2.land}")

#Can I have parameterized constructor for the parent class?we can achieve

class Calc:
    def __init__(self,abc):
        self.abc=abc
    def add(self,x,y):
        print(self.abc)
        return x+y
    def sub(self,x,y):
        print(self.abc)
        return x-y

from wd30.pythonprogramming.scratch import *
obj1=Calc(10)#instance of the class (is an object)
print(obj1)
print(obj1.add(100,200))#instance method
print(obj1.add(100,200))

print("10.2 - Poly-morphism (Overloading & Overriding)")
print("10.2.1 - Overloading (Python directly don't support overloading)- Function with same name behave differently based on the number or type of arguments")
class cls1:
    def func1(self,*args):
        if (len(args)>1):
            return args[0]+args[1]#sum of 2 input values
        else:
            return args[0]*args[0]


obj1=cls1()
print(f"sum of 2 numbers {obj1.func1(10,20)}")
obj1.func1(10)
print(f"sqrt of a number {obj1.func1(10)}")

#scala
'''
class a{
     | def f1(a:Int,b:Int):Int={a+b}
     | def f1(a:Int):Int={a*a}
     | }
val obj1=new a()
scala> obj1.f1(10,20)
res3: Int = 30

scala> obj1.f1(10)
res4: Int = 100
'''

print("10.1.2 - Overriding - ")
print("Poly-morphism")
print("Overriding - Function with same name across the different classes is a overrided function to enhance or extend the given function")
class Father:#base class
    land=1000
    def func1(self,*args):
        if (len(args)>1):
            return args[0]+args[1]#sum of 2 input values
        else:
            return args[0]*args[0]

class Son(Father):#derived class
    def func1(self,*args):#Overrided function
        if (args[0]=='a'):
            return args[1]+args[2]#sum of 2 input values
        elif (args[0]=='m'):
            return args[1]*args[2]#mul of 2 input values
        elif (args[0]=='d'):
            return args[1]/args[2]#div of 2 input values
        else:
            return args[1]*args[1]#sqrt of input value

obj1=Son()
print(f"sum of 2 numbers {obj1.func1('a',10,20)}")
print(f"sqrt of a number {obj1.func1('s',10)}")
print(f"son wanted to use the land of father without overriding {obj1.land}")

print("10.3 - Abstraction")

#A company is manufacturing a product, each and every version of the product may have differnt feature + native features?
#Eg. Samsung manufacturing an AC remote - mandatory buttons on/off,temp control (Any AC), optional buttons (clean, turbo, heater) (Type of AC)
from abc import ABC,abstractmethod
class Common(ABC):#Abstract Class
    remote_body='plastic'
    @abstractmethod
    def power(self,input):#No implementation of logic, because this is an abstract method
        pass
    #@abstractmethod
    def temp(self,input):#No implementation of logic, since it is just a normal method, no need of implementation by the child class
        pass
'''        if input=='on':
            return True
        else:
            return False
'''
class AllWeather(Common):#Restricting all the derived class to mandatorily implement the abstract method of the base class.
    def power(self, input):
    #A function defined in the parent, but (abstracted) not implemented with any functionalities, so the inherited class can have their own functionality
            if input=='on':
                return True
            elif input=='long':
                return 'restart'
            else:
                return False
    def heater(self,temp):
        return temp

class Inverter(Common):
    def power(self, input):
        # A function defined in the parent, but (abstracted) not implemented with any functionalities, so the inherited class can have their own functionality
        if input == 'on':
            return True
        else:
            return False
    def cleaner(self,input):
        return input

obj1=AllWeather()
print(f"switched to {obj1.power('off')}")
print(f"temp is set to {obj1.heater(33)} degree")

obj2=Inverter()
print(f"switched to {obj2.power('off')}")
print(f"calling cleaner function {obj2.cleaner('clean')}")


'''
    def power(self, input):
#A function defined in the parent, but (abstracted) not implemented with any functionalities, so the inherited class can have their own functionality
        if input=='on':
            return True
        else:
            return False
'''

print("10.3 - Encapsulation - Access Controls in the form of access specifiers such public, private, protected")
#For eg: If I a car is in a showroom and I am planning to buy a car
#Access control -> public - anyone can see, touch the car, feel the car without buying it
#Access control -> protected - i bought the the car, all public + start car with key/button + drive car + own car
#Access control -> private - i bought the the car, I don't have access to know how the immobilizer (security system) works,
# only the manufacturer knows it
#Encapsulation achived with the help of Access controls in python using notation of without any underscores (public))
# with one _ for protected and with __ for private
class CarCompany:#parent class
    car_showroom='xuv1'#public member - anyone or child (buyer)
    _car_sold = 'xuv2'#protected member - child class (buyer) (in python dynamically type language - even public can access the protected member)
    __car_security = ' hidden security system'#private member - parent class (manufacture)
    _key='wrap'+__car_security

PubliccarCompanyObj1=CarCompany()#public access controls
print(PubliccarCompanyObj1.car_showroom)
#print(PubliccarCompanyObj1._car_sold)#(only the child class has to access the protected member) buy in python dynamically type language - even public can access the protected member
#print(PubliccarCompanyObj1.__car_security)#only in python dynamically type language - even public can access the protected member

class Buyer(CarCompany):#inherited child class
    accessories='suncool stickers'
print("lets see how encapsulation works with the understanding of the access control the child class has on the parent class members")
buyerObj1=Buyer()#instantiating the Buyer class to create an object
print(buyerObj1.car_showroom)
print(buyerObj1._car_sold)#child class can able to access the protected members of the parent class
print(buyerObj1._key)#private members of the parent class will not exposed to derived/child or direct object also


class Father:#parent class
    road='everyone'#public member - anyone or child (buyer)
    _inside_house = 'accessed by members of the house (son/mother/maid)'#protected member - child class (buyer) (in python dynamically type language - even public can access the protected member)
    __locker_system = 'money of 1000000 only accessed by the father, if needed he can expose to the members'#private member - parent class (manufacture)
    _money=__locker_system+str(100000)

PubliccarCompanyObj1=Father()#public access controls
print(PubliccarCompanyObj1.road)
#print(PubliccarCompanyObj1._inside_house)#(only the child class has to access the protected member) buy in python dynamically type language - even public can access the protected member
#print(PubliccarCompanyObj1.__car_security)#only in python dynamically type language - even public can access the protected member

class Son(Father):#inherited child class
    accessories='suncool stickers'
print("lets see how encapsulation works with the understanding of the access control the child class has on the parent class members")
buyerObj1=Son()#instantiating the Buyer class to create an object
print(buyerObj1.road)
print(buyerObj1._inside_house)#child class can able to access the protected members of the parent class
print(buyerObj1._money)#private members of the parent class will not exposed to derived/child or direct object also

