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