print("************** 9. Classes, Functions & Methods **************")
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

print("************** 10. OOPS in Python: ****************")
print(" ************** A) Polymorphism (Function Overloading doesn't support directly in Python, can be achieved using arbitrary or arbitrary keyword args functions) **************")
# Polymorphism is creating an similar named entity to represent different behaviours at different scenarios
# method names are same but purposes are different for different scenarios
# polymorphism can be applicable at built-in function levels as well. "len" is a classic example
# "len" can be applied on string, list, dictionary, etc. function name is same but usage is different as per data type

#Function Overloading with difference in type of arguments
print("User defined function with same name but different in data type")
print("Duck typing")
#Using Duck Typing, we do not check types at all. Instead, we check for the presence of a given method or attribute
#“If it looks like a duck and quacks like a duck, it’s a duck”

print("Builtin function with same name but different in data type (Duck typing)")
print(len("Python"))
print(len(["Python", "Java", "Scala"]))
print(len({"Name": "Inceptez", "City": "Chennai"}))

def calc(a,b,c):return a+b+c
print(calc(10,20,30))
print(calc("inceptez ","tech"," pvt ltd"))

print("Ducktyping + Function Overloading with difference in number of arguments + Closure example")
lst = [5,4,2,3,1] #Closure
def func1() :
    if lst.count(6) == 1:
        print("method if block")
    elif lst.count(5)==1:
        print("method elif block")
    else:
        print("method else block")

func1()

def func1(arg1:int):
  print ("method with one int argument")
#func1()# will not work
func1(10)


print(" ************** B) Inheritance **************")

print("Single Inheritance")

# Base class
class Parent:
    parents="mom,dad"
    def func1(self):
        print("This function is in parent class "+self.parents)

#class Child extends Parent - scala syntax
# Derived class
class Child(Parent):
    childrens="child1,child2"
    def func2(self):
        print("This function is in child class "+self.parents+" property accessed by childs -"+self.childrens)


# Driver's code
object = Child()
print(object.parents)
print(object.childrens)
object.func1()
object.func2()

# multiple inheritance
print("Multiple Inheritance")
# Base class1
class Mother:
    mothername = "X"

# Base class2
class Father:
    fathername = "Y"

# Derived class
class Son(Mother, Father):
    def parents(self):
        print("Father name attribute is ", self.fathername)
        print("Mother name attribute is ", self.mothername)

# Driver's code
s1 = Son()
s1.parents()


print("Multilevel Inheritance")
# Base class1
class grandpaclass:
    grandpaname = "A"

# Base class2
class fatherclass(grandpaclass):
    fathername = "B"

# Derived class
class myclass(fatherclass):
    myname = "C"
    def myfamilyhierarchy(self):
        print("My Grandfather is "+ self.grandpaname + " My father is " + self.fathername + " My self is " + self.myname)
# Driver's code
objmyself = myclass()
objmyself.myfamilyhierarchy()


print(" ************* C) Encapsulation is a way of restricting access to methods inside a class *************")
# As python is an interpreted language ; hence encapsulation is weak
# Restriction levels: private, protected, public

class Base:

    def __private(self): #Scope within this class
        print("private value in Base")

    def _protected(self):
#Protected variables are those data members of a class that can be accessed within the class and the classes derived from that class
        print("protected value in Base")

    def public(self):
        print("public value in Base")
        self.__private()


class Derived(Base):

    def _protected(self):
        print("protected value in Derived")


d = Derived()
d.public()
# not recommended to use/call protected method
d._protected()
# private methods - we won't be able to call as python doesn't recognize "AttributeError: 'Derived' object has no attribute '__private'"
#d.__private()

e = Base()
e.public()
e._protected()
# private methods - we won't be able to call as python doesn't recognize "AttributeError: 'Derived' object has no attribute '__private'"
#e.__private()


print(" ************* 11. Constructors & Types of methods in Python (Instance method and Static Method) *************")
# __init__ is a special method that is called when instance of class [object] is created
# self allows access to the attributes and methods of each object in python
# class attribute is going to defined outside __init__ method
# instance attribute are defined within __init__ method

#scala
#class Asia(country:String) {
# def this(){ Asia("India") } #aux constructor in scala, but don't have it in python
# val obj1=new Asia(); val obj1=new Asia("Singapore");
print("Constructor python will provide a default constructor if no constructor is defined")
print("Constructor without any arguments is called a non-parameterized constructor. "
      "This type of constructor is used to initialize each object with default values.")
print("Constructor with defined parameters or arguments is called a parameterized constructor. "
      "We can pass different values to each object at the time of creation using a parameterized constructor.")
class Asia:
    # Instance Method
    # 1. Must have self parameter
    # 2. Most commonly used
    # 3. Can be accessed through objects (instance of the class)
    country="" #paramenter for class
    #def __init__(self, argument1):
    #    self.country = argument1 # Instantiating and setting with the parameter passed using contructor (parameterized constructor)
    #def __init__(self): # __init__ is a special method that is called when instance of class [object] is created
    #    self.country="India" #non parameterized constructor
    def __init__(self, argument1): #used for passing arguments to the class and for creating constructors (parameterized/non parmeterized)
        self.country = argument1 # Instantiating and setting with the parameter passed using contructor (parameterized constructor)
    def capital(self):
        print("New Delhi is the capital of {}".format(self.country))

    def language(self):
        print("Hindi is widely spoken language in {}".format(self.country))

    def type(self):
        print("{} is a developing country".format(self.country))

    @staticmethod
    #decorator - For Static method
    # 1. No need to create an object as it is a static function just works with class reference itself
    # 2. Cannot refer the class attributes, only can pass paramenter for eg. self.country cant be accessed as like the Instance Methods
    def static_method_example(param1):
        print ("static method not using any attributes " + param1 ) # can't use self.country reference

#asiancountries = Asia()
#asiancountries.capital()
#asiancountries.language()
asiancountries = Asia("India")
#asiancountries = Asia("Singapore")

asiancountries.capital()
asiancountries.language()
#Asia.capital() # Error occurs as it is not a static method, it is a instance method, it needs object reference
asiancountries.type()
asiancountries.static_method_example("called using the object reference")
Asia.static_method_example("called using the class reference") #No need to create an object as it is a static function just works with class reference itself

#Decorators:
def df(x):
    print("inside decorator")
    return x

@df
def fun1(a,b):
    print("inside fun1 function")
    return a+b

print(fun1(10,20))

def df(x):
    print("inside decorator")
    def f2():
        return x()+100
    print(f2())
    return x

@df
def fun1():
    print("inside fun1 function")
    return 10

print(fun1())
