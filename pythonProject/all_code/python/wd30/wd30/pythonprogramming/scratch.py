#1. Define a class
#2. Define member of the class (passing self arg)
#3. Import the pkg.subpkg.module.class
#4. instantiate the class by creating object
#5. access the members of the class using object.membername
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
obj1=Calc()#instance of the class (is an object)
print(obj1)
print(obj1.add(100,200))#instance method
print(obj1.add(100,200))