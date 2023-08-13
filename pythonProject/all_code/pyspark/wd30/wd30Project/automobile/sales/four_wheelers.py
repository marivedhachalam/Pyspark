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
def decorator(some_func):
    print("I will decorate the function passed to me")
    return some_func

@decorator
def add(x,y):
    print("add two numbers return the result")
    return x+y

add(10,20)


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
# *3. CM can access all the member variables of the class
#Static Method ->
# 1. decorator is needed (@staticmethod)
# *2. SM can be accessed by using the class directly (no need to create an instance of the class, CM can be accesed directly)
# *3. SM cannot access member variables of the class because it is standalone/static
