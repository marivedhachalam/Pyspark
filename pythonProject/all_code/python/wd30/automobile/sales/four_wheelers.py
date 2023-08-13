#just going with FBP, yet to add the OOPS in this program
#adding OOPS feature (class,objects) also in this FBP
#Using this program we can learn about - pkg,subpkg,module,class,members,objects, self keyword, constructors(3 types)
x=100
class VehicleTypes:#Default constructor #template or a blue print program will be instantiated or initialized when we create an object
    features=['abs','led lights']#Member Variable of a Class (class variable)
    def suv(self,model,color):#Memeber function of the Class
        colors=['blue','red','black','white']
        if model=='w6':
            self.features.append("reverse camera")
            print(x)
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
    def emi(self,amt):
        return amt/self.class_tenure
    def down_payment(self,amt):
        return amt-(amt*.5)

print("Different types of class methods/functions")
print("1.Instance method")
class EmiSchemesInstanceMethod:#Regularly used class functions in python
    def __init__(self,tenure):#parameterized constructor
        self.class_tenure=tenure
    def emi(self,amt):
        return amt/self.class_tenure
    def down_payment(self,amt):
        return amt-(amt*.5)

obj1_instance=EmiSchemesInstanceMethod(36)
obj1_instance.emi(100000)#instance method

print("2.Class method")
class EmiSchemesClassMethod:  # Regularly used class functions in python
    class_tenure = 36
    @classmethod #decorator (I am decorating or adding more functionality to my function)
    def emi(cls, amt):#becomes class method and not a instance method
        return amt / cls.class_tenure
    def down_payment(cls, amt):
        return amt - (amt * .5)


obj1_instance = EmiSchemesInstanceMethod(36)
obj1_instance.emi(100000)  # instance method
print("2.Static method")
