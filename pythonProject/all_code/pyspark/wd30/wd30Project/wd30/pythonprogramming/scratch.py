print("Poly-morphism")
print("Overriding - Function with same name across the different classes is a overrided function to enhance or extend the given function")
class Father:#base class
    a=100
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

