
def generate_mailid(fname:str,lname:str):
    mailid=fname+lname+'@inceptez.com'
    #print(mailid)
    return mailid#return help us return the value to the calling environment that is derived inside the function

print("A. different function calling methodologies using different number, position, name and type of arguments?")
#1.call by position (positional arguments fuctions)
print(generate_mailid('mohamed','irfan'))
print(generate_mailid('irfan','mohamed'))
#2.call by name (named/keyword arguments fuctions)
print(generate_mailid(lname='irfan',fname='mohamed'))
#3.call with default values (default argument functions)
def generate_mailid(fname:str,lname:str,domain='@inceptez.com'):
    mailid=fname+lname+domain
    #print(mailid)
    return mailid
print(generate_mailid('raj','kumar'))#positional default arg function call
print(generate_mailid(lname='kumar',fname='raj'))#named default arg function call
print(generate_mailid(lname='kumar',fname='raj',domain='@gmail.com'))#override default arg function call

#4. Arbitrary (dynamic/unknown/different) argument function
#if we wanted to call a function with different number of arguments which is not fixed while creating the functions
#using positional arguments, we can't use named/keyword arguments
print("Arbitrary (dynamic/unknown/different numbers/random) argument function")
def generate_mailid(*args):
    print(args)
    print(f"the type of arguments passed is {type(args)}")
    print(f"the number of arguments passed is {len(args)}")
    print(f"how to access the argument values, by using index {args[0]} , {args[1]}")
    if len(args)==2:
        return args[0]+args[1]+'@inceptez.com'
    else:
        return args[0] + args[1] + args[2]

print(generate_mailid('mohamed','irfan'))#calling only by positional, cannot be called by name
print(generate_mailid('mohamed','irfan','@gmail.com'))
print(generate_mailid('mohamed','irfan','@gmail.com','a','b','c'))

#literal example for going with the arbitrary argument function?
#sal+incentive+bonus
#cts -> sal+incentive
#wipro -> sal+i+b
#infy -> sal
#hrworkways -> payroll processing

def sal_bon_inc(*comp_amount):
    print(type(comp_amount))
    if comp_amount[0]=='infy':
        return comp_amount[1]
    elif comp_amount[0]=='cts':
        return comp_amount[1]+comp_amount[2]
    elif comp_amount[0]=='wipro':
        return comp_amount[1] + comp_amount[2]+comp_amount[3]

print(sal_bon_inc('infy',10000))#why the type for arbitrary arg function is tuple
print(sal_bon_inc('wipro',10000,1000,2000))
print(sal_bon_inc('cts',10000,1000))

#5. Arbitrary Keyword (named) argument function
#if we wanted to call a function with different number of arguments which is not fixed while creating the functions
#by using named/keyword arguments

print("Arbitrary (dynamic/unknown/different numbers/random) keyword (named) argument function")
def generate_mailid(**kwargs):
    print(kwargs)
    print(f"the type of arguments passed is {type(kwargs)}")
    print(f"the number of arguments (k,v) passed is {len(kwargs)}")
    fname=kwargs["fname"]
    lname = kwargs["lname"]
    print(f"how to access the argument values, by using the key {fname} , {lname}")
    domain=kwargs.get("domain")
    if domain!=None:
        return fname + lname+domain
    else:
        return fname + lname

print(generate_mailid(fname='mohamed',lname='irfan'))#calling by positional
print(generate_mailid(lname='irfan',fname='mohamed',domain='@gmail.com'))
#print(generate_mailid('mohamed','irfan','@gmail.com','a','b','c'))

#based on the functionality we have to pass the arguments as we don't know the position
def sal_bon_inc_arb_keyword(**comp_amount):
    print(type(comp_amount))
    if comp_amount["comp"]=='infy':
        return comp_amount["sal"]
    elif comp_amount["comp"]=='cts':
        return comp_amount["sal"]+comp_amount["bonus"]
    elif comp_amount["comp"]=='wipro':
        return comp_amount["sal"]+comp_amount["bonus"]+comp_amount["incentive"]

print(sal_bon_inc_arb_keyword(sal=10000,incentive=1000,bonus=2000,comp='wipro'))
print(sal_bon_inc_arb_keyword(sal=10000,incentive=1000,bonus=2000,comp='cts'))
print(sal_bon_inc_arb_keyword(sal=10000,comp='infy',incentive=1000,bonus=2000))


print("B. Global and Local variables in Function Based Programming")
#************By default every variable defined outside the functions are global
#what is global means -> if the variable defined outside the function has the scope inside or outside the function also
#we can explicitly define a variable as global (not mandatory) in a program
#global varname (not mandatory if we use outside the function)
#varname=value
#************In the other hand, By default every variable defined inside the function is LOCAL,
# in order to convert into global we have to explicitly mention global (mandatorily)
global_variable_by_default=100
print(f"I am a variable defined outside the function, by default I am global - {global_variable_by_default}")
localvar='Tech'
def f2():
 global mycompname
 mycompname='Inceptez'
 localvar='Technologies'
 return localvar#regular way of bring the value outside the function

print(localvar)#this prints Tech and it won't print Technologies
f2_returned_localvar=f2()#if i want to get Technologies?call the function and assign the return to a variable
print(f2_returned_localvar)#this prints Technologies

print(f"I am a variable defined inside the function, explicitly has to be defined as global (then only we can access outside the functio) - {mycompname}")
#print(f"I am a variable defined inside the function as local - {localvar}")

print("C. Different types of functions in python?")
#6. Closure Function in Python (The scope of the global variable defined outside the function, impacts the result of the function)
#If a result of the given function is affected with the value defined outside the function
bonus=1000
def bonus_sal(sal):#this is a closure function because bonus defined outside the function is impacting the function result
    return sal+bonus
#why we call it as closure - if the function has a close impact with the value defined/derived outside of the function
print(f"bonus applied salary is {bonus_sal(10000)}")

#7. Higher Order Functions:
#Type1: If a function returns another function, then it is HOF
#Type2: If a function takes an input argument as another function, then it is HOF
def calc(n1,n2,op):
    if op=='a':
        return (op,n1+n2)# A function can return any type
    elif op=='s':
        return (op,n1-n2)
    elif op=='m':
        return (op,n1*n2)
    else:
        return (op,n1/n2)

result=calc(10,20,'a')
print(result[0],result[1])

def calc(op):#A function can return another function (HOF)
    if op=='a':
        def add(n1,n2):
            return n1+n2
        return add
    elif op=='s':
        def sub(n1, n2):
            return n1 - n2
        return sub
    elif op=='m':
        def mul(n1,n2):
            return n1*n2
        return mul
    else:
        def div(n1,n2):
            return n1/n2
        return div

add_func=calc('a')
type(add_func)
add_func(10,20)

def scientific_calc(mode):#A function can return another function (HOF)
    if mode=='normal':
        def add(n1,n2):
            return n1+n2
        def sub(n1, n2):
            return n1 - n2
        def mul(n1,n2):
            return n1*n2
        def div(n1,n2):
            return n1/n2
        return (add,sub,mul,div)
    elif mode=='scientific':
        pass
        #define some functions to calculate sin/cos/tan etc.,

#Challenge1: Create a scientific calculator using hof part1 concept

#Type2: If a function takes an input argument as another function, then it is HOF
#question? What are the input argument types we can pass to a function? Any type/functions
sal=[10000,20000,30000,50000]
print(sum(list(map(lambda x:x+1000,list(filter(lambda x:x>10000,sal))))))

sal=[10000,20000,30000,50000]
bonus=1000
def bonus_sal(sal):
    return sal+bonus

print(list(map(bonus_sal,sal)))#this is a simple higher order function type2 concept
#map is a function take another function bonus_sal as an input, so map is a higher order function

#Challenges2: Create a normal calculator using hof part2 concept


#create a scientific calculator function,
# where if we chose mode='normal' -> add,sub,mul,div
#

#what are the return types of a function?
#a function can return anytype, any functions





#bonus=1000
def bonus_sal(sal):#this is a closure function because bonus defined outside the function is impacting the function result
    sal_bonus=sal+1000
    def tax_sal(tax):#closure function
        tax_calculated=sal_bonus-(sal_bonus*tax)
        return tax_calculated
    return tax_sal
