#Function Based Programming****
print("What is FBP & Why FBP is needed")
# Simple/elegant/high-level, rather than writing 100 lines, we can achive the same result in 1 or 2 lines code
#Help us modularise/ create the entire application as a plug and play components of codebase
#Help us for reusability
#Help us calling concurrently, parallelly, distributed (with the help of spark kind of framework)
#Help us create Frameworks to bring more uniformity/coding standard, centralized controllability and organized way of managing/using code base
#Using generic common programming model as like how other languages uses (c,c++,java,c#)
#What we are learning:
#Fundas of FBP -> What is FBP, why FBP, How create functions,how to call function (positional, named, default args), arbitrary args, arbitrary keyword
#argument function
#Types of Functions -> Closures, Higher Order Functions,Anonymous/Lambda Functions, Hierarchical/Nested Functions,  Recursive Functions

def fun1():
    sum1=0
    lst=[10000,20000,30000,50000]
    for i in lst:
        if i>10000:
            sal=i
            bonus_sal=sal+1000
            sum1=sum1+bonus_sal
            print(sum1)

#using FBP
def fun2():
    sal=[10000,20000,30000,50000]
    print(sum(list(map(lambda x:x+1000,list(filter(lambda x:x>10000,sal))))))
    #print(sum([21000,31000,51000]))
#fun2()
#fun1()

print("1. How to Create/define a Function (syntax & symantics)")
#naming convention & syntax
#start with def keyword
#use lowercase with _ for the name of the functions
#close the definition with () and following with : that help us start the body of the program
#write atleast 1 line of code in the body of the function (if no code to write then type pass)
#minimum function syntax
def func_name():
    pass#minimum one line of code is needed
#minimum way to call the function
print("calling the funciton")
func_name()
#optimal function syntax
def generate_mailid(fname:str,lname:str):
#type definition is not mandatory since python dynamic inference property will identify the type automatically
#dynamically typed feature of python automatically change the type if you pass any other type rather than string
    mailid=fname+lname+'@inceptez.com'#body of the function
    print(mailid)#print help us print the output in the console, mostly used for debugging or testing or development purpose
    return mailid#return help us return the value to the calling environment that is derived inside the function
    #print(mailid)
#print(mailid)
#calling environment
mailid_generated1=generate_mailid('mohamed','irfan')
mailid_generated2=generate_mailid('Vinoth','Selvam')

#mail_id('md','irfan','@inceptez.com')

#Real life simple scenario - create a function to take 3 input args given below and calculate the
#Input arguments are -
# Total cart amount in int, discount percentage 10 in int, max_offer_amount int =50
#Output to return
#offer_applied_amount
#swiggy -> total 800,offer 10 , max offer 50 rs (default)
#swiggy_offer(800,10,50) -> 800-50=750
#swiggy_offer(400,10,50) -> 400-40=360
#800-50=750
#400-40=360
#dynamic pricing
#promo campaign
#shopping cart
#business logic
#apply offer percent to the cart amount and get the calculated offer amount
# if the calculated offer amount exceeds the maximum offer amt then use the maximum offer amout
# else use the calculated amount

#I have hard coded discount amt 10% and the max discount limit 50rs.  shopping cart amount is input from the user (Closure)
#i gave the discount and max price as default, mobile application can change also. (Defaut Argument Function)
#it could be possibly from file or tables also

#1. how to approach writing this code logic (as a pseudo code then with the actual code)
total_cart_amount=800
discount_percentage=10
max_offer_amount=50
#hardcoded logic
disc_percentage_in_float=discount_percentage/100
calculated_offer_amount=total_cart_amount*disc_percentage_in_float
if max_offer_amount<=calculated_offer_amount:
    offer_to_apply=max_offer_amount
    print(f"applying the company's offer amount of {offer_to_apply}")
else:
    offer_to_apply = calculated_offer_amount
    print(f"applying the calculated offer amount of {offer_to_apply}")

total_offer_applied_amount=total_cart_amount-offer_to_apply
print(f"Total amount to pay after applying the offer is {total_offer_applied_amount}")

#Convert the above code as a generic reusable function:
#total_cart_amount=800

#coupon=input("enter the coupon")
#discount_percentage=avail_coupon(coupon)
#from pythonpkg.swiggy.offers import *

def avail_coupon(coupon):
    if (coupon=='summer10'):
        discount_percentage=10
    else:
        discount_percentage =5
    return discount_percentage

coupon='summer10'
#global max_offer_amount
max_offer_amount=50
#discount_percentage = avail_coupon(coupon)
def promo_campaign_dynamic_pricing_func(total_cart_amount,discount_percentage=10):#pass the arguments as input argument by having some default value set also
    #discount_percentage = avail_coupon(coupon)#derive the required arguments by using some programs/file/table
    disc_percentage_in_float = discount_percentage / 100
    calculated_offer_amount = total_cart_amount * disc_percentage_in_float
    if max_offer_amount <= calculated_offer_amount:
        offer_to_apply = max_offer_amount
        print(f"applying the company's offer amount of {offer_to_apply}")
    else:
        offer_to_apply = calculated_offer_amount
        print(f"applying the calculated offer amount of {offer_to_apply}")
    total_offer_applied_amount = total_cart_amount - offer_to_apply
    print(f"Total amount to pay after applying the offer to input amt {total_cart_amount} is {total_offer_applied_amount}")
    return total_offer_applied_amount

cust1=promo_campaign_dynamic_pricing_func(800,5)
cust2=promo_campaign_dynamic_pricing_func(300,15)
cust3=promo_campaign_dynamic_pricing_func(1000)

#Real life scenario ends here

print("How to Calling functions:")
def generate_mailid(fname:str,lname:str):
#type definition is not mandatory since python dynamic inference property will identify the type automatically
    mailid=fname+lname+'@inceptez.com'
    print(mailid)
    return mailid#return help us return the value to the calling environment that is derived inside the function
print("2. Positional Arguments")
mailid_generated_positional=generate_mailid('mohamed','irfan')

print("3. Named/Keyword Arguments")
print(generate_mailid(lname='irfan',fname='mohamed'))

print("3.1. Default Arguments functions")
def generate_mailid(fname:str,lname:str,mailid='@inceptez.com'):
  mailid1=fname+lname+mailid
  print(mailid1)
  return mailid1
print(generate_mailid(mailid='@gmail.com',lname='irfan',fname='mohamed'))


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
#simple concept (HOF type1)
def f3():#if it is not HOF, then how do we execute f4() Irfan while calling f3?
    def f4():
        return 'hello'
    return f4()#result of the function is returned, hence this is not a HOF

def f3():
    def f4():
        return 'hello'
    return f4#function is returned, hence this is a HOF

f4_func=f3()#HERE f3 is executed to define f4 and return f4 as a function and not a value
print(type(f4_func))#HERE f4 is executed to get the resultant value
print(f4_func())

#simple concept (HOF type2)
def f1():
 return 100
def f2(a):#This is a HOF, Because we are passing a function as an input argument
 return a()

print(f2(f1))

##############simple example of hof completed here##########################

####Application of HOF######################

def calc(n1,n2,op):
    if op=='a':
        return (op,n1+n2)# A function can return any type of values
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
        return (add,sub,mul,div)#we are returning udfs
    elif mode=='scientific':
        import math
        return (math.sin,math.cos)#we are returning builtin functions

calctype=scientific_calc('scientific')
print(f"sin value of the given input is {calctype[0](90)}")
print(f"cos value of the given input is {calctype[1](90)}")

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

print(list(map(bonus_sal,sal))) #this is a simple higher order function type2 concept
#map is a function take another function bonus_sal as an input, so map is a higher order function

#Challenges2: Create a normal calculator using HOF part2 concept
def calc(func,a,b):#A function can return another function (HOF)
    return func(a,b)

def gst_func(n1,p1):
    return n1*(p1/100)

def state_tax_func(n1,p1):
    return n1+(n1*(p1/100))

def calc(n1,n2,op,custom_func=gst_func):#Simple calculator function is changed to HOF (type2) by adding custom_func as input arg
    if op=='a':
        return (op,n1+n2)# A function can return any type of values
    elif op=='s':
        return (op,n1-n2)
    elif op=='m':
        return (op,n1*n2)
    elif op == 'd':
        return (op,n1/n2)
    else:
        return custom_func(n1,n2)

#calc is purchased by a company in India
print(calc(100,200,'a'))
print(f" GST function passed as HOF {calc(10000,18,'p',gst_func)}")
#calc is purchased by a company in US
print(f" STATE function passed as HOF {calc(10000,8,'p',state_tax_func)}")

print("8. BuiltIn Function or Predefined Functions - Any functions are already developed and can be consumed at anytime with predefined generic logics")
#prefer more of builtin/predefined functions (for better optimization) rather than creating a UDF
#only go with UDF if it is "in-evitable" - "un avoidable"
def map_udf(func,iter):#A function can return another function (HOF)
    lst=[]
    for i in iter:
        lst.append(func(i))
    return lst

sal=[10000,20000,30000,15000]
bonus=1000

def bonus_func(sal):#Closure function
    return sal+bonus

print(f"HOF using Builtin function Bonus applied salary is {list(map(bonus_func,sal))}") #Builtin function with HOF
print(f"HOF using Builtin function Bonus applied salary using the calculator function {list(calc(bonus_func,sal,'irfan',map))}") #Builtin function with HOF
print(f"HOF using UDF Bonus applied salary is {map_udf(bonus_func,sal)}")

#create a scientific calculator function,
# where if we chose mode='normal' -> add,sub,mul,div
#

#what are the return types of a function?
#a function can return anytype, any functions

#bonus=1000
def bonus_sal(sal):#this is a HOF & closure function because bonus defined outside the function is impacting the function result
    sal_bonus=sal+1000
    def tax_sal(tax):#closure function and Nested Functions
        tax_calculated=sal_bonus-(sal_bonus*tax)
        return tax_calculated
    return tax_sal


print("9. Lambda Functions/Anonymous Functions/Function Variables/Function Literals ")
#Always should I have define a function with def keyword in a formal way
#simple lambda function
add1=lambda a,b:a+b
print(add1,100,200)
#complex lambda function using ternary operator-Not advisable, rather go with def functions
calc1=lambda op,a,b:a+b if op=='a' else (a-b if op=='s' else (a*b if op=='m' else a/b))
print(calc1('a',100,200))

#Real life example of Lambda functions
#Requirement: apply bonus for all the 3 department employees?
#We have 3 departments to calculate bonus for their employees?
#Case1: preferred
#In general all regular def functions are kept inside some common modules in some packages
from pythonpkg.swiggy.common_salary_functions import bonus_func
#I will make of the existing functions from some custom/predefined libraries if it is available
company_bonus=1000
it_sal=[100000,200000,300000,150000]
print(bonus_func(it_sal,company_bonus))
mkt_sal=[90000,80000,50000,15000]
print(bonus_func(mkt_sal,company_bonus))
hr_sal=[50000,70000,60000,15000]
print(bonus_func(hr_sal,company_bonus))

#Case2: preferred if case1 is not applicable
#if this function is not kept inside some common modules in some packages,
# then create one using either using regular def functions or lambda functions
#from pythonpkg.swiggy.common_salary_functions import bonus_func#not available
#I will create a def function in this program itself using def if it is complex
def bonus_func(sal_lst,bonus): #if the logic is complex, which can't be implemented in lambda easily
    bonus_sal_lst=[]
    for i in sal_lst:
        bonus_sal_lst.append(i+bonus)
    return bonus_sal_lst

company_bonus=1000
it_sal=[100000,200000,300000,150000]
print(bonus_func(it_sal,company_bonus))
mkt_sal=[90000,80000,50000,15000]
print(bonus_func(mkt_sal,company_bonus))
hr_sal=[50000,70000,60000,15000]
print(bonus_func(hr_sal,company_bonus))

# if it is simple i will create it ananymously as a lambda function (advisable)
#Lambda function also create in a common library and call it any where right ? We can do, but not preferred
def bonus_func(sal,bonus):#this is preferred?not preferred, because it is formal function
    return sal+bonus
bonus_func=lambda sal,bonus:sal+bonus#this is preferred?because anonymous and going to be used only in this program not accross the application
company_bonus=1000
it_sal=[100000,200000,300000,150000]
bonus_sal_lst=[]
for i in it_sal:
    bonus_sal_lst.append(bonus_func(i,company_bonus))
print(bonus_sal_lst)
mkt_sal=[90000,80000,50000,15000]
bonus_sal_lst=[]
for i in mkt_sal:
    bonus_sal_lst.append(bonus_func(i, company_bonus))
print(bonus_sal_lst)
hr_sal=[50000,70000,60000,15000]
bonus_sal_lst=[]
for i in hr_sal:
    bonus_sal_lst.append(bonus_func(i, company_bonus))
print(bonus_sal_lst)

#creating complex lambda function
company_bonus=1000
it_sal=[100000,200000,300000,150000]
bonus_sal_lst=[]
bonus_func_lam=lambda sal_lst,bonus:list(map(lambda sal:bonus_sal_lst.append(sal+1000),sal_lst))
bonus_func_lam(it_sal,1000)
print(bonus_sal_lst)
bonus_sal_lst=[]
mkt_sal=[90000,80000,50000,15000]
bonus_func_lam=lambda sal_lst,bonus:list(map(lambda sal:bonus_sal_lst.append(sal+1000),sal_lst))
bonus_func_lam(mkt_sal,1000)
print(bonus_sal_lst)
bonus_sal_lst=[]
hr_sal=[50000,70000,60000,15000]
bonus_func_lam=lambda sal_lst,bonus:list(map(lambda sal:bonus_sal_lst.append(sal+1000),sal_lst))
bonus_func_lam(hr_sal,1000)
print(bonus_sal_lst)

print("10. Recursive Function/Recursion/Iteratitive functions")
print("Technical definition of Recursive Function - If a function calls itself")
print("Implementation of Recursive Function -If I want to do the same functionality repeatedly on top of the the function result")
#Interview -> some common programming questions?
#prime number, greatest of 3, palindrom, even/odd, fibonacci, factorial, regular expression, collection types
#factorial example: Factorial of 4 -> 4*3*2*1
#n * ( n-1) * (n-2)…. * 1
#4*(4-1)*(4-2)*1
#5*(5-1)*(5-2)*(5-3)*1

def fact(n):
    print(n)
    if n==1:
        return 1
    #print(n*fact(n-1))
    return n*fact(n-1)
#fact(4) ->4*fact(3) -> 3*fact(2)-> 2*fact(1) -> 1
#fact(4) ->4*6=24 <- 3*2=6 <- 2*1=2 <- 1
print(fact(4))

#LOOPING IS REPETITION AND RECURSION IS NOT REPETITION

#some real life scenario - Compound interest
#1000 with a compound interest 10% for 3 months -> 1000+100 -> 1100+110 -> 1210+121 = 1331
#1000 with a simple interest 10% for 3 months -> 1000+100 -> 1000+100 -> 1000+100 = 1300
def simple_int(amt,tenure,interest):
    total_int=0
    for i in range(0,tenure):
        total_int+=(amt*(interest/100))
    return amt+total_int

def compound_int(amt,mth):
    amt=amt+(amt*.1)
    print(f"amount for every month {mth} is {amt}")
    mth -= 1
    if mth==0:
        return amt
    return compound_int(amt,mth)

print(compound_int(10000,12))

#fibonacci Series
#Sprint releases of (2 weeks/3 weeks) / Agile development (SDLC)
#Story points has to be in a fibboneci series -> 1,2,3,5,8,13
#jira ticketing system
#Create fibboneci series of 0->5
#0,1,1,2,3,5,8,13,21..
#n-1+n-2
def fibboneci(n):
    #print(n)
    if n==0:
        return 0
    elif n==1:
        return 1
    #print(n*fact(n-1))
    return fibboneci(n-1)+fibboneci(n-2)


