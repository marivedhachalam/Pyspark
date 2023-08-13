#Function Based Programming*
print("What is FBP & Why FBP is needed")
# Simple/elegant/high-level, rather than writing 100 lines, we can achive the same result in 1 or 2 lines code
#Help us modularise/ create the entire application as components the code
#Help us for reusability
#Help us calling concurrently, parallelly, distributed (with the help of spark kind of framework)
#Help us create Frameworks to bring more uniformity, centralized controllability and organized way of managing/using code base
#
print("1. How to Create/define a Function (syntax & symantics)")
#naming convention & syntax
#start with def keyword
#use lowercase with _ for the name of the functions
#close the definition with () and following with : that help us start the block
#write atleast 1 line of code in the body of the function
#minimum function syntax
def profit_promo_calc():
    #print('bodyofthefunction')
    pass

#optimal function syntax
#domain:str='@inceptez.com'
def mail_id(fname:str,lname:str,domain:str):
    full_mail_id=fname+'.'+lname+domain#scope of this variable is limited within the function
    return full_mail_id
#mail_id()
#mail_id('md','irfan','@inceptez.com')

def mail_id_1(fname,lname,domain):
    full_mail_id=fname+'.'+lname
    try:
        if domain=='@inceptez.com':
            print('inceptez domain')
            fullmailid_with_domain=full_mail_id+domain
            return fullmailid_with_domain #whether return is mandatory? no, whether return can return multiple values/types? yes
        else:
            print('some other domain, hence making it as gmail.com')
            return full_mail_id+'@gmail.com'
    except Exception as e:
        print(f"some common exception occured with {e} for the given input of {fname},{lname},{domain}")

from wd30.pythonprogramming.scratch import *
#get_offer_cart_amt("","")
#get_offer_cart_amt1("","")

#Real life simple scenario - create a function to take 3 input args given below and calculate the
#offer applied amount with either 10% or 50 rupees (which ever is lesser)
#swiggy -> total 800,offer 10% (max offer 50 rs)
#800-50=750
#400-40=360
#dynamic pricing
#promo campaign
#shopping cart
food_amt=800

cart_amt=int(input("enter the amt"))
offer_pct=10
max_off_amt=50
#apply offer percent to the cart amount and get the calculated offer amount
# if the calculated offer amount exceeds the maximum offer amt then use the maximum offer amout
# else use the calculated amount
calculated_offer_amt=cart_amt*(10/100)
if calculated_offer_amt>=max_off_amt:
    print(f"we are using the max offer amout given by the company {max_off_amt}")
else:
    print(f"we are using the max offer amout given by the company {calculated_offer_amt}")

def get_offer_cart_amt(cart_amt,max_off_amt,offer_pct=10):
    #offer_pct = 10
    percent_calc=offer_pct/100
    calculated_offer_amt=cart_amt*percent_calc
    if calculated_offer_amt>max_off_amt:
     total_amt_topay=cart_amt-max_off_amt
     print(f"Customer has to pay total amount arrived with company offer amt {total_amt_topay}")
    else:
     total_amt_topay = cart_amt - calculated_offer_amt
     print(f"Customer has to pay total amount arrived with calculated offer amt {total_amt_topay}")
    return total_amt_topay

custname='irfan'
tax=.18
#from wd30.pythonprogramming.scratch import get_offer_cart_amt
total_amt=get_offer_cart_amt(600,50,10)
tax_calculated=total_amt*tax
tax_applied_total_amt_topay=total_amt+tax_calculated
print(f"final bill to pay is {tax_applied_total_amt_topay}")

print("How to Calling functions:")
final_mail_id=mail_id_1('anand','suresh','@inceptez.com')

print("2. Positional Arguments")
def mail_id(fname,lname,domain):
    full_mail_id=fname+'.'+lname+domain#scope of this variable is limited within the function
    return full_mail_id

mail_id('@inceptez.com','mohd','irfan') #positional argument

print("3. Named/Keyword Arguments")
mail_id(domain='@inceptez.com',fname='mohd',lname='irfan') #named argument

print("3.1. Default Arguments functions")
def mail_id(fname,lname,domain='@gmail.com'):
    full_mail_id=fname+'.'+lname+domain#scope of this variable is limited within the function
    return full_mail_id

mail_id('mohd','irfan','@inceptez.com') #default positional arguments
mail_id('mohd','irfan') #default positional arguments
mail_id(lname='irfan',fname='mohd') #default named arguments

mail_id(domain='@inceptez.com',fname='mohd',lname='irfan') #named argument

def bonus_sal(sal,bonus=1000):
    return sal+bonus

a=bonus_sal(10000,1000)#positional args
print(a)
a=bonus_sal(bonus=1000,sal=10000)#named args
print(a)

for sal in [10000,20000,15000,25000,30000,35000]:
    if (sal>25000):
        print(bonus_sal(sal,2000))
    else:
        print(bonus_sal(sal))#default args

print("4. Arbitraty/anything Arguments")
def mail_id(*args,domain='@gmail.com'):#add * before a single argument will help pass n number of args to the function (arbitrary)
    len_args=len(args)
    if len_args==3:
        full_mail_id=args[0]+args[1]+domain #scope of this variable is limited within the function
    elif len_args==2:
        full_mail_id = args[0]+args[1] + domain
    else:
        full_mail_id = args[0]+domain
    return full_mail_id
#mail_id('mohamed.x.irfan','@inceptez.com') ##call using the positional argument methodology
#mail_id('mohamed.x.irfan',domain='@inceptez.com')

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

print(sal_bon_inc('wipro',10000,1000,2000))
print(sal_bon_inc('infy',10000))
print(sal_bon_inc('cts',10000,1000))

print("5. Arbitraty Keyword Arguments")
def mail_id(**kwargs):#add * before a single argument will help pass n number of args to the function (arbitrary)
    #len_args=len(kwargs)
    full_mail_id=kwargs["fname"]+kwargs["lname"]+kwargs["domain"]
    return full_mail_id

mail_id(fname="mohamed",lname="irfan",domain="@inceptez.com")#call using the named argument methodology

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
print(sal_bon_inc_arb_keyword(sal=10000,comp='infy'))

print("Global and local variable concepts")
#global bonus #not need because by default every variable we define is a global
#global bonus
bonus=int(input("enter bonus amt"))

def bon_sal(sal):#closure function
    global total_sal
    if bonus>=1000:
        print("inside bonus>=1000")
        total_sal=sal+bonus#not a global variable, rather it is a local variable
        #return total_sal #the variable declared outside the function is changing/impacting the result of the function - Closure in FBP
    else:
        print("inside bonus<1000")
        total_sal=sal+900
    #global total_sal
    #return total_sal

def bon_sal_inc(sal,incentive):
    return sal+bonus+incentive

bon_sal(10000)
print(total_sal)#since the total_sal is defined as global value (inside the function), we can access outside of the function
#print(bon_sal_inc(10000,2000))
#positional args(10,20), named args (a=10,b=20), default args (a=10), arbitrary argument def f(*args) - f(10,20,30) - tuple(args[0],args[1]..)
#arbitrary keyword argument function def f(**kwargs) - f(a=10,b=20,c=30) - dict({"a":10,"b":20,"c":30}) - kwargs["a"]

print("Different special types functions:")#lambda, higher order functions, recursive/iterative functions, closure functions
print("************** 6. Anonymous Functions/Lambda Functions/Simple functions/Function Variable "
      "(not keeping in a pkg or classes or in a common module)/Named reusable def Function **************" )
print("Fuel allowance program for applicable departments")
#limitation for lambda function is (simple logics can be writterned, used only within the given program)
kms_calc_lambda=lambda x:x[2] * 5 #Anonymous Functions/Lambda Functions
def kms_calc_funct(x):
    return x[2]*5
kms_driven=[('a','petrol',20),('b','diesel',30)]
result_lam=map(kms_calc_lambda,kms_driven)#advisable
result_lam1=map(lambda x:x[2] * 5,kms_driven)#advisable
result_regular=map(kms_calc_funct,kms_driven)#not advisable
print(list(result_lam))
print(list(result_regular))

print("complex logic in lambda function (not advisable)")
kms_driven=[('a','petrol',20),('b','diesel',30),('irfan','gasoline',40)]
surcharge_anonymous=lambda kmd_ft:(kmd_ft[0],kmd_ft[2]*5 if kmd_ft[1]=='petrol' else (kmd_ft[2]*3 if kmd_ft[1]=='diesel' else kmd_ft[2]*2))
print(list(map(surcharge_anonymous,kms_driven)))

#Marketing dept
#marketing manager irfan
kms_driven=[('a','petrol',20),('b','diesel',30)]
#rather than using the for loop, use map function
#map(kms_calc_lambda,kms_driven)
for i in kms_driven:
    print(kms_calc_lambda(i))
#marketing manager raj
kms_driven=[('c','diesel',40),('d','gas',50)]
for i in kms_driven:
    print(kms_calc_lambda(i))
#marketing manager saravanan
kms_driven=[('c','diesel',60),('d','gas',70)]
for i in kms_driven:
    print(kms_calc_lambda(i))

print(" ************** 7. Higher Order Function  ************** ")
print("part1: HOF - A Function that takes another function as an input parameter is called a higher-order function.")
print("part2: HOF - A Function that RETURNS another function as an output parameter is called a higher-order function.")
#simple example of hof part1
def profit_calc(cp,sp):
    profit=sp-cp
    return profit

def promo_ind(hof_function,cp,sp):
    if hof_function(cp,sp)>0:
        return True
    else:
        return False

#if i don't use hof?
def promo_ind(cp,sp):
    if (sp-cp)>0:
        return True
    else:
        return False

#below funcs used by sales team
def profit_calc(cp,sp):
	profit=sp-cp
	return profit

def loss_calc(cp,sp):
	loss=cp-sp
	return loss

#below funcs used by the marketing team#irfan's question? did you understand what is hof? If a func (promo_ind) take an input as another function (profit_calc,loss_calc)
def promo_ind(hof_func1,hof_func2,cp,sp):#essaki's question
	#read from a table/file
	promoind=hof_func1(cp,sp)>0 #berlin's question
	lossind=hof_func2(cp,sp)>0
	return {"promoind":promoind,"lossind":lossind}#guru's question



#simple example of HOF part2 (otherwise called as currying or partial functions)
def calc(op_type):
    if (op_type=='a'):
        def add(a,b,c): #if the above if condition is met, define a function and return that function itself
            return a+b+c
        return add
    elif(op_type=='s'):
        def sub(a,b):
            return a-b
        return sub
    elif(op_type=='m'):
        def mul(a,b):
            return a*b
        return mul
    else:
        def div(a,b):
            return a/b
        return div

func_returned_add=calc('a')
func_returned_sub=calc('s')
func_returned_add(100,20,300)
func_returned_sub(100,20)

print("****************8. Closure Functions ***************")
print("A function's result is depending upon the value defined outside of the function")
#the below global values can be defined by the company (kept inside a file/table/passed as argument/constructor of class
max_off_amt=50
offer_pct=10
def get_offer_cart_amt(cart_amt):#Closure Function because the values max_off_amt,offer_pct defined outside of this func is affecting the result
    #offer_pct = 10
    percent_calc=offer_pct/100
    calculated_offer_amt=cart_amt*percent_calc
    if calculated_offer_amt>max_off_amt:
     total_amt_topay=cart_amt-max_off_amt
     print(f"Customer has to pay total amount arrived with company offer amt {total_amt_topay}")
    else:
     total_amt_topay = cart_amt - calculated_offer_amt
     print(f"Customer has to pay total amount arrived with calculated offer amt {total_amt_topay}")
    return total_amt_topay

custname='irfan'
tax=.18
#from wd30.pythonprogramming.scratch import get_offer_cart_amt
total_amt=get_offer_cart_amt(600)
print(total_amt)

print("higher order function + closure function+nested function definition")
#first I have only sal and hike values are available
#later I am getting the incentive amount defined by the company
def sal_hike(sal,hike):#Higher Order Function because sal_hike is returning another function incentives as a return type
    salhike=sal+hike#local variable for sal_hike function, but global variable for incentives function
    def incentives(incentive):#Closure function because the value salhike defined outside of incentives is affecting the incentives result
        return salhike+incentive
    return incentives

sal_hike_hof_incentives=sal_hike(10000,2000)#this call have did 2 works - 1. calculating the salhike variable,
cooking_food_withoutsalt=sal_hike(10000,2000)
got_thesalt_adding_to_the_cooked_food=cooking_food_withoutsalt(1000)
# 2. defining, applying salhike and returning incentives function
print(sal_hike_hof_incentives(1000))#calling the partially/currying function incentives with the input arg which will be added with the closure value (salhike)

print(" ************** 9. Iterative/Recursive (Recursion) Functions  ************** ")
print("Function that calls itself is recursive")
#do a work A -> A(1)+A(2)+A(3)
#Fibbonaci Series, Factorial of N numbers, Swapping(Palindrome), Prime Numbers, Regular Expressions
#Factorial of 4 -> 4*3*2*1=24
#fact(4) -> 24
#4*3*2*1
#to achieve factorial we have to (perform the below logic - write pseudo code)
# 1. subract given input (n) with 1 recursively
#n*(n-1)
#n*(n-1)*((n-1)-1)...*1
# 2. multiply the subracted value with the given value recursively
#4*3
#3*2
#2*1
def fact(n):
    if n==1:
        print(f"with in the n==1 condition with the value of {n}")
        return 1
    else:
        print(f"with in the n<>1 condition with the value of {n} with the function call {fact(n-1)*n}")
        return n*fact(n-1)

#iter1 -> fact(4) -> 4*fact(3) -> 4*6=24
    #iter2 -> fact(3) -> 3*fact(2) -> 3*2=6
        # iter3 -> fact(2) -> 2*fact(1) -> 2*1=2
            # iter4 -> fact(1) -> 1

print("Fibbonaci Series")
range_of_values=range(0,6)
#0+1+2

def fibbonaci(val):
    iter = 0
    #print(f"Main Function {iter}")
    if val==0:
        iter += 1
        print(f"If condition{val}")
        return 0
    elif val==1:
        iter += 1
        print(f"El If condition{val}")
        return 1
    else:
        iter += 1
        print (f"Else condition{val}")
        #print(fibbonaci(val-1) + fibbonaci(val - 2))
        return fibbonaci(val-1) + fibbonaci(val-2)

#fibbonaci(2)-> else -> 1+0=> 1
#fibbonaci(3)-> else -> fibbonaci(val-1) + fibbonaci(val-2) -> fibbonaci(2) + fibbonaci(1)=> 1+1 = 2
#fibbonaci(4)-> else -> fibbonaci(val-1) + fibbonaci(val-2) -> fibbonaci(4-1) + fibbonaci(4-2)=> 2 + 1 = 3
#fibbonaci(5)-> else -> fibbonaci(val-1) + fibbonaci(val-2) -> fibbonaci(5-1) + fibbonaci(5-2)=> fibbonaci(4) + fibbonaci(3) => 3 + 2 = 5
#fibbonaci(6)-> else -> fibbonaci(val-1) + fibbonaci(val-2) -> fibbonaci(5-1) + fibbonaci(5-2)=> fibbonaci(4) + fibbonaci(3) => 3 + 2 = 5

#1->8
#0,1,1,2,3,5,8,13,21...> To calculate the story points in the scrum/agile kind of project management
#3-2+3-1=1+2=>3
#5-2+5-1=2+3=>5
#8-2=3+8-1=5=2+3=>3+5=>8

#functions are not going to affect any performance, improve the performance even in spark
'''def json_file_read(arg1):
    import json
    try:#block level exception
        #num1=int(input("enter number1 to multiply"))
        #file1 = input()
        file2=open(arg1)
        jsonfile=json.load(file2)
        print("No exception occured")
        #print(jsonfile)
        return (jsonfile)
    except ValueError as detailsOfError:
        print(f"Give the right value : {detailsOfError}")
    except FileNotFoundError as detailsOfError:
        print(f"filenot found: {detailsOfError} , opening default file")
        #file2=open("/home/hduser/sparkdata/file2.json")
        #jsonfile=json.load(file2)
        #print(jsonfile)
        #print(f"provide the right value eg:10 or 20, running a default multiplication program")
        exit(10)
    else:#else case for exception, ie if no exception occurs then else block will be executed
        print("Json is parsed and no exception occured")
    finally:#either exception with exit or no exception occured, finally block will be called
        print("mainly used to close the files or db connections or cleaning up of resources used")
        print("whether exception occured or not, finally will be executed")

jsonData=[]
for i in ["/home/hduser/sparkdata/jsondata/file21.json","/home/hduser/sparkdata/jsondata1/file3.json"]:
    getJson=json_file_read(i)
    jsonData.append(getJson)

print(jsonData)
print(jsonData[0])
print(jsonData[1])
#[{'process': 'ETL Process1', 'source': ['hive', 'Bigquery'], 'target': ['HDFS', 'GCS'], 'cols': ['custid', 'upper(custname) as upper_custname'], 'tablename': 'customer', 'where': "(city='chennai')", 'gcs_uri': 'gcs://abc/xyz_bucket/'},
# {'process': 'ETL Process2', 'source': ['Bigquery'], 'target': ['s3'], 'cols': ['custid', 'lower(custname) as upper_custname'], 'tablename': 'customer', 'where': "(city='chennai')", 'gcs_uri': 's3a://abc/xyz_bucket/'}]
'''