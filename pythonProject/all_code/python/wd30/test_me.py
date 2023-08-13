
print("experiencing with variable declaration")
print('--------------------------------------------')

name = '''Inceptez Technologies '''
Name = " 'Inceptez!' "
print (name)
print (Name)

x="Hello Mari"
print(x)
print(type(x))

name = " 'Mari here!' "
print (name)
print (type(name))
name = '''Inceptez
Technologies '''
print (name)
print (type(name))

x=100
print(x)
print(type(x))

x=100.50
print(x)
print(type(x))

x=10e7
print(x)
print(type(x))

xx=True
print(xx)
print(type(xx))

x=None
print(x)
print(type(x))

x=100
(bytes(x))
print(x)

x = str(100)
y = int(100)
z = float(100)
print(type(x))
print(type(y))
print(type(z))

print('--------------------------------------------')

print('Relational Operators')
x=7
y = 5
if x > y:
   print("x is greater than Y")
elif y>x:
   print ("y is greater than x")
else:
   print("x equals y")

i = 3
while True: #infinate loop (scheduling), Do-While
   print(i)
   if i <= 2:
    break
   i -= 1
   print("Do while example loop with break completed here")

print('--------------------------------------------')
print("How to define a function & Call function:")

def return_mail_id(fname, lname):  # number of arguments can be any or nothing
 mail_id = fname + '.' + lname + '@gmail.com'
 return mail_id  # optional return statement

mail_id_is=return_mail_id('mari', 'vedhachalam')
print(mail_id_is)

def get_mail_id(fname, lname, domain):
 mail_id = fname + '.' + lname + domain  # scope of this variable is limited within the function
 return mail_id

mail_id=get_mail_id('Mari','Vedhachalam','@inceptez.com')
print()
print(mail_id)

#offer applied amount with either 10% / 50 rupees (whichever is lesser)
total_order_amount=490
offer_pct=10
max_offer=50
applied_offer=total_order_amount * offer_pct/100
if applied_offer > 50:
 final_order_amount=total_order_amount - max_offer
else:
 final_order_amount=total_order_amount - applied_offer

print(f'Amount to be paid after applying today discount % : {final_order_amount}')
print('--------------------------------------------')

kms_driven=[("a","Petrol",20),("b","Diesel",30)]
print(kms_driven[0])
mylambda3 = lambda kms:kms[2]*5 if kms[1]=="Petrol" else kms[2]*3
map2 =map(mylambda3,kms_driven)
print(list(map2))

print('--------------------------------------------')
def optimal_offer(purchase_amount, offer_percent, max_offer_amt):
 offer_amt = purchase_amount * offer_percent
 print(f"calculated offer amount is {offer_amt} ,Max offer discount is {max_offer_amt}")
 if (offer_amt > max_offer_amt):
  print('''Calculated offer amount is more than the max offer amount, hence returning the
  max_offer_amt applied total amount''')
  return purchase_amount - max_offer_amt
 else:
  print('''Calculated offer amount is less than the max offer amount, hence returning the
  offer_percent applied total amount''');
 return purchase_amount - (purchase_amount * offer_percent)

amount_paid=optimal_offer(2000,.10,200)
print(f"amount to be paid  {amount_paid}")

print(return_mail_id(lname="technologies",fname="inceptez"))
print(optimal_offer(1000, .04, 50))
print(optimal_offer(1000, .06, 50))

#Default Arguments Value:
#If we call the function without argument, it uses the default value:
def return_mail_id(fname,lname,domainname="@inceptez.com"):
    mail_id=fname + '.' + lname + domainname
    return mail_id
print(return_mail_id("inceptez","technologies")) #Default arguments
print(return_mail_id("inceptez","technologies","@gmail.com"))#Overriding arguments

def return_mail_id(*name):
    mail_id=name[0] + '.' + name[1] + '@gmail.com'
    return mail_id
print(return_mail_id("inceptez","technologies"))

def return_mail_id(**name):
    mail_id=name["fname"] + '.' + name["lname"] + '@gmail.com'
    return mail_id
print(return_mail_id(lname="technologies",fname="Inceptez"))

global default_domainname # no need to define, by default global variable
default_domainname= '@explicit_default_domainname.com' # explicit Global variable
default_global_domainname= '@default_global_domainname.com' # By default Global variable
def return_mail_id(fname,lname):
  local_domain_name= '@gmail.com' # By default local variable
  global global_domain_name
  global_domain_name= '@inceptez.com' # global variable
  local_mail_id = fname + '.' + lname + local_domain_name
  print(local_mail_id)
  global_mail_id = fname + '.' + lname + global_domain_name
  print(global_mail_id)
return_mail_id("Inceptez", "technologies")
print(default_domainname)
print(default_global_domainname)
#print(local_domain_name) - print doesnt work as its defined inside function and not a global variable.
print(global_domain_name)

print('----------------------------------------------------')
def add_all(numbers):
    sum_of = sum(numbers)
    print(f"sum of given numbers {sum_of}")
    return sum_of

def sqrt_hof(input_hof_func,args):
    sum_of_args=input_hof_func(args)
    return sum_of_args*sum_of_args
print("Square root of the sum of second argument is "+ str(sqrt_hof(add_all,[1,2,3])))
print('--------------------------------------------')
print('Anonymous function & Lambda')
anonymous_add_all =(lambda a:a+1)
print(f"anonymous add numbers {anonymous_add_all(5)}")

lst=[10000,20000,5000,25000,9000,15000]
lambda_filter=lambda x:x>10000
list(filter(lambda_filter,lst))
print(f'filtered record is {list(filter(lambda_filter,lst))}')

print('---- closure function starts here ------')
def sal_hike(sal,hike):
    salhike=sal+hike
    print(f'calculated salary is {salhike}')
    def incentives(incentive):
        final_sal= salhike+incentive
        print(f'final calculated salary added after incentive is {final_sal}')
    return incentives
sal_hike(20000,2000)
print('------------------------------------------')
mainout = sal_hike(20000,2000)
mainout(1000)
print('---- closure function stops here --------')

print('------------------------------------------')
print('---- Recursive function starts here ------')
f=3
def recurse_fact(arg):
    if arg == 1:
       return 1
    else:
       return (arg * recurse_fact(arg-1))
print(f'Result of factorial {f} is {recurse_fact(f)}')

print('---- Recursive function stops here --------')
capitals = {"USA":"Washington D.C.", "England" :"London", "India":"New Delhi"}
capitals["India"] #If the value not found it will throw key error
capitals.get("India","NA") #To avoid key error we can use get function
capitals.update({"India":"Delhi NCR"}) #update the value if key exists
capitals.update({"Malaysia":"KL"}) #add the key,value if key doesn’t exists
capitals.pop("England") #Delete the key and the value
print(capitals)

set1={"Washington D.C.", "London"}
set1.add("Delhi")
set1.remove("London")
print(set1)
set2={"Beijing", "Penang"}
set1.update(set2)
print(set1)

print("Example1: pre defined - statement level exception")
print("control will go to the exception then continue run the next set of statements in the next block")
try:
    num1=int(input("enter number1 to multiply"))
except ValueError as detailsOfError:
    print(f"ValueError: {detailsOfError}")
    #ValueError: invalid literal for int() with base 10: 'ten'
    print(f"User you made some errors: {detailsOfError}")
    print(f"provide the right value eg:10 or 20, running a default multiplication program, as per the eg. given below")
    num1 = 10
    num2=20
    print(f"product of 2 numbers is {num1 * num2}")
    #exit(1)

class InceptezCustomException(Exception):
      pass


try:  # block level exception
        idx = int(input("enter numerator index"))
        den1 = int(input("enter a nonzero denominator number"))
        lst = [100, 200, 300, 400]
        maxidx = len(lst) - 1
        if (maxidx >= idx):
            num1 = lst[idx]
        else:
            num1 = lst[maxidx]
        print(num1)
        if den1 != 0:
            dividedresult = num1 / den1
            print(dividedresult)
        else:
            dividedresult = num1 / 1
            print(f"default denominator of 1 is used, {dividedresult}")
        sumresult = num1 + dividedresult
        print(sumresult)

        if (den1 == 0):  # user raised exception whenever needed
            print("finally failing the program since the denominator is zero")
            # raise InceptezCustomException
            raise ValueError

except ValueError as errmsg:
        print(f"value error: {errmsg}")
except NameError as errmsg:
        print(f"provide the denominator error: {errmsg}")
        # dividedresult = num1 / den1
        # print(f"result {dividedresult}")
except ZeroDivisionError as errmsg:
        print(f"provide the non zero denominator: {errmsg}")
except IndexError as errmsg:
        print(f"provide the right index value: {errmsg}")
        print(len(lst) - 1)
except InceptezCustomException as somemsg:
        print(f"custom error occured: {somemsg}")
        exit(1)
except Exception as somemsg:
        print(f"some error occured: {somemsg}")
        exit(1)

    # try:#dividing the above block to statement level exception
    #    sumresult = num1 + den1
    #    print(sumresult)
    # except Exception as somemsg:
    #    print(f"some error occured: {somemsg}")
a=4
b=2
c=3
if (a == b == c):
    print("all are equal")
    print("one more print all are equal")
    if a > 100:
        print("a is greater than 100")
elif a > b or a > c:
    print("a is greater..a value is %d " % a)
elif b > a and b > c:
    print("b is greater")
else:
    print("c is greater")

greet="Welcome to " \
      "Python"
greet1="""Welcome to 
Pyspark"""






