#from automobile.sales.four_wheelers import EmiSchemes
import automobile.sales.four_wheelers
#showroom1 application
memory_showroom1_bank_person1_2yrs=automobile.sales.four_wheelers.EmiSchemes(24)
#contructing/instantiating/loading the class as an object by passing parameters
memory_showroom1_bank_person1_5yrs=automobile.sales.four_wheelers.EmiSchemes(60)
#customers who are all coming to know about monthly emi i have to pay for 2 years?
print(memory_showroom1_bank_person1_2yrs.emi(1000000))
print(memory_showroom1_bank_person1_2yrs.emi(1400000))
#customers who are all coming to know about monthly emi i have to pay for 5 years?
print(memory_showroom1_bank_person1_5yrs.emi(1000000))
print(memory_showroom1_bank_person1_5yrs.emi(1500000))

#showroom1
#customer is coming to know about SUV - x8, black?
memory_showroom1_sales_person1=automobile.sales.four_wheelers.VehicleTypes()#contructing an object with the default constructor class
print(memory_showroom1_sales_person1.suv("w8","black"))

#showroom2
#customer is coming to know about accessories?
memory_showroom2_sales_person2=automobile.sales.four_wheelers.Accessories()#contructing an object of non parameterized constructor class
print(memory_showroom2_sales_person2.basic_accessories("mat1"))
