print("Fuel allowance program for applicable departments")
kms_calc_lambda=lambda x:x[2] * 5
def kms_calc_funct(x):
    return x[2]*5

#Marketing dept
#marketing manager irfan
kms_driven=[('a','petrol',20),('b','diesel',30)]
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