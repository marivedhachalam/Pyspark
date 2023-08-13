print("Emp monthend wage calculation")
from org.inceptez.all_accts_functions import *
print("prog to calculate bonus")
irfan_emps=[('a',10000),('b',20000),('c',30000)]
bonus_amt=bonus(100,1000000)
print(f"Bonus for each emp is {bonus_amt}")

print("prog to calculate pf")
for i in irfan_emps:
    pf_amt=pf(i[1])
    emp_name=i[0]
    print(f"pf of the employee {emp_name} is {pf_amt}")

