def bonus_func(sal_lst,bonus):
    bonus_sal_lst=[]
    for i in sal_lst:
        bonus_sal_lst.append(i+bonus)
    return bonus_sal_lst

def tax_func(sal,tax):#
    return sal-tax

def incent_func(sal,incentives):#
    return sal+incentives

def pf_func(sal,pf):#
    #complex pf calculation
    return sal+pf

def gratuity_func(sal,gratuity):#
    #gratuity calculation
    return sal+gratuity