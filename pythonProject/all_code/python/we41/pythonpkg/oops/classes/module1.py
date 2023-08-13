class All_Sals:
    def bonus_func(self,sal_lst, bonus):
        bonus_sal_lst = []
        for i in sal_lst:
            bonus_sal_lst.append(i + bonus)
        return bonus_sal_lst

    def tax_func(self,sal,tax):#
        return sal-tax

    def incent_func(self,sal,incentives):#
        return sal+incentives

    def pf_func(self,sal,pf):#
        #complex pf calculation
        return sal+pf

    def gratuity_func(self,sal,gratuity):#
        #gratuity calculation
        return sal+gratuity

class Timesheet:
    def day_off(self,hrs):
        return 100*hrs