def sample_func1(x):#FBP alone
    return x + 100

class SampleProg:
    def sample_func(self,x):
        return x+100
    def sample_func1(x):
        return x+100
    def sample_func2(x):
        return x+100
class SalCalc:
    def sal_calc(sal):
        return sal+1000
    def sal_incentive(sal,incentive):
        return sal+incentive

class BalanceCalc:
    def bank_bal(withdrawal):
        return 100000-withdrawal
    def stock_bal(sal,incentive):
        return sal+incentive

class AllAccounts:#OOPS+FBP
    def bonus(self,empcnt, profit, percent=.10):
        bonus_amt = (profit * percent) / empcnt
        return bonus_amt
    def pf(self,basicpay):
        pf_amt = basicpay * .13
        return pf_amt
    def gratuity(self,empcnt, profit, percent=.10):
        bonus_amt = (profit * percent) / empcnt
        return bonus_amt
    def joining_bonus(self,empcnt, profit, percent=.10):
        bonus_amt = (profit * percent) / empcnt
        return bonus_amt