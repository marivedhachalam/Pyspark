def avail_coupon(coupon):
    if (coupon=='summer10'):
        discount_percentage=10
    else:
        discount_percentage =5
    return discount_percentage

coupon='summer10'
max_offer_amount=50

def promo_campaign_dynamic_pricing_func(total_cart_amount):
    discount_percentage = avail_coupon(coupon)
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
#from wd30.pythonprogramming.scratch import get_offer_cart_amt