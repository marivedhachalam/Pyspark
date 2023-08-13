'''def maxDiscountedPrice(int cartAmount, int offerPercent, int maxOfferAmount):
	if (cartAmount*(offerPercent/100) <= maxOfferAmount):
		discountedPrice = cartAmount - cartAmount*(offerPercent/100)
	else:
		discountedPrice = cartAmount - cartAmount*(offerPercent/100)
	return discountedPrice

cartAmount = int(input("Enter your cart amount:"))
discountedPrice = maxDiscountedPrice(cartAmount, 10, 50)
print("Discounted Price is:" + discountedPrice)

def swiggy_offer (total:float, offer:float, maximum:int):
    calculated_discount = (offer * maximum) / 100
    if calculated_discount > maximum:
        final_discount = maximum
    else:
        final_discount = calculated_discount
    cart_amount = total - calculated_discount
    return (cart_amount)
'''