def offer_func(foodamt, offer, max_offer):
    foodamt = int(input())
    offer = 10 / 100
    max_offer = 50

    if foodamt * (offer) > max_offer:
        print(foodamt - max_offer)
    else:
        print(foodamt - foodamt * (offer))


# func calling
cal = offer_func(int(), float(), int())
print(cal)


def get_order_det(tot_amnt, offer_per, max_offer):
    calc_offer = tot_amnt * (offer_per / 100)
    if (calc_offer > max_offer):
        calc_offer = max_offer
    return calc_offer


tot_amnt = int(input())
offer_per = 10
max_offer = 50
sub_amnt = get_order_det(tot_amnt, offer_per, max_offer)
amnt_to_pay = tot_amnt - sub_amnt
print(f"The amount to pay is INR.{amnt_to_pay}")


def swiggy_order(amt, disctp, disctamt):
    try:
        if (amt * (disctp / 100) > disctamt):
            return amt - amt * (disctp / 100)
        else:
            return amt - disctamt
    except Exception as e:
        print(f"Some common exception occured {e}")


a = swiggy_order(400, 10, 50)
print(a)


def test(foodamount, Percent, Maxoffer):
    if foodamount * (10 / foodamount) > Maxoffer:
        Billamount = foodamount - Maxoffer
        return Billamount
    else:
        Dedamount = foodamount * (10 / foodamount)
        Billamount = foodamount - Dedamount
        return Billamount


def test(foodamount, Percent, Maxoffer):
    if foodamount * (Percent / foodamount) > Maxoffer:
        Billamount = foodamount - Maxoffer
        return Billamount
    else:
        Dedamount = foodamount * (Percent / foodamount)
        Billamount = foodamount - Dedamount
        return Billamount

def fnc_get_max_discount(p_food_amt, p_offer_pct, p_max_off, p_final_offer):
    food_amt = p_food_amt
    offer_pct = p_offer_pct
    max_off = p_max_off

    if p_food_amt * (10 / 100) > p_max_off:
        p_final_offer = p_max_off
    else:
        p_final_offer = p_food_amt * (10 / 100)

    return p_final_offer

def calc_bill(total_billed_Amt, max_discount, discount_offered_percent):
    if (max_discount > total_billed_Amt * discount_offered_percent / 100):
        return total_billed_Amt - total_billed_Amt * discount_offered_percent / 100
    else:
        return total_billed_Amt - max_discount

def calculate_discount(food_amt, offer_pct, max_off_amt):
    # Calculating the discount amount based on the percentage offer
    discount_amt = (offer_pct / 100) * food_amt

    # If the discount amount exceeds the maximum discount amount, limit it to the maximum
    if discount_amt > max_off_amt:
        discount_amt = max_off_amt

    # Calculating the final amount after discount
    final_amt = food_amt - discount_amt

    # Return the final amount and the discount amount
    return final_amt, discount_amt


# calling this function

final_amt, discount_amt = calculate_discount(800, 10, 50)

print("Total amount: Rs.", 800)
print("Discount amount: Rs.", discount_amt)
print("Final amount: Rs.", final_amt)

