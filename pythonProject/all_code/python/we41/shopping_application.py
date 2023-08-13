def shopping_cart(shopname_arg:int,list_to_purchase_arg:str):
    try:
        print("Program execution started")
        shopname=shopname_arg#program aborted without any handler
        #any exception occured in the line 46 will take the control to line 68->72,73,74->85,86,87
        list_to_purchase=list_to_purchase_arg
        list_to_purchase = list_to_purchase.split(",")
        print(list_to_purchase)
        print(shopname)
        print("go to the shop...")
        list_of_products_in_the_shop={"pencil":[20,24],"oil":[100,1],"notes":[50,12]}

        #list_to_purchase=["pencil","oil","fanta"]
        shopping_cart_price=[]

        #print(f"identify only the products in the shop and complete your purchase")
        #set(list_of_products_in_the_shop.keys()).intersection(set(list_to_purchase))
        list_of_items_matching_with_my_shopping_list=set(list_of_products_in_the_shop.keys()).intersection(set(list_to_purchase))
        for prod in list_of_items_matching_with_my_shopping_list:
           print(prod)
           shopping_cart_price.append(list_of_products_in_the_shop[prod][0])#anything goes wrong any other line of code will not be executed
           if list_of_products_in_the_shop[prod][1] <= 3:
               msg1="product expiry date is not meeting my criteria, hence aborting the shopping"
               raise Exception#if no exception occured of its own, we can raise any type of exception when needed
           print(f"product in the list to purchase {prod}")
        print(f"If any exception occurs in the above line, this statement will not be called - Insert an entry in the audit/log table with the given error")
        totalamt=sum(shopping_cart_price)
        promo=10
        finalamt=totalamt-promo
        print(f"amout to pay finally {finalamt}")

    except KeyError as msg:
        #print(f"some exception occured ")
        print(f"Some of the products is not there in the shop, hence aborting my purchase {msg}")
        print(f"Insert an entry in the audit/log table with the given error {msg}")
    except ValueError as msg:
        print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")
        print(f"Insert an entry in the audit/log table with the given error {msg}")
    except Exception as msg:
        print(f"some exception occured {msg1}")

        '''except ValueError as msg:
            #print(f"some exception occured ")
            print(f"enter the shop number as a whole number like 10 or 20 or 30 {msg}")'''
        #exit(1)
    else:#if no error in the try block, execute the below line of code
        print("Insert into the log table (if no exception occured)- Shopping completed successfully, going back to home")
        print("closing the db connections or close the files openened ")
    finally:#at any cost this block will run, either exception occured or not
        print("closing the db connections or files openened or deleting the temp files ")
        print("Insert into the log table (if either exception occured or not)- cleaning & locking my vehicle")
