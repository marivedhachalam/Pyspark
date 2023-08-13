import sys
def main(args):
    print("my spark program")
    print(args[0])#scriptname
    print(args[1])#first arg
    print(args[2])#second arg
    #print(args[3])

if (__name__=="__main__"):
    if (len(sys.argv)>=3):
        print(type(sys.argv))
        main(sys.argv)
    else:
        print("No enough arguments to call main method")