import sys
def main(args):
    print("my spark program")
    print(args[0])
    print(args[1])
    print(args[2])
    print(args[3])

if (__name__=="__main__"):
    if (len(sys.argv)==4):
        print(type(sys.argv))
        main(sys.argv)
    else:
        print("No enough arguments to call main method")