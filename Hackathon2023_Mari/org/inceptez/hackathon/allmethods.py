import re
# Generic function to remove special character from text or string passed
def remspecialchar(input_string):
    cleaned_string = re.sub("[;\\@%-/:~,*?\"<>|&'0-9]",'',input_string)
    return cleaned_string

# Generic function to write into HDFS location
def writetofile(filetype,location,delimiter,mode,data):
    print(f"filetype : {filetype}")
    print(f"location : {location}")
    print(f"delimiter : {delimiter}")
    print(f"mode : {mode}")
    print(f"dataframe to be written as {filetype}")
    data.show(5)
    if filetype == 'csv':
        data.write.option('delimiter', delimiter).mode(mode).csv(location)
    elif filetype == 'json':
        data.write.mode(mode).json(location)
    else:
        raise ValueError(f"Unsupported file type: {filetype}")