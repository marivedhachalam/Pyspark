def read_data(type,sparksess,src,strtype,mod='failfast',infersch=False,delim=',',headerflag=False): # reusable function
    if type=='csv' and strtype!="":
     df1=sparksess.read.csv(src,schema=strtype, mode=mod,header=headerflag, inferSchema=infersch, sep=delim)
     return df1
    elif type=='csv':
     df1 = sparksess.read.csv(src, mode=mod, header=headerflag, inferSchema=infersch, sep=delim)
     return df1
    elif type=='json':
     df1 = sparksess.read.option("multiline", "true").schema(strtype).json(src)
     #df1.select(col("pagevisit").getItem(0)).show()
     #df1=sparksess.read.json(src)
     return df1

def deDup(df,cols,ord,subst):# reusable function
    return df.sort(cols,ascending=ord).dropDuplicates(subset=subst)

def optimize_performance(sparksess,df,numpart,partflag,cacheflag,numshufflepart=200):
    print("Number of partitions in the given DF {}".format(df.rdd.getNumPartitions()))
    if partflag:
     df = df.repartition(numpart)
     print("repartitioned to {}".format(df.rdd.getNumPartitions()))
    else:
     df = df.coalesce(numpart)
     print("coalesced to {}".format(df.rdd.getNumPartitions()))
    if cacheflag:
     df.cache()
     print("cached ")
    if numshufflepart!=200:
     # default partions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
     sparksess.conf.set("spark.sql.shuffle.partitions", numshufflepart)
     print("Shuffle part to {}".format(numshufflepart))
    return df

def munge_data(df,dict1,drop,fill,replace,coltype='all'):# reusable function
    df1=df.na.drop(coltype,subset=drop)
    df2=df1.na.fill("na", subset=fill)
    df3=df2.na.replace(dict1, subset=replace)
    return df3

def age_conversion(age):#Python Function for UDF conversion
   if age < 13:
      return "Children"
   elif age >=13 and age<=18:
      return "Teen"
   else:
      return "Adults"

def fil(df,condition):# reusable function
    return df.filter(condition)

def mask_fields(df,cols,masktype,bits=-1):#is it supposed to be a inline or reusable function?reusable function
    for i in cols:
        if (bits<0):
         df=df.withColumn(i,masktype(i))
        else:
         df=df.withColumn(i,masktype(i,bits))
    return df

from configparser import *
def writeRDBMSData(df,propfile,db,tbl,mode):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode, properties={"driver": driver})


