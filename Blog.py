import os
from pyspark import SparkConf, SparkContext


def func(string,s):     #Custom sub function which is called inside mapper lambda
    dic={}
    for ele in s:
        dic[ele.lower()]=string.count(ele.lower())

    l=list(dic.items())
    l=list(filter(lambda x:x[1]!=0,l))
    l=sorted(l)
    return l

def format_count(data,s):   #Custom main function which is called in mapper lambda to give the desired format where each element is a file in itelf
    data=data.split('<date>')
    data=list(map(lambda x: x.split('</date>'),data[1:]))
    data=list(map(lambda x: (x[1].lower(),x[0].split(',')[2]+'-'+x[0].split(',')[1]),data))

    data=list(map(lambda x: (func(x[0],s),x[1]),data))
    #print(data)
    data=list(filter(lambda x:x[0]!=[],data))
    #print(data)
    data=list(map(lambda x: ((x[0][0][0],x[1]),x[0][0][1]),data))
    #print(data)

    dic1={}
    for ele in data:
        try:
            dic1[ele[0]]=dic1[ele[0]]+ele[1]
        except KeyError:
            dic1[ele[0]]=ele[1]

    data=list(dic1.items())
    return data


if __name__=="__main__":
    
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    var="blogs"         #Directory variable where files are stored. This needs to be changed accordingly depending on the location while testing
    file_names=os.listdir(var)
    #print(file_names)

    file_names=list(map(lambda x: var+'/'+x, file_names))
    rdd_files=sc.parallelize(file_names)
    rdd_set_industries=rdd_files.map(lambda x:x.split('.')[3]).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).map(lambda x:x[0])

    s=rdd_set_industries.collect()
    s=set(s)

    rdd_b=sc.broadcast(s)    #Broadcast variable of set of all industries
    #print(rdd_b.value)

    
    files=','.join(file_names)
    rdd_files=sc.wholeTextFiles(files)
    #print("done")
    #print("rdd_files : ",rdd_files.take(1))
    rdd_files=rdd_files.map(lambda x:x[1].replace('\r\n',' '))
    #print("\n\nrdd_files : \n\n",rdd_files.take(1))
    rdd1=rdd_files.flatMap(lambda x: format_count(x,s))
    #print("\n\n",rdd1.collect())
    rdd2=rdd1.reduceByKey(lambda a,b:a+b)
    #print(rdd2.collect())
    rdd3=rdd2.map(lambda x:(x[0][0],[(x[0][1],x[1])]))
    #print(rdd3.collect())
    rdd4=rdd3.reduceByKey(lambda a,b:a+b)
    print(rdd4.collect())
