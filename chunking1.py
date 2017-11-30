from pyspark import SparkContext, SparkConf

#word count implementation below
data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
(7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
(9, "The car raced past the finish line just in time."),(10, "Car engines purred and the tires burned.")]


conf=SparkConf()
sc=SparkContext(conf=conf)
#formatting : removing special characters ( :,;=!)
rdd1=sc.parallelize(data)
rdd2=rdd1.map(lambda x:(x[0],x[1].replace(',',' '))).map(lambda x:(x[0],x[1].replace('.',' '))).map(lambda x:(x[0],x[1].replace('-',' ')))
rdd3=rdd2.map(lambda x:(x[0],x[1].replace(':',' '))).map(lambda x:(x[0],x[1].replace(';',' '))).map(lambda x:(x[0],x[1].replace('!',' '))).map(lambda x:(x[0],x[1].replace('=',' ')))

#(index,word) transform to (word,1) followed by reduceByKey
rdd4=rdd3.flatMap(lambda x:x[1].split()).map(lambda x:(x.lower(),1))
rdd5=rdd4.reduceByKey(lambda a,b:a+b)
print("word count output :\n",rdd5.collect())


################set difference################################################
from random import random
data2 = [('R', [x for x in range(50) if random() > 0.5]),('S', [x for x in range(50) if random() > 0.75])]

rdd=sc.parallelize(data2)
#print(rdd.collect())
rdd_r=rdd.filter(lambda x:x[0]=='R').flatMap(lambda x:x[1]).map(lambda x:(x,'R'))
#print(rdd_r.collect())
rdd_s=rdd.filter(lambda x:x[0]=='S').flatMap(lambda x:x[1]).map(lambda x:(x,'S'))
#print(rdd_s.collect())

print("\n\n\nset difference output : ")
print(rdd_r.union(rdd_s).groupByKey().map(lambda x:(x[0],list(x[1]))).filter(lambda x:x[1]==['R']).map(lambda x:x[0]).collect())
