# Author : Sumukha Rajapuram Mohan
# LSH/BUCKETING/Streaming/hashing for on-fly clustering on similar regions of Islandia

import os
from pyspark import SparkConf, SparkContext
from tifffile import TiffFile
import io
import zipfile
import numpy as np
import hashlib
import math
from scipy import linalg

def getOrthoTif(zfBytes):
    #given a zipfile as bytes (i.e. from reading from a binary file),
    # return a np array of rgbx values for each pixel
    bytesio = io.BytesIO(zfBytes)
    zfiles = zipfile.ZipFile(bytesio, "r")
    #find tif:
    for fn in zfiles.namelist():
        if fn[-4:] == '.tif':#found it, turn into array:
            tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))
            return tif.asarray()

def format(t):
    s=t[0]
    arr=t[1]
    temp_list=[]
    for i in range(0,25):
            string=s+'-'+str(i)
            portion=arr[ int(i/5)*500:((int(i/5)+1)*500), (i%5)*500:((i%5)+1)*500 ]
            #portion=((int(i/5)*500,((int(i/5)+1)*500)),((i%5)*500,((i%5)+1)*500))
            temp_list.append((string,portion))

    temp_tuple=tuple(temp_list)
    return temp_tuple


def transform(arr):
    result=[[0 for col in range(0,500)] for row in range(0,500)]
    
    for i in range(0,500):
        for j in range(0,500):
            result[i][j]=int(((int(arr[i][j][0])+int(arr[i][j][1])+int(arr[i][j][2]))/3)*(arr[i][j][3]/100))            
 

    print("transform")
    print(result)
    return result


def mean(arr,start_row,end_row,start_col,end_col):
    ans=0
    for i in range(start_row,end_row):
        for j in range(start_col,end_col):
            ans=ans+arr[i][j]

    ans=ans/100
    return ans 

def compress(arr):
    result=[[0 for col in range(0,50)] for row in range(0,50)]
    for i in range(0,50):
        for j in range(0,50):
            result[i][j]=(mean(arr,i*10,(i+1)*10,j*10,(j+1)*10)) 

    #print("compress")
    #print(result)
    return result    


def row_col_diff(arr):
    #print("row_col_diff")
    res1=[[0 for col in range(0,50)] for row in range(0,49)]
    for j in range(0,50):
        for i in range(0,49):
            res1[i][j]=arr[i+1][j]-arr[i][j]
            if res1[i][j] < -1:
                res1[i][j]=-1
            elif res1[i][j]>1:
                res1[i][j]=1
            else:
                res1[i][j]=0

    res1=np.array(res1)
    res1=res1.ravel()
    
    
    res2=[[0 for col in range(0,49)] for row in range(0,50)]
    for i in range(0,50):
        for j in range(0,49):
            res2[i][j]=arr[i][j+1]-arr[i][j]
            if res2[i][j]<-1:
                res2[i][j]=-1
            elif res2[i][j]>1:
                res2[i][j]=1
            else:
                res2[i][j]=0

    res2=np.array(res2)
    res2=res2.ravel()

    return np.append(res2,res1)
    
def signature(features):
    sign=[]
    for i in range(0,127):
        string=features[38*i:38*(i+1)]
        hash=hashlib.md5(string).hexdigest()
        hash2=bin(int(hash,16))
        sign.append(hash2[-1])        

    sign=np.array(sign)
    return sign

def LSH(hashed_list_bits):
    l=[]
    #@1 for i in range(0,16):
    for i in range(0,12):
        #@1 l.append(hashlib.md5(hashed_list_bits[i*8:(i+1)*8]).hexdigest())
        l.append(hashlib.md5(hashed_list_bits[i*10:(i+1)*10]).hexdigest())


    return l

def is_present(image_list):
    if ('3677454_2025195.zip-1' in image_list) or ('3677454_2025195.zip-18' in image_list) or  ('3677454_2025195.zip-0' in image_list) or ('3677454_2025195.zip-19' in image_list):
        return 1
    else:
        return 0


def buckets_hash_pairs(record):
    image_name=record[0]
    hash=record[1]
    temp_list=[]
    #@1for bucket in range(0,16):
    for bucket in range(0,12):
        temp_list.append(((bucket,hash[bucket]),[image_name]))

    return temp_list
    
def group_similar(l):
    temp_list=[]
    for ele in l:
        if ele=='3677454_2025195.zip-1' or ele=='3677454_2025195.zip-18' or ele=='3677454_2025195.zip-0' or ele=='3677454_2025195.zip-19':
            temp_list.append((ele,l))

    return temp_list

def pca(features):
    features1=np.array(features)
    features1_diff=np.diff(features1.astype(np.int8), axis=1)

    mu, std = np.mean(features1_diff, axis=0), np.std(features1_diff, axis=0)
    features_diffs_zs = (features1_diff - mu) / std

    U, s, Vh = linalg.svd(features_diffs_zs, full_matrices=1)

    fv=np.concatenate((U[0],U[1],U[2],U[3],U[4],U[5],U[6],U[7],U[8],U[9],U[10]),axis=0)
    return fv[0:10]

def group_them(record):
    key=record[0]
    l=record[1]
 
    temp_list=[]
    for ele in l:
        temp_list.append((ele,key))

    return temp_list

def euclidean_distance(record,v_0,v_1,v_18,v_19):
    key=record[0]
    val=record[1][0]
    fv=record[1][1]

    if key=='3677454_2025195.zip-0':
        ed=math.sqrt(sum(np.square(fv-v_0)))

    elif key=='3677454_2025195.zip-1':
        ed=math.sqrt(sum(np.square(fv-v_1)))

    elif key=='3677454_2025195.zip-18':
        ed=math.sqrt(sum(np.square(fv-v_18)))

    elif key=='3677454_2025195.zip-19':
        ed=math.sqrt(sum(np.square(fv-v_19)))

    return (key,[val,ed])


if __name__=="__main__":
    
    print("Image points")
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    rdd1=sc.binaryFiles('hdfs:/data/large_sample')
    rdd2=rdd1.map(lambda x:(x[0].split('/')[5],getOrthoTif(x[1])))
    rdd3=rdd2.flatMap(lambda x:format(x))
    rdd4=rdd3.filter(lambda x:x[0]=='3677454_2025195.zip-0' or x[0]=='3677454_2025195.zip-1' or x[0]=='3677454_2025195.zip-18' or x[0]=='3677454_2025195.zip-19').map(lambda x:(x[0],x[1][0][0]))
    print(rdd4.collect())
    

    print("\nFeature values\n")    
    rdd_pre_feature=rdd3.map(lambda x:(x[0],transform(x[1]))).map(lambda x:(x[0],compress(x[1])))
    #use for pca

    rdd_feature=rdd_pre_feature.map(lambda x:(x[0],row_col_diff(x[1])))
    print(rdd_feature.filter(lambda x:x[0]=='3677454_2025195.zip-1' or x[0]=='3677454_2025195.zip-18').collect())

    #Begin 3 with rdd_feature, each rdd, put split 4900 in chunks of 38,
    rdd_signature=rdd_feature.map(lambda x:(x[0],signature(x[1])))
    print("\n\n")
    #print(rdd_signature.filter(lambda x:x[0]=='3677454_2025195.zip-1' or x[0]=='3677454_2025195.zip-18').collect())

    #Generating sub md5 hashes for each of the 8 bit chunks. There are 16 such chunks
    rdd_LSH=rdd_signature.map(lambda x:(x[0],LSH(x[1])))        #Now it contains img,sub_md5_hashes
    print("\n\nAfter LSH, similar images. 10 bits/bin. Totally 12 bins.\n\n")
    #---------print(rdd_LSH.filter(lambda x:(x[0]=='3677454_2025195.zip-1' or x[0]=='3677454_2025195.zip-18' or x[0]=='3677454_2025195.zip-0' or x[0]=='3677454_2025195.zip-19' or x[0]=='3677454_2025195.zip-2')).collect())

    #rdd_similar_items=rdd_LSH.map(lambda x:(x[1][0],[x[0]])).reduceByKey(lambda a,b:a+b).filter(lambda x:is_present(x[1])==1)
    rdd_similar_items=rdd_LSH.flatMap(lambda x: buckets_hash_pairs(x)).reduceByKey(lambda a,b:a+b).filter(lambda x: is_present(x[1])==1)
    rdd_similar_items1=rdd_similar_items.map(lambda x:(x[0],list(set(x[1]))))
    
    rdd_similar_items2=rdd_similar_items1.flatMap(lambda x:group_similar(x[1])).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0],list(set(x[1]))))
    #@ uncomment print(rdd_similar_items2.filter(lambda x:x[0]=='3677454_2025195.zip-1' or x[0]=='3677454_2025195.zip-18').collect())
    print(rdd_similar_items2.collect())    

    #pca
    #rdd_kv_transform=rdd_similar_items2.flatMap(lambda x:group_them(x)).join(
    rdd_pre_feature1=rdd_pre_feature.map(lambda x:(x[0],pca(x[1])))
    val_0=rdd_pre_feature1.filter(lambda x:x[0]=='3677454_2025195.zip-0').collect()
    val_1=rdd_pre_feature1.filter(lambda x:x[0]=='3677454_2025195.zip-1').collect()
    val_18=rdd_pre_feature1.filter(lambda x:x[0]=='3677454_2025195.zip-18').collect()
    val_19=rdd_pre_feature1.filter(lambda x:x[0]=='3677454_2025195.zip-19').collect()
    v_0=val_0[0][1]
    v_1=val_1[0][1]
    v_18=val_18[0][1]
    v_19=val_19[0][1]
    rdd_kv_transform=rdd_similar_items2.flatMap(lambda x:group_them(x)).join(rdd_pre_feature1).map(lambda x:(x[1][0],(x[0],x[1][1]))).map(lambda x:euclidean_distance(x,v_0,v_1,v_18,v_19)).reduceByKey(lambda a,b:a+b)

    print("\n\nDistances\n\n")
    print(rdd_kv_transform.collect())

