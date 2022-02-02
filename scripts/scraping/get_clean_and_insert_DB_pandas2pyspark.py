import pyspark
from pyspark.sql import SparkSession
#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import subprocess
import sys
#from cassandra.cluster import Cluster
from random import choice
from shutil import move
from os import path, mkdir
from time import time

spark = SparkSession.builder.master("local[*]").appName("SparkWorker").getOrCreate()

t0=time()


file = "masterfilelist.txt"
url = "http://data.gdeltproject.org/gdeltv2"
liste = requests.get(f"{url}/{file}").content.decode("utf-8").split("\n")
masterfile0 = [i for i in liste if "/2021" in i]
#open(f"{file}","w",encoding="utf8").write("\n".join(liste))

file = "masterfilelist-translation.txt"
url = "http://data.gdeltproject.org/gdeltv2"
liste = requests.get(f"{url}/{file}").content.decode("utf-8").split("\n")
masterfile1 = [i for i in liste if "/2021" in i]
#open(f"{file}","w",encoding="utf8").write("\n".join(liste))


def dl_unzip(url, file, tardir):
  subprocess.call(f"wget {url}{file} -P {tardir}".split())#, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
  subprocess.call(f"unzip {tardir}{file} -d {tardir}".split())#, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
  subprocess.call(f"rm {tardir}{file}".split())#, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
  return True

url = "http://data.gdeltproject.org/gdeltv2"
timespan = "day"
#masterfile0 = open("masterfilelist.txt","r",encoding="utf8").readlines()
#masterfile1 = open("masterfilelist-translation.txt","r",encoding="utf8").readlines()
print(masterfile0[-5:])
print(masterfile1[-5:])
files_en = list(map(lambda x: x.split(url)[-1], masterfile0))
files_tr = list(map(lambda x: x.split(url)[-1], masterfile1))
if timespan == "hour":
    files = files_en[-3*4:] + files_tr[-3*4:]
elif timespan == "day":
    files = files_en[-3*4*24:] + files_tr[-3*4*24:]
elif timespan == "month":
    files = files_en[-3*4*24*30:] + files_tr[-3*4*24*30:]
elif timespan == "year":
    files = files_en[-3*4*24*30*365:] + files_tr[-3*4*24*365:]
else:
    print("Please provide a valid timespan")


def nettoyage_event(file):
    col = ['GlobalEventID', 'Day', 'MonthYear', 'Year', 'FractionDate',
           'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
           'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
           'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code',
           'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
           'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
           'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent',
           'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
           'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
           'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
           'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat',
           'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type',
           'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code',
           'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
           'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName',
           'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code',
           'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED',
           'SOURCEURL']
    df = pd.read_csv(file, sep="\t", usecols=list(range(len(col))), names=col, encoding="ISO-8859-1",keep_default_na=False)
    df = df.loc[df["Year"]==2021][['GlobalEventID', 'Day', 'MonthYear', 
                               'GoldsteinScale', 'ActionGeo_CountryCode']] #,'SOURCEURL']]
    df.to_csv(file, sep="\t", header=False)
    return df

event = nettoyage_event('../../data/20220202123000.export.CSV')
sparkDF2 = spark.createDataFrame(event)
sparkDF2.printSchema()
sparkDF2.show()