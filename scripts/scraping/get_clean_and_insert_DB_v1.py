#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import subprocess
import sys
from cassandra.cluster import Cluster
from random import choice
from shutil import move
from os import path, mkdir
from time import time

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


def nettoyage_mentions(file):
    col = ['GlobalEventID', 'EventTimeDate', 'MentionTimeDate', 'MentionType',
           'MentionSourceName', 'MentionIdentifier', 'SentenceID',
           'Actor1CharOffset', 'Actor2CharOffset', 'ActionCharOffset', 'InRawText',
           'Confidence', 'MentionDocLen', 'MentionDocTone',
           'MentionDocTranslationInfo']
    df = pd.read_csv(file, sep="\t", usecols=list(range(len(col))), names=col, encoding="ISO-8859-1",keep_default_na=False)
    # Evénements de 2021, et seulement les colonnes qui nous intéressent
    df = df.loc[df["EventTimeDate"] >= 20210000000000][['GlobalEventID','MentionDocTranslationInfo']]
    # Langue de l'article qui mentionne l'événement
    df["MentionDocTranslationInfo"] = df["MentionDocTranslationInfo"].fillna("eng")
    df["MentionDocTranslationInfo"] = df["MentionDocTranslationInfo"].apply(lambda x: x if x=="eng" else x[6:9])
    df.to_csv(file, sep="\t", header=False)
    return df 


def nettoyage_gkg(file):
    col = ['GKGRECORDID','V2.1DATE','V2SOURCECOLLECTIONIDENTIFIER',
           'V2SOURCECOMMONNAME','V2DOCUMENTIDENTIFIER','V1COUNTS','V2.1COUNTS',
           'V1THEMES','V2ENHANCEDTHEMES','V1LOCATIONS','V2ENHANCEDLOCATIONS',
           'V1PERSONS','V2ENHANCEDPERSONS','V1ORGANIZATIONS',
           'V2ENHANCEDORGANIZATIONS','V1.5TONE','V2.1ENHANCEDDATES','V2GCAM',
           'V2.1SHARINGIMAGE','V2.1RELATEDIMAGES','V2.1SOCIALIMAGEEMBEDS',
           'V2.1SOCIALVIDEOEMBEDS','V2.1QUOTATIONS','V2.1ALLNAMES','V2.1AMOUNTS',
           'V2.1TRANSLATIONINFO','V2EXTRASXML']
    df = pd.read_csv(file, sep="\t", usecols=list(range(len(col))), names=col, encoding="ISO-8859-1")
    # éléments de 2021
    df = df.loc[df["V2.1DATE"] > 2021_00_00_00_00_00]
    # Dates au format int(YYYYMMDD) (sans les heures minutes secondes)
    df["V2.1DATE"] = df["V2.1DATE"].apply(lambda x: int(str(x)[0:8]))
    # Extraction de la langue originale
    df["V2.1TRANSLATIONINFO"] = df["V2.1TRANSLATIONINFO"].fillna("eng")
    df["V2.1TRANSLATIONINFO"] = df["V2.1TRANSLATIONINFO"].apply(lambda x: x if x=="eng" else x[6:9])

    # Colonnes utiles uniquement
    df = df[['V2.1DATE', 'V2SOURCECOMMONNAME',
             'V1THEMES','V1LOCATIONS',
             'V1.5TONE','V2.1TRANSLATIONINFO']]
    #Extraction des valeurs de TONE intéressantes
    df[["score","positif","negatif"]] = df["V1.5TONE"].str.split(pat=",", expand=True)[[0,1,2]]
    df.drop(labels="V1.5TONE",axis=1, inplace=True)

    # Premier theme de la liste
    df["V1THEMES"].apply(lambda x: str(x).split(";")[0])

    #Extraction du CountryCode uniquement pour LOCATION
    idx = df.loc[df["V1LOCATIONS"].notna()].index
    df.loc[idx, "V1LOCATIONS"] = df.loc[df["V1LOCATIONS"].notna()]["V1LOCATIONS"].apply(lambda x: x.split("#")[2])
    df.fillna("", inplace=True)
    df.to_csv(file, sep="\t", header=False)
    return df


def insert_mentions(globaleventid: int ,language: str):
    session.execute(
        """
        INSERT INTO mentions (globaleventid, language)
        VALUES (%(globaleventid)s, %(language)s)
        """,
        {'globaleventid': globaleventid, 'language': language}
    )

def insert_event(
                 globaleventid : int,
                 date : str,
                 goldsteinscale : float,
                 actiongeocountrycode : str):
    session.execute(
        """
        INSERT INTO event (globaleventid, date,goldsteinscale,actiongeocountrycode )
        VALUES (%s, %s, %s, %s)
        """,
        (globaleventid, date,  goldsteinscale, actiongeocountrycode)
    )

def insert_kb(
              date : int,
              source : str,
              theme : str,
              location : str,
              translation : str,
              score : float,
              positif: float,
              negatif: float):
    session.execute(
        """
        INSERT INTO kb (date, source, theme, location, translation, score, positif, negatif)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s )
        """,
        (date, source, theme, location, translation, score, positif, negatif)
    )
    
def loginDB(coordinator:str,keyspace:str):
    ## Connection au cluster
    cluster = Cluster([coordinator]) # coordinateur must be name/IP of one node (ex 'tp-hadoop-1')
    session = cluster.connect(keyspace)  # connect to keyspace on DB (ex: 'test')
    return session

    
#nodes = ['tp-hadoop-1', 'tp-hadoop-2','tp-hadoop-5', 'tp-hadoop-6', 'tp-hadoop-7', 'tp-hadoop-21']
failFiles =[]    
session = loginDB('tp-hadoop-2','test')

for file in files:
    print(f'file:= {file}')
    dl_unzip(url, file, "/tmp/tests")
    file = "/tmp/tests"+file.strip(".zip")
    #coord = choice(nodes) # choix d'un coordinateur different pour ne pas saturer le même ordi
    #session = loginDB(coord,'test')
    if not path.exists('/tmp/tests/traiter'):
        mkdir('/tmp/tests/traiter')
    if "export" in file:
        df=nettoyage_event(file)
        try:
            df.apply(lambda x: insert_event(x[0], x[1],x[3],x[4]),axis=1)
        except:
            print(f"## Error:can't write {file} on CASSANDRA DataBase ")
            failFiles.append(file)
            continue
        move(file, '/tmp/tests/traiter/')

    elif "mentions" in file:
        df=nettoyage_mentions(file)
        try:
            df.apply(lambda x: insert_mentions(x[0],x[1]),axis=1)
        except:
            print(f"## Error:can't write {file} on CASSANDRA DataBase ")
            failFiles.append(file)
            continue
        move(file, '/tmp/tests/traiter/')
    elif "gkg" in file:
        df=nettoyage_gkg(file)
        try:
            df.apply(lambda x: insert_kb(x[0],x[1],x[2],x[3],x[4],float(x[5]), float(x[6]), float(x[7])),axis=1)
        except:        
            print('Error gkg')
            failFiles.append(file)
            continue
        move(file,'/tmp/tests/traiter')
    else: pass      



t = time()-t0
m = t//60
s = t%60
if failFiles:
    print('Error: FAIL TO WRITE FOLLOWING FILES TO CASSANDRA:')
    print(failFiles)

print(f'\nDUREE EXECUTION: {m}min:{s}sec')

