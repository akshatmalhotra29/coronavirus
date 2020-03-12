# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 21:25:04 2020

@author: Akshat Malhotra
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import requests
#import pyodata
from datetime import datetime, date, time, timezone,timedelta
from sklearn.preprocessing import LabelEncoder
from pyslet import iso8601 as iso
from pyslet.odata2 import metadata as edmx
from pyslet.odata2 import core as core
from pyslet.odata2 import csdl as edm
from pyslet.odata2.memds import InMemoryEntityContainer
from pyslet.odata2.server import Server
from pyslet.py2 import character, output, range3
#------------------For getting and transforming Data from github and converting into json------------------------------------------------#
def getDataset():
    start_date = datetime.strptime("01-22-2020","%m-%d-%Y")
    data= pd.DataFrame(columns=['Province/State','Country/Region','Last Update','Confirmed','Deaths','Recovered'])
    day_before = datetime.now() - timedelta(days=1)
    iter=start_date
    while(iter.date()<=day_before.date()):
        url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
        if(len(str(iter.month))==1):
            url+='0'+str(iter.month)+"-"
        else:
            url+=str(iter.month)+"-"
        if(len(str(iter.day))==1):
            url+='0'+str(iter.day)+"-"
        else:
            url+=str(iter.day)+"-"
        url+=str(iter.year)+".csv"
        s=requests.get(url).content
        c=pd.read_csv(io.StringIO(s.decode('utf-8')))
        data=data.append(c[['Province/State','Country/Region','Last Update','Confirmed','Deaths','Recovered']])
        iter = iter + timedelta(days=1)
    data.columns=['Province/State','Country','Last Update','Confirmed','Deaths','Recovered']
    return data

def transform(data):
    data['Province/State']=data['Province/State'].fillna('Not defined')
    data[['Confirmed','Deaths','Recovered']]=data[['Confirmed','Deaths','Recovered']].fillna(0)
    data['Country']= data['Country'].apply(lambda x: 'China' if x=='Mainland China' else x)
    data['Country']= data['Country'].apply(lambda x: 'Iran' if x=='Iran (Islamic Republic of)' else x)
    data['Country']= data['Country'].apply(lambda x: 'Ivory Coast' if x=='Cote d\'Ivoire' else x)
    data['Country']= data['Country'].apply(lambda x: 'Vietnam' if x=='Viet Nam' else x)
    data['Country']= data['Country'].apply(lambda x: 'South Korea' if x=='Republic of Korea' or x=='Korea, South' else x)
    data['Date'] = pd.to_datetime(data['Last Update'], errors='coerce')
    data=data.drop(['Last Update'],axis=1)
    return data

def convertCumultoLineItem(data):
    data=data.sort_values(by=['Country','Province/State','Date'])
    df=data.groupby(['Country','Province/State'])[['Confirmed','Deaths','Recovered']].diff()
    df = df[['Confirmed','Deaths','Recovered']]
    df.columns=['ConfirmedN','DeathsN','RecoveredN']
    data['Key'] = [i for i in range(data.shape[0])]
    df['Key'] = [i for i in range(df.shape[0])]
    data=data.merge(df,how='inner',on=['Key'])
    data['ConfirmedN'].fillna(data['Confirmed'],inplace=True)
    data['DeathsN'].fillna(data['Deaths'],inplace=True)
    data['RecoveredN'].fillna(data['Recovered'],inplace=True)
    data=data.drop(['Confirmed','Deaths','Recovered'],axis=1)
    data.rename(columns={
        'ConfirmedN':'Confirmed',
        'DeathsN':'Deaths',
        'RecoveredN':'Recovered'},inplace=True)
    data_sub_2=data[['Country']]
    labelencoder = LabelEncoder()
    data_sub_2['Country']=labelencoder.fit_transform(data_sub_2['Country'])
    data_sub_2.columns=['loc_id']
    data=data.join(data_sub_2)
    return data

#------------------------------------For implementing Odata V2------------------------------------------------------------#
def load_metadata():
    """Loads the metadata file from the current directory."""
    doc = edmx.Document()
    with open('C:/dev_stuff/ML Practice/novel-corona-virus-2019-dataset/odata python/Metadata/metadata.xml', 'rb') as f:
        doc.read(f)
    return doc

def test_data(mem_cache):
    with mem_cache.open() as collection:
        for i in range(26):
            e = collection.new_entity()
            e.set_key(str(i))
            e['Value'].set_from_value(character(0x41 + i))
            e['Expires'].set_from_value(
                iso.TimePoint.from_unix_time(time.time() + 10 * i))
            collection.insert_entity(e)

def create_entities(mem_cache,data):
    with mem_cache.open() as collection:
        for i in range(data.shape[0]):
            e = collection.new_entity()
            #e.set_key(data.iloc[i,3])
            e['Key'].set_from_value(str(data.iloc[i,3]))
            e['Province/State'].set_from_value(data.iloc[i,0])
            e['Country'].set_from_value(data.iloc[i,1])
            e['Date'].set_from_value(data.iloc[i,2])
            e['Confirmed'].set_from_value(data.iloc[i,4])
            e['Deaths'].set_from_value(data.iloc[i,5])
            e['Recovered'].set_from_value(data.iloc[i,6])
            e['loc_id'].set_from_value(str(data.iloc[i,7]))
            collection.insert_entity(e)

def create_model(data):
    """Read and write some key value pairs"""
    doc = load_metadata()
    InMemoryEntityContainer(doc.root.DataServices['MemCacheSchema.MemCache'])
    mem_cache = doc.root.DataServices['MemCacheSchema.MemCache.CoronavirusStats']
    create_entities(mem_cache,data)
    with mem_cache.open() as collection:
        for e in collection.itervalues():
            output("%s \n" %
                   (e['Country'].value))
    return (mem_cache,doc)
            
def test_model():
    """Read and write some key value pairs"""
    doc = load_metadata()
    InMemoryEntityContainer(doc.root.DataServices['MemCacheSchema.MemCache'])
    mem_cache = doc.root.DataServices['MemCacheSchema.MemCache.CoronavirusStats']
    test_data(mem_cache)
    with mem_cache.open() as collection:
        for e in collection.itervalues():
            output("%s: %s (expires %s)\n" %
                   (e['Key'].value, e['Value'].value, str(e['Expires'].value)))
            

def init():
    data = getDataset()
    data = transform(data)
    data = convertCumultoLineItem(data)
    data['Country']= data['Country'].apply(lambda x: 'Azerbaijan' if x==' Azerbaijan' else x)
    coord = pd.read_csv("./latlong.csv",encoding = "ISO-8859-1")
    coord.columns=['Code','latitude','longitude','Country']
    test=data.merge(coord,how='left',on=['Country'])
    data = test[['Province/State', 'Country', 'Date', 'Key', 'Confirmed', 'Deaths','Recovered', 'loc_id','latitude', 'longitude']]
    data.to_csv("./newdata.csv")
    #data.to_csv("https://drive.google.com/file/d/1qWONkjTUaMJastaWAy52astwPdMjzZGj/view?usp=sharing")
    #data_json = data.to_json(orient='records',date_format='iso')
    
    #mem_cache,doc=create_model(data)

init()