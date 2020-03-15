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
def getConfirmed():
    data=pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv")
    cnt=data[['Province/State','Country/Region','Lat','Long']]
    cnt['Key']=cnt.index
    temp=data.drop(['Province/State','Country/Region','Lat','Long'],axis=1)
    lst=[]
    for i in range(temp.shape[0]):
        for j in range(temp.shape[1]):
            tls=[i,temp.columns[j],temp.iloc[i,j],i]
            lst.append(tls)
    df_lst=pd.DataFrame(lst)
    df_lst.columns=['Key','Date','Confirmed','loc_id']
    datas = cnt.merge(df_lst,how='inner',on=['Key'])
    datas.columns=['Province/State','Country','latitude','longitude','Key','Date','Confirmed','loc_id']
    return datas

def getDeaths():
    data=pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv")
    cnt=data[['Province/State','Country/Region','Lat','Long']]
    cnt['Key']=cnt.index
    temp=data.drop(['Province/State','Country/Region','Lat','Long'],axis=1)
    lst=[]
    for i in range(temp.shape[0]):
        for j in range(temp.shape[1]):
            tls=[i,temp.columns[j],temp.iloc[i,j],i]
            lst.append(tls)
    df_lst=pd.DataFrame(lst)
    df_lst.columns=['Key','Date','Deaths','loc_id']
    datas = cnt.merge(df_lst,how='inner',on=['Key'])
    datas.columns=['Province/State','Country','latitude','longitude','Key','Date','Deaths','loc_id']
    return datas

def getRecovered():
    data=pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv")
    cnt=data[['Province/State','Country/Region','Lat','Long']]
    cnt['Key']=cnt.index
    temp=data.drop(['Province/State','Country/Region','Lat','Long'],axis=1)
    lst=[]
    for i in range(temp.shape[0]):
        for j in range(temp.shape[1]):
            tls=[i,temp.columns[j],temp.iloc[i,j],i]
            lst.append(tls)
    df_lst=pd.DataFrame(lst)
    df_lst.columns=['Key','Date','Recovered','loc_id']
    datas = cnt.merge(df_lst,how='inner',on=['Key'])
    datas.columns=['Province/State','Country','latitude','longitude','Key','Date','Recovered','loc_id']
    return datas

def transform(data):
    data['Date']=pd.to_datetime(data['Date'], errors='coerce')
    data['Country']= data['Country'].apply(lambda x: 'Azerbaijan' if x==' Azerbaijan' else x)
    data['Province/State']=data['Province/State'].fillna('Not defined')
    """mask = (data['Province/State'] == 'Not defined') & (data['Country'] == 'France')
    data['Province/State'][mask] = 'France'
    mask = (data['Province/State'] == 'Not defined') & (data['Country'] == 'UK')
    data['Province/State'][mask] = 'UK'
    data['Country']= data['Country'].apply(lambda x: 'United Kingdom' if x=='UK' else x)
    data[['Confirmed','Deaths','Recovered']]=data[['Confirmed','Deaths','Recovered']].fillna(0)"""
    # data['Country']= data['Country'].apply(lambda x: 'China' if x=='Mainland China' else x)
    # data['Country']= data['Country'].apply(lambda x: 'Iran' if x=='Iran (Islamic Republic of)' else x)
    #data['Country']= data['Country'].apply(lambda x: 'Ivory Coast' if x=='Cote d\'Ivoire' else x)
    #data['Country']= data['Country'].apply(lambda x: 'Vietnam' if x=='Viet Nam' else x)
    #data['Country']= data['Country'].apply(lambda x: 'South Korea' if x=='Republic of Korea' or x=='Korea, South' else x)
    #data['Date'] = pd.to_datetime(data['Last Update'], errors='coerce')
    #data=data.drop(['Last Update'],axis=1)
    return data

def convertCumultoLineItem(data):
    #data=data.sort_values(by=['Country','Province/State','Date'])
    df=data.groupby(['Country','Province/State','Key','latitude','longitude','loc_id'])[['Confirmed','Deaths','Recovered']].diff()
    df = df[['Confirmed','Deaths','Recovered']]
    df.columns=['ConfirmedN','DeathsN','RecoveredN']
    data['KeyN'] = [i for i in range(data.shape[0])]
    df['KeyN'] = [i for i in range(df.shape[0])]
    data=data.merge(df,how='inner',on=['KeyN'])
    data['ConfirmedN'].fillna(data['Confirmed'],inplace=True)
    data['DeathsN'].fillna(data['Deaths'],inplace=True)
    data['RecoveredN'].fillna(data['Recovered'],inplace=True)
    data=data.drop(['Confirmed','Deaths','Recovered'],axis=1)
    data.rename(columns={
        'ConfirmedN':'Confirmed',
        'DeathsN':'Deaths',
        'RecoveredN':'Recovered'},inplace=True)
    #data_sub_2=data[['Country']]
    #labelencoder = LabelEncoder()
    #data_sub_2['Country']=labelencoder.fit_transform(data_sub_2['Country'])
    #data_sub_2.columns=['loc_id']
    #data=data.join(data_sub_2)
    return data

#------------------------------------For implementing Odata V2------------------------------------------------------------#
def load_metadata():
    """Loads the metadata file from the current directory."""
    doc = edmx.Document()
    with open('./metadata.xml', 'rb') as f:
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
    data_c = getConfirmed()
    data_c.rename(columns={"Key":"KeyC"},inplace=True)
    data_d = getDeaths()
    data_d=data_d[['Key','Deaths']]
    data_r = getRecovered()
    data_r=data_r[['Key','Recovered']]
    data=pd.concat([data_c,data_d,data_r],axis=1)
    data.drop(['Key'],axis=1,inplace=True)
    data.rename(columns={"KeyC":"Key"},inplace=True)
    data = transform(data)
    data = convertCumultoLineItem(data)
    data=data.drop(['Key'],axis=1)
    data=data.rename(columns={
        "KeyN":"Key"
    })
    #data = test[['Province/State', 'Country', 'Date', 'Key', 'Confirmed', 'Deaths','Recovered', 'loc_id','latitude', 'longitude']]
    data.to_csv("./newdata.csv")
    #data.to_csv("https://drive.google.com/file/d/1qWONkjTUaMJastaWAy52astwPdMjzZGj/view?usp=sharing")
    #data_json = data.to_json(orient='records',date_format='iso')
    
    #mem_cache,doc=create_model(data)

init()