# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 21:25:04 2020

@author: Akshat Malhotra
"""

from flask import Flask, request, Response
import os
from concurrent.futures import ThreadPoolExecutor
from cfenv import AppEnv
from flask import abort
import json
from flask import make_response
from flask import jsonify

import pandas as pd
import numpy as np
import io
from sklearn.preprocessing import LabelEncoder
import requests
import pyslet
from datetime import datetime, date, time, timezone,timedelta


import logging
import threading
import time



executor = ThreadPoolExecutor()

app = Flask(__name__)
#port='8083'
#port = os.getenv("PORT")

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
    return data

def convertCumultoLineItem(data):
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
    return data
            
@app.route('/')
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
    #data.to_csv("./newdata.csv")
    return data.to_json()
if __name__ == '__main__':
    app.run()