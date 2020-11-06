import sys
import requests
import pyodbc
import numpy as np
import os
import pandas as pd
import time
from datetime import datetime, date, timedelta
import logging
import io
import sentry_sdk

def getWeather(date1, date2, location):
    url = "http://coagmet.colostate.edu/cgi-bin/web_services.pl?type=hourly&sids="+location+"&sdate=" + \
        str(date1) + "&edate=" + str(date2) + \
        "&elems=tmean,rh,sr,ws,wind_vec,pp,st5,st15,gust,gustdir"
    df = pd.read_csv(url, names=['StationID', 'Valid_time', 'Temperature_c', 'Relative_Humidity', 'Incoming_Solar_Radiation', 'Wind_Speed_ms',
                                 'Wind_Direction_degree', 'Hourly_Precipitation_cm', '2inch_Soil_Temperature_c', '6inch_Soil_Temperature_c', 'Wind_Gust_ms', 'Wind_Gust_Direction_degree'], skiprows=[0], skipfooter=2, engine="python")

    #print('request: \n')
    #print(df)
    df['Timezone'] = 'MT'
    df['Hourly_Precipitation_cm'] = df.apply(
        lambda row: mm_to_cm(row, 'Hourly_Precipitation_cm'), axis=1)
    df['Wind_Direction'] = df.apply(
        lambda row: get_wind_dir(row, 'Wind_Direction_degree'), axis=1)
    df['Relative_Humidity'] = df['Relative_Humidity']*100
    df['Wind_Gust_Direction'] = df.apply(
        lambda row: get_wind_dir(row, 'Wind_Gust_Direction_degree'), axis=1)
    df['Name'] = df.apply(get_name, axis=1)
    df['Temperature_f'] = df.apply(
        lambda row: c_to_f(row, 'Temperature_c'), axis=1)
    df['2inch_Soil_Temperature_f'] = df.apply(
        lambda row: c_to_f(row, '2inch_Soil_Temperature_c'), axis=1)
    df['6inch_Soil_Temperature_f'] = df.apply(
        lambda row: c_to_f(row, '6inch_Soil_Temperature_c'), axis=1)
    df['Wind_Speed_mph'] = df.apply(
        lambda row: ms_to_mph(row, 'Wind_Speed_ms'), axis=1)
    df['Wind_Gust_mph'] = df.apply(
        lambda row: ms_to_mph(row, 'Wind_Gust_ms'), axis=1)
    df['Hourly_Precipitation_in'] = df.apply(
        lambda row: cm_to_in(row, 'Hourly_Precipitation_cm'), axis=1)

    df = df.round({"Temperature_f": 3, "Relative_Humidity": 1, "Incoming_Solar_Radiation": 3,
                   "Hourly_Precipitation_in": 2, "Wind_Speed_mph": 2, "2inch_Soil_Temperature_f": 3, "6inch_Soil_Temperature_f": 3, "Wind_Gust_mph": 3, "Temperature_c": 3,
                   "Hourly_Precipitation_cm": 2, "Wind_Speed_ms": 2, "2inch_Soil_Temperature_c": 3, "6inch_Soil_Temperature_c": 3, "Wind_Gust_ms": 3})

    df['Temperature_f'] = df['Temperature_f'].astype(str)
    df['Temperature_c'] = df['Temperature_c'].astype(str)
    df['Valid_time'] = df['Valid_time'].astype(str)
    df['Relative_Humidity'] = df['Relative_Humidity'].astype(str)
    df['Incoming_Solar_Radiation'] = df['Incoming_Solar_Radiation'].astype(
        str)
    df['Hourly_Precipitation_cm'] = df['Hourly_Precipitation_cm'].astype(str)
    df['Hourly_Precipitation_in'] = df['Hourly_Precipitation_in'].astype(str)
    df['Wind_Speed_ms'] = df['Wind_Speed_ms'].astype(str)
    df['Wind_Speed_mph'] = df['Wind_Speed_mph'].astype(str)
    df['Wind_Gust_ms'] = df['Wind_Gust_ms'].astype(str)
    df['Wind_Gust_mph'] = df['Wind_Gust_mph'].astype(str)
    df['2inch_Soil_Temperature_c'] = df['2inch_Soil_Temperature_c'].astype(str)
    df['2inch_Soil_Temperature_f'] = df['2inch_Soil_Temperature_f'].astype(str)
    df['6inch_Soil_Temperature_c'] = df['6inch_Soil_Temperature_c'].astype(
        str)
    df['6inch_Soil_Temperature_f'] = df['6inch_Soil_Temperature_f'].astype(
        str)

    print(df)

    if df.empty == True:
        print("Data frame is empty!" + location + ' is not reporting!')
    else:
        return(df)

#functions for unit conversions
def get_name(row):
    ix = row['StationID']
    if ix == 'wry02':
        return 'Wray'
    elif ix == 'yum02':
        return 'Yuma'
    else:
        return 'unknown'


def c_to_f(row, col):
    if np.isnan(row[col]) == False:
        output = ((float(row[col])*9/5)+32)
        return output
    else:
        return row[col]


def mm_to_cm(row, col):
    if np.isnan(row[col]) == False:
        return row[col]/10
    else:
        return row[col]


def cm_to_in(row, col):
    if np.isnan(row[col]) == False:
        return row[col]/2.54
    else:
        return row[col]


def ms_to_mph(row, col):
    if np.isnan(row[col]) == False:
        return row[col]*2.23694
    else:
        return row[col]


def get_wind_dir(row, col):
    if np.isnan(row[col]) == False:
        dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        ix = int((row[col] + 11.25)/22.5-0.02)
        return dirs[ix % 16]
    else:
        return row[col]

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
today = datetime.strftime(datetime.now(), '%Y-%m-%d')
date1 = yesterday
date2 = today
location = ['yum02']

writeFile = r'C:\Users\rkeenan\OneDrive - Aurora Cooperative\Documents\Development\UpdatedCoMesonetScraper\CoMesonetDataTest.csv'

for i in location:
    df_scrape = getWeather(date1, date2, i)
    if df_scrape.empty == True:
        print("Empty data frame!")
    else:
        df_scrape.to_csv(writeFile, sep="\t") 
        #print("df_scrape" + df_scrape)
        for j in range(0, len(df_scrape)):
            print('*********************The issue is below')
            df_sub = df_scrape[j : (j+1)]
            #to_db(df_sub)
            print("df_sub: " + df_sub)
