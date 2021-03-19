# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 10:38:27 2021

@author: oorishko
"""
import pandas as pd
import numpy as np
from sumo.io import sql
from BMRS_flow_multiple_bmus_mel import bmrs_flow
import time
import urllib.parse
from sqlalchemy import create_engine
from ukpower.data import system_prices as sp
from dbConnection import Mssql


#def connect_to_db():
#    Connection_Server_Name = 'hartreesandbox.lilacenergy.com'
#    Connection_Database_Name = 'sandbox'
#    uid = 'oorishko@hartreepartners.com'
#    pwd = 'WCU=*539f?Sm'
#    
#    params_sql = urllib.parse.quote_plus('Driver={SQL Server};'
#                                 'Server=' + Connection_Server_Name + ';'
#                                 'Database=' + Connection_Database_Name + ';'
#                                 'uid=' + uid + ';'
#                                 'pwd=' + pwd)
#    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params_sql)
#    return engine
#
#def write_to_db(engine, df, table_name):    
#    if not engine:
#        return None
#    else:
#        st = time.time()
#        df.to_sql(table_name, engine, if_exists = 'append', chunksize = 50, index  = False, method='multi')
#        et = time.time()
#        print('Written {} rows to into {} in {:4.2f} seconds.'.format(len(df), table_name, et-st))


def process_and_write_wind_db(wind_unit,start_date, end_date):
    df = None
    try:
#        start_date = '2017-01-01'
#        end_date = '2021-03-16'
#        units = ['T_KEAD-1','T_PEHE-1','T_MRWD-1','T_MEDP-1', 'T_LAGA-1']
        df = bmrs_flow(wind_unit, start_date = start_date, end_date = end_date)
        try:
            columns = ['DateTime','bmunit_id','FPN Unadjusted', 'MEL', 'FPN', 'FPN Volume', 'CO Volume', 
                       'Bid Vol @ 2', 'Bid Vol @ 1', 'Bid Volume','Offer Volume', 
                       'bid_price-2', 'bid_price-1', 'offer_price-1',
                       'offer_price-2', 'bl-2', 'bl-1', 'ol-1', 'ol-2']
            df = df[columns]
        except:
            pass
        df = df.round(decimals=3)
        df = df.replace([np.inf, -np.inf], np.nan)
    except ValueError as e:
        print(e)
        pass
    if df is not None:
        connection = Mssql(host = "hartreesandbox.lilacenergy.com",
                           user='oorishko@hartreepartners.com',
                           password='WCU=*539f?Sm',
                           database = 'sandbox'
                )
        connection.upsert('Wind_BM_profile', df, ['DateTime', 'bmunit_id'])
    else:
        pass
    
#st = time.time()
#for wind_unit in wind_units[:5]:
#    process_and_write(wind_unit)
#et=time.time()
#print((et-st)/60)