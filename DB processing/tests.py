# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 11:44:57 2021

@author: oorishko
"""

import pandas as pd
import numpy as np
from sumo.io import sql
from BMRS_flow import bmrs_flow
import time
import urllib.parse
from sqlalchemy import create_engine
from ukpower.data import system_prices as sp
from dbConnection import Mssql
from datetime import datetime, timedelta, date

connection = Mssql(host = "hartreesandbox.lilacenergy.com",
                   user='oorishko@hartreepartners.com',
                   password='WCU=*539f?Sm',
                   database = 'sandbox'
        )

df = None

start_date = date(2020,5,2)
end_date = str(date(2020,5,7))
pull_start_date = str(start_date - timedelta(days=1))
units = ['T_KEAD-1']
df = bmrs_flow(units, start_date = pull_start_date, end_date = end_date)
#try:
#    columns = ['DateTime','bmunit_id','FPN Unadjusted', 'MEL', 'FPN', 'FPN Volume', 'CO Volume', 
#               'VWA Bid Price', 'VWA Offer Price',  'Bid Vol @ 2', 'Bid Vol @ 1', 'Bid Volume',
#               'Offer Volume', 'Bid Income @ 2', 'Bid Income @ 1', 'Bid Income',
#               'Offer Income', 'Offer Income @ 1', 'Offer Income @ 2',
#               'bid_price-2', 'bid_price-1', 'offer_price-1',
#               'offer_price-2', 'bl-2', 'bl-1', 'ol-1', 'ol-2']
#    df = df[columns]
#except:
#    pass
df = df.round(decimals=3)
df = df.replace([np.inf, -np.inf], np.nan)
df = df[df.DateTime >= datetime(start_date.year, start_date.month, start_date.day)]
if df is not None:
    df_dict = df.to_dict('records')
    connection.upsert('BMU_Profiles', df_dict, ['DateTime', 'bmunit_id'])
    #connection._close()
else:
    pass

str(start_date + timedelta(days=1))