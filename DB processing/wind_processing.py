# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 19:03:28 2021

@author: oorishko
"""
#from __future__ import division
#import sys
import pandas as pd
import numpy as np
from sumo.io import sql
#from BMRS_flow_multiple_bmus_mel import bmrs_flow
import time
import urllib.parse
from sqlalchemy import create_engine
from ukpower.data import system_prices as sp
import warnings
warnings.filterwarnings("ignore")

import workers


wind_units_lcp = sql.get_data('''
                              SELECT DISTINCT(plant_id) FROM LCP_BM_Leaderboard
                              WHERE plant_fuel = 'Wind' 
                              ''', 'sandbox')

configs = {'PWD' : urllib.parse.quote_plus("vC8?LgPB+X\!")}
print('Connecting to DB..')
engine = create_engine(r'''postgresql://oorishko@hartreepartners.com:'''+configs['PWD']+r'''@htre-euenergypower-postgres-instance-1-eu-west-1c.clfv9xeyss2b.eu-west-1.rds.amazonaws.com/kitchentap''')
connection = engine.connect()
print('Connected to DB..')

wind_units_enappsys_sql =   """                
                            SELECT DISTINCT(entity_id) FROM "Enappsys_BMU_Metadata"
                            WHERE fuel = 'Wind'
                            """

wind_units_enappsys = pd.read_sql(wind_units_enappsys_sql, connection)

wind_units = list(wind_units_enappsys.entity_id)
#wind_units = ['T_KEAD-1']
for wind_unit in wind_units:
    st = time.time()
    workers.process_and_write_wind_db(wind_unit,start_date = '2021-03-01', end_date = '2021-03-19')
    et=time.time()
    print('Took {} minutes to process and write {}.'.format((et-st)/60, wind_unit))
#big_boy_units = ['T_CARR-1','T_CARR-2','T_DAMC-1','E_KLYN-A-1','T_MEDP-1', 'T_MRWD-1','T_PEHE-1', 
#                 'T_SEAB-1','T_SEAB-2', 'T_RYHPS-1','T_KEAD-1','T_LAGA-1','T_HUMR-1', 'T_CNQPS-4',
#                 'T_CNQPS-1','T_SPLN-1','T_WBURB-1', 'T_WBURB-2', 'T_WBURB-3']

#for i in range(len(wind_units)):
#def process_and_write()
#    df = bmrs_flow(wind_units[i], start_date = '2016-01-01', end_date = '2021-03-01')
#    df = df[['DateTime','bmunit_id','FPN Unadjusted', 'MEL', 'FPN', 'FPN Volume', 'CO Volume', 
#               'VWA Bid Price', 'VWA Offer Price',  'Bid Vol @ 2', 'Bid Vol @ 1', 'Bid Volume',
#               'Offer Volume', 'Bid Income @ 2', 'Bid Income @ 1', 'Bid Income',
#               'Offer Income', 'Offer Income @ 1', 'Offer Income @ 2',
#               'bid_price-2', 'bid_price-1', 'offer_price-1',
#               'offer_price-2', 'bl-2', 'bl-1', 'ol-1', 'ol-2']].round(decimals=3)
#    df = df.replace([np.inf, -np.inf], np.nan)
#    sandbox_engine = connect_to_db()
#    write_to_db(sandbox_engine, df, table_name)


#print(wind_units[:5])
#a_pool = multiprocessing.Pool()
#start_time = time.time()
#for i, _ in enumerate(a_pool.imap(workers.process_and_write, iter(wind_units[:5])), 1):
#    sys.stderr.write('\rdone {0:%}'.format(i/len(wind_units[:5])))
#end_time = time.time()
#print('{:.2f}'.format(end_time-start_time/60))