# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 19:03:28 2021

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
import warnings
warnings.filterwarnings("ignore")
                        
def connect_to_db():
    Connection_Server_Name = 'hartreesandbox.lilacenergy.com'
    Connection_Database_Name = 'sandbox'
    uid = 'oorishko@hartreepartners.com'
    pwd = 'WCU=*539f?Sm'
    
    params_sql = urllib.parse.quote_plus('Driver={SQL Server};'
                                 'Server=' + Connection_Server_Name + ';'
                                 'Database=' + Connection_Database_Name + ';'
                                 'uid=' + uid + ';'
                                 'pwd=' + pwd)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params_sql)
    return engine

def write_to_db(engine, df, table_name):    
    if not engine:
        return None
    else:
        st = time.time()
        df.to_sql(table_name, engine, if_exists = 'append', chunksize = 50, index  = False, method='multi')
        et = time.time()
        print('Written {} rows to into {} in {:4.2f} seconds.'.format(len(df), table_name, et-st))

table_name = 'CCGT_BMRS_Processing'
#
#
#wind_units_lcp = sql.get_data('''
#                              SELECT DISTINCT(plant_id) FROM LCP_BM_Leaderboard
#                              WHERE plant_fuel = 'Wind' 
#                              ''', 'sandbox')
#
#configs = {'PWD' : urllib.parse.quote_plus("vC8?LgPB+X\!")}
#print('Connecting to DB..')
#engine = create_engine(r'''postgresql://oorishko@hartreepartners.com:'''+configs['PWD']+r'''@htre-euenergypower-postgres-instance-1-eu-west-1c.clfv9xeyss2b.eu-west-1.rds.amazonaws.com/kitchentap''')
#connection = engine.connect()
#print('Connected to DB..')
#
#wind_units_enappsys_sql =   """                
#                            SELECT DISTINCT(entity_id) FROM "Enappsys_BMU_Metadata"
#                            WHERE fuel = 'Wind'
#                            """
#
#wind_units_enappsys = pd.read_sql(wind_units_enappsys_sql, connection)
#
#wind_units = list(set(wind_units_enappsys.entity_id).union(set(wind_units_lcp.plant_id)) - set(['2__PPGEN001','T_GANW-11','T_GLWSW-1','T_GNFSW-2','T_HOWAO-1','T_HRSTW-1','T_SHRSW-1','T_WHILW-2']))

#big_boy_units = ['T_CARR-1','T_CARR-2','T_DAMC-1','E_KLYN-A-1','T_MEDP-1', 'T_MRWD-1','T_PEHE-1', 
#                 'T_SEAB-1','T_SEAB-2', 'T_RYHPS-1','T_KEAD-1','T_LAGA-1','T_HUMR-1', 'T_CNQPS-4',
#                 'T_CNQPS-1','T_SPLN-1','T_WBURB-1', 'T_WBURB-2', 'T_WBURB-3', 'T_DRAXX-4']
big_boy_units = ['T_HUMR-1','T_RYHPS-1','T_DAMC-1','E_KLYN-A-1','T_LAGA-1',]

df = bmrs_flow(big_boy_units)
df.replace([np.inf, -np.inf], np.nan, inplace = True)
sandbox_engine = connect_to_db()
write_to_db(sandbox_engine, df, table_name)
 