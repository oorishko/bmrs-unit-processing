# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 11:01:38 2020

@author: oorishko
"""
import logging
import logging.handlers

logging_message_format  = '%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s'
logging_date_format = '%Y-%m-%d %H:%M:%S'

log_filename = 'S://Power Trading//Ostap//BMRS//BMRS Loggers//GenByFuelType24HInstantDataService.log'

#sets console format. File handler uses separate properties
logging.basicConfig(
            level=logging.DEBUG,
            format='%(message)s',
            datefmt=logging_date_format
            )
#Stop these being verbose (happens when logging is imported). Only show warnings/errors
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
# create logger, handler, formatter
logger = logging.getLogger(__name__)
formatter = logging.Formatter(logging_message_format)

logging_handler = logging.handlers.TimedRotatingFileHandler(filename = log_filename, when = 'midnight', interval = 1)
logging_handler.suffix = '%Y-%m-%d.log'
logging_handler.setFormatter(formatter)
logger.addHandler(logging_handler)

logger.info('Importing packages...')

#import subprocess
#
#subprocess.call(['pip', 'install','xmltodict'])

import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import time
import requests
from io import StringIO
import xmltodict
import warnings
warnings.filterwarnings('ignore')
import site
site.addsitedir(r'S:\Power Trading\Ostap\sumo')
from sumo.io import sql

API_KEY = 'x38n828w1dn2avo'
fuelinst_url = 'https://api.bmreports.com/BMRS/FUELINST/V1'

create_new = False
if not create_new:
    logger.info('Hopefully writing into a table that already exists..')
else:
    logger.info('Planning to create a new table..')
    


def get_db_date(min_or_max = 'MAX', metric = 'GenByFuelType24HInstantDataService'):
    date_query = '''SELECT {}(ValueDate) FROM BMRS_{}'''.format(min_or_max.upper(), metric)
    date_df = sql.get_data(date_query, 'sandbox')
    record = date_df.iloc[0][0]
    print(record)
    return record

def get_generationByFuelType24HInstantDataService(start_date_time = None, end_date_time = None, service_type = 'xml', hours_ahead = '6 hours'):
    # get date, date+1 
    # then startdatetime is date - 30 minutes
    #enddatetime is date 00:00:00
    if start_date_time == None and end_date_time == None:
        sd = get_db_date(metric = 'GenByFuelType24HInstantDataService') - pd.Timedelta('30 minutes')
        start_date_time = str(sd)
        end_date_time = str((pd.Timestamp.now() + pd.Timedelta(hours_ahead)).ceil(freq='s'))
    else:
        start_date_time = str(datetime.strptime(start_date_time, '%Y-%m-%d') - timedelta(minutes = 30))
        end_date_time = str(datetime.strptime(end_date_time, '%Y-%m-%d'))
    print(start_date_time, end_date_time)
    
    params= {
            'APIKey': API_KEY,
            'FromDateTime': start_date_time, 
            'ToDateTime' : end_date_time,
            'ServiceType': service_type
            }
    req = requests.get(fuelinst_url, params = params)
    req_json_tree = xmltodict.parse(req.content)
    resp = req_json_tree['response']
    obj_list = resp['responseBody']['responseList']['item']
    df = pd.DataFrame(obj_list)
    df.rename(columns = {'settlementPeriod': 'settlement_period', 
                         'publishingPeriodCommencingTime': 'ValueDate'
                         }, inplace = True)
    df = df[['ValueDate', 'settlement_period', 'ccgt', 'oil', 'coal', 'nuclear',
       'wind', 'ps', 'npshyd', 'ocgt', 'other', 'intfr', 'intirl', 'intned',
       'intew', 'biomass', 'intnem', 'intelec', 'intifa2', 'intnsl']]
    df['ValueDate'] = pd.to_datetime(df['ValueDate'], format = '%Y-%m-%d %H:%M:%S')
    for col in list(df.columns)[1:]:
        df[col] = df[col].astype(int)
    return df
        
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
        print('Not connected to DB...')
        return None
    else:
        df.to_sql(table_name, engine, if_exists = 'append', chunksize = 100, index  = False, method='multi')


# CONNECT TO DB
engine = connect_to_db()
if engine:
    logger.info('Connected to DB.')
else: 
    logger.info('Not connected to DB!')
    
if create_new:
    create_table_sql = '''  
            CREATE TABLE [dbo].[BMRS_GenByFuelType24HInstantDataService](
                [id] [int] IDENTITY(1,1) PRIMARY KEY,
            	[ValueDate] [datetime] NOT NULL,
                [settlement_period] [int] NOT NULL,
                [ccgt] [int] NOT NULL, 
                [oil] [int] NOT NULL,
                [coal] [int] NOT NULL, 
                [nuclear] [int] NOT NULL,
                [wind] [int] NOT NULL,
                [ps] [int] NOT NULL,
                [npshyd] [int] NOT NULL, 
                [ocgt] [int] NOT NULL,
                [other] [int] NOT NULL, 
                [intfr] [int] NOT NULL, 
                [intirl] [int] NOT NULL, 
                [intned] [int] NOT NULL,
                [intew] [int] NOT NULL, 
                [biomass] [int] NOT NULL, 
                [intnem] [int] NOT NULL,
                [intelec] [int] NOT NULL,
                [intifa2] [int] NOT NULL,
                [intnsl] [int] NOT NULL
            )
            '''
    creation = engine.connect()
    table_creation = creation.execute(create_table_sql)
    
# FOR BIG MANUAL BACKFILLS
# PUT THE CODE in the for loop
#download_start_date = date(2018,1,1)
#download_end_date = date(2020,11,5)
# delta = download_end_date - download_start_date  # as timedelta
#for i in range(delta.days):
#    try:
#        start_date_time = str(download_start_date + timedelta(days=i))
#        end_date_time = str(download_start_date + timedelta(days=i+1))
#        print(start_date_time)
#        print(end_date_time)
start_date_time_info = str(get_db_date(metric = 'GenByFuelType24HInstantDataService') - pd.Timedelta('30 minutes'))
hours_ahead = '6 hours'
end_date_time_info = str((pd.Timestamp.now() + pd.Timedelta(hours_ahead)).ceil(freq='s'))


# DATA GRAB
try:
    df = get_generationByFuelType24HInstantDataService(start_date_time = None, end_date_time = None, service_type = 'xml', hours_ahead = hours_ahead)
    logger.info('Successfully requested data from {} to {} (with {} hours ahead).'.format(start_date_time_info, end_date_time_info, hours_ahead))
except Exception as e:
    logger.info('Could not get data from {} to {} (with {} hours ahead).'.format(start_date_time_info, end_date_time_info, hours_ahead))    
    logger.info('API Error: {}'.format(e))
    
# DB WRITE
try:
    write_to_db(engine, df, 'BMRS_GenByFuelType24HInstantDataService')
    logger.info('Written {} rows to DB'.format(len(df)))
except Exception as e:
    logger.info('Failed to write to DB!')
    logger.info('DB Error: {}'.format(e))
    
# Duplicates Handling    
try:
    remove_duplicates = '''
                        Delete from [sandbox].[dbo].[BMRS_GenByFuelType24HInstantDataService] WHERE [id] in 
                        (
                        SELECT max([id])
                        FROM  [sandbox].[dbo].[BMRS_GenByFuelType24HInstantDataService]
                        GROUP BY [ValueDate], [settlement_period]
                        HAVING Count(*) >1
                        )
                        '''
    connection = engine.connect()
    duplicates = connection.execute(remove_duplicates)
    logger.info('Removed {} duplicates.'.format(str(duplicates.rowcount)))
except Exception as e:
    logger.info('Failed to remove duplicates!')
    logger.info('Duplicates Error: {}'.format(e))

