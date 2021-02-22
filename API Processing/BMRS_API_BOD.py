# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 18:11:29 2020

@author: oorishko
"""
import logging
import logging.handlers

logging_message_format  = '%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s'
logging_date_format = '%Y-%m-%d %H:%M:%S'

log_filename = 'S://Power Trading//Ostap//BMRS//BMRS Loggers//BOD.log'

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

logging_handler = logging.handlers.TimedRotatingFileHandler(filename = log_filename, when = 'W0', interval = 1)
logging_handler.suffix = '%Y-%m-%d.log'
logging_handler.setFormatter(formatter)
logger.addHandler(logging_handler)

logger.info('Importing packages...')

import subprocess

subprocess.call(['pip', 'install','xmltodict'])

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
from kpow.io import sql

API_KEY = 'x38n828w1dn2avo'
API_VERSION = 'V3'
phybmdata_url = 'https://api.bmreports.com/BMRS/PHYBMDATA/V1'
bod_url = 'https://api.bmreports.com/BMRS/BOD/V1'
hist_acc_url = 'https://api.bmreports.com/BMRS/HISTACCEPTS/V1'
derived_url = 'https://api.bmreports.com/BMRS/DERBMDATA/V1'

create_new = False
error_list = []
#
#long_days_list = ['2016-10-30','2017-10-29','2018-10-28', '2019-10-27', '2020-10-25', '2021-10-31', 
#                  '2022-10-30', '2023-10-29', '2024-10-27', '2025-10-26', '2026-10-25', '2027-10-31', 
#                  '2028-10-29', '2029-10-28']
#long_sp_list = [49,50]
#date_today = str(datetime.now().date())
#if date_today in long_days_list:
#    skip_list = [(x,date_today) for x in long_sp_list]
#else: 
skip_list = []
#ldl = ['2019-10-27', '2020-10-25']
#spl = [49,50]
#skip_list = [(x, y) for x in spl for y in ldl]   
 
#download_start_date = date(2019,1,1)
#download_end_date = date(2020,11,8)

def get_bmu_list():
    bmu_query = '''SELECT [BM Unit ID] FROM BM_UNIT_MASTER_META'''
    bmu_df = sql.get_data(bmu_query, 'sandbox')
    return list(bmu_df['BM Unit ID'])

def get_db_date(min_or_max = 'MAX', metric = 'FPN'):
    date_query = '''SELECT {}(to_time) FROM BMRS_{}'''.format(min_or_max.upper(), metric)
    date_df = sql.get_data(date_query, 'sandbox')
    record = date_df.iloc[0][0]
    return pd.Timestamp(record)

def get_settlement_date_period_pair(start_date, 
                                    end_date = 'latest', 
                                    hours_ahead = 2):
    # agree on the end date
    # start date is determined by a parent function
    if end_date == 'latest':
        end_date = pd.Timestamp.now() + pd.Timedelta(hours=hours_ahead)
    elif end_date == 'hours ahead':
        end_date = start_date + pd.Timedelta(hours=hours_ahead)
    else:
        end_date = end_date
    # get range of 30 minute intervals for settlement periods
    end_date = pd.Timestamp(end_date) - pd.Timedelta('30 minutes')
    start_date = pd.Timestamp(start_date)
    hh_range = pd.date_range(start = start_date, end = end_date, freq = '30T')
    
    # get date timestamp and SP in a tuple
    hh_date = hh_range.date
    respective_hh_sps = (hh_range.hour*2 + hh_range.minute/30)+1
    
    return list(zip(hh_date, respective_hh_sps))

###################################### BID OFFER LEVEL DATA ######################################################

# Bid Offer Level Data
def get_bod_xml(start_date = None,
                end_date = None,
                bm_unit_id=None,
                bm_unit_type=None,
                lead_party_name=None,
                ngc_bm_unit_name=None,
                settlement_period = None,
                service_type='xml', 
                hours_ahead = 1):
    global skip_list
    # handle the api for BOD data
    logger.info('Querying start and end date times')
    if start_date == None and end_date == None:
        end_date = 'latest'
        print('Ending Bid Offer Level data grab at {}'.format(end_date))
        
        start_date = str(get_db_date('MAX','BOD'))
        print('Starting Bid Offer Level data grab at {}'.format(start_date))  
    # collect DataFrames from all dates together
    logger.info('Queried DB. Starting at {}'.format(start_date))
    bod_df_list = []
    if settlement_period == None:
        logger.info('Getting a range of settlement periods')
        date_sp_pairs = get_settlement_date_period_pair(start_date, end_date, hours_ahead = hours_ahead)
        #bmu_list = get_bmu_list()

        params = {
                'APIKey': API_KEY,
                'BMUnitID' : bm_unit_id,
                'BMUnitType': bm_unit_type,
                'LeadPartyName': lead_party_name,
                'NGCBMUnitName': ngc_bm_unit_name,
                'ServiceType': service_type
                }
    
        st = time.time()
    #    for bmu in bmu_list:
    #        params['BMUnitID'] = bmu
        for d, sp in date_sp_pairs:
            params['SettlementDate'] = d.strftime(format = '%Y-%m-%d')
            params['SettlementPeriod'] = int(sp)
            # perform get request
            bod_request = requests.get(bod_url, params=params)
    
            try:
                # parse request xml content 
                bod_json_tree = xmltodict.parse(bod_request.content)
                # handle no data for BOD
                bod_object_list = bod_json_tree['response']['responseBody']['responseList']['item']
                bod_curr_df = pd.DataFrame(bod_object_list)
                bod_curr_df['settlement_period'] = int(sp)
                bod_df_list.append(bod_curr_df)
                print('Added {} rows from settlement period {} for {}'.format(len(bod_curr_df), sp, d.strftime(format = '%Y-%m-%d')))
            except Exception as e:
                skip_list.append((int(sp), d.strftime(format = '%Y-%m-%d')))
                logger.info('Skipped settlement period {} for {}'.format(sp, d.strftime(format = '%Y-%m-%d')))
                print(e)
                continue
        et = time.time()
                
        bod_df = pd.concat(bod_df_list)
        logger.info("Finished all requests successfully in {:4.2f} seconds".format(et - st))
    else:
        logger.info('Handling a specific settlement period')
        sp = settlement_period
        params = {
                'APIKey': API_KEY,
                'BMUnitID' : bm_unit_id,
                'BMUnitType': bm_unit_type,
                'LeadPartyName': lead_party_name,
                'NGCBMUnitName': ngc_bm_unit_name,
                'ServiceType': service_type,
                'SettlementPeriod' : settlement_period,
                'SettlementDate': start_date
                }
        bod_request = requests.get(bod_url, params=params)
        try:
            # parse request xml content 
            bod_json_tree = xmltodict.parse(bod_request.content)
            # handle no data for BOD
            bod_object_list = bod_json_tree['response']['responseBody']['responseList']['item']
            bod_curr_df = pd.DataFrame(bod_object_list)
            bod_curr_df['settlement_period'] = int(sp)
            bod_df_list.append(bod_curr_df)
            print('Added {} rows from settlement period {} for {}'.format(len(bod_curr_df), sp, start_date))
        except Exception as e:
            skip_list.append((int(sp), start_date))
            print('Skipped settlement period {} for {}'.format(sp, start_date))
            print(e)
        bod_df = pd.concat(bod_df_list)
    return bod_df

def get_bod_table(bod_df):
    # do all the cleaning and data transformation 
    
    bod_df = bod_df[['bmUnitID', 'bidOfferPairNumber', 'timeFrom','bidOfferLevelFrom', 'timeTo', 'bidOfferLevelTo', 'bidPrice', 'offerPrice', 'settlement_period']]
    
    bod_df['timeFrom'] = pd.to_datetime(bod_df['timeFrom'], format = '%Y-%m-%d %H:%M:%S')
    bod_df['timeTo'] = pd.to_datetime(bod_df['timeTo'], format = '%Y-%m-%d %H:%M:%S')
    bod_df['bidOfferPairNumber'] = bod_df['bidOfferPairNumber'].astype(float).astype(int)
    bod_df['bidOfferLevelTo'] = bod_df['bidOfferLevelTo'].astype(float).astype(int)
    bod_df['bidOfferLevelFrom'] = bod_df['bidOfferLevelFrom'].astype(float).astype(int)
    bod_df['bidPrice'] = bod_df['bidPrice'].astype(float).astype(int)
    bod_df['offerPrice'] = bod_df['offerPrice'].astype(float).astype(int)
    
    
    bod_df = bod_df.rename(columns = {'recordType' : 'record_type',
                                      'bmUnitID' : 'bmunit_id', 
                                      'bidOfferPairNumber': 'bm_offer_pair_number',
                                      'timeFrom' : 'from_time',
                                      'bidOfferLevelFrom' : 'from_level', 
                                      'timeTo' : 'to_time',
                                      'bidOfferLevelTo' : 'to_level',
                                      'bidPrice': 'bid_price', 
                                      'offerPrice': 'offer_price'
                                      })
    #bod_df = bod_df[(bod_df['from_level'] != 0) | (bod_df['to_level'] != 0)]
    # add a timestamp
    bod_df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    return bod_df

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
        logger.info('Could not perform DB write. Not connected to DB...')
        return None
    else:
        st = time.time()
        df.to_sql(table_name, engine, if_exists = 'append', chunksize = 100, index  = False, method='multi')
        et = time.time()
        logger.info('Written {} rows to DB in {:4.2f} seconds.'.format(len(df), et-st))
############################### Testing
        
#s = '2020-10-29'
#e = '2020-10-30'
#bod_df = get_bod_xml(s, e)
#bod = get_bod_table(bod_df)
##############################

#delta = download_end_date - download_start_date  # as timedelta

#if create_new:
#    create_table_sql = '''  
#            CREATE TABLE [dbo].[BMRS_BOD](
#                [id] [int] IDENTITY(1,1) PRIMARY KEY,
#                [bmunit_id] [varchar](max) NOT NULL,
#                [bm_offer_pair_number] [int] NOT NULL,
#                [from_time] [datetime] NOT NULL,
#                [from_level] [float] NOT NULL,
#                [to_time] [datetime] NOT NULL,
#                [to_level] [float] NOT NULL,
#            	[bid_price] [float] NOT NULL,
#                [offer_price] [float] NOT NULL,
#                [settlement_period] [int] NOT NULL,
#                [timestamp] [datetime] NOT NULL
#            )
#            '''
#    creation = engine.connect()
#    table_creation = creation.execute(create_table_sql)

#for i in range(delta.days + 1):
#try:
    #start_date = str(download_start_date + timedelta(days=i))
    #end_date = str(download_start_date + timedelta(days=i+1))
logger.info('Connecting to DB.')
engine = connect_to_db()
logger.info('Connected')
try:    
    bod_df = get_bod_xml()
    bod = get_bod_table(bod_df)
    write_to_db(engine, bod, 'BMRS_BOD')
    
except Exception as e:
    logger.info('Could not perform DB write')
    print('Error: {}'.format(e))

if skip_list:
    logger.info('Handling skipped SPs...')
else:
    logger.info('No SPs were skipped..')
count = 0
while skip_list and count < 10:
    sp, start_date = skip_list.pop(0)
    try:
        bod_df= get_bod_xml(start_date = start_date, settlement_period = sp)
        bod = get_bod_table(bod_df)
        
        write_to_db(engine, bod, 'BMRS_BOD')
        
    except Exception as e:
        count += 1
        skip_list.append((sp, start_date))
        print('Error: {}'.format(e))
        logger.info('Check skip_list! Something is wrong.')
        logger.info(list(set(skip_list)))