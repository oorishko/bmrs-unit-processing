# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 12:24:18 2020

@author: OOrishko
"""
import logging
import logging.handlers

logging_message_format  = '%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s'
logging_date_format = '%Y-%m-%d %H:%M:%S'

log_filename = 'S://Power Trading//Ostap//BMRS//BMRS Loggers//PHYDATA.log'

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

logging_handler = logging.handlers.TimedRotatingFileHandler(filename = log_filename, when = 'W6', interval = 1)
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

def send_error_mail(message_text):        
    from_address = 'OOrishko@HartreePartners.com;'
    emailing_list = 'OOrishko@HartreePartners.com;'
    subject = '"AUTO - BMRS Physical Data Pipeline Failure"'
    message_text = message_text.replace("\"","\'")
    args = 'S:\\Stephen\\MailEngine\\MailEngine.exe --from={from_address} --to={to} --subject={subject} --body={body} --push=false'.format(
            from_address=from_address, to=emailing_list, subject=subject, body='"<html>' + message_text + '</html>"')
    logger.info('Successfully sent email about the error with message: {}'.format(message_text))
    subprocess.call(args, shell=False)

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
#ldl = ['2016-10-30','2017-10-29','2018-10-28']
#spl = [49,50]
#skip_list = [(50, y) for y in ldl]
#skip_list = [(49, '2016-10-30')]
error_list = []

def get_bmu_list():
    bmu_query = '''SELECT [BM Unit ID] FROM BM_UNIT_MASTER_META'''
    bmu_df = sql.get_data(bmu_query, 'sandbox')
    return list(bmu_df['BM Unit ID'])

def get_db_date(min_or_max = 'MAX', metric = 'FPN'):
    if metric == 'BOALF':
        date_query = '''SELECT {}(DateTime) FROM BMRS_{}_New'''.format(min_or_max.upper(), metric)
    else: 
        date_query = '''SELECT {}(to_time) FROM BMRS_{}'''.format(min_or_max.upper(), metric)
    date_df = sql.get_data(date_query, 'sandbox')
    record = date_df.iloc[0][0]
    print(record)
    return record

def get_settlement_date_period_pair(start_date, 
                                    end_date = 'latest', 
                                    hours_ahead = 1):
    # agree on the end date
    # start date is determined by a parent function
    if end_date == 'latest':
        end_date = pd.Timestamp.now() #- pd.Timedelta(hours=hours_ahead)
    elif end_date == 'hours ahead':
        end_date = start_date + pd.Timedelta(hours=hours_ahead)
    else:
        end_date = end_date
    # get range of 30 minute intervals for settlement periods
    end_date = pd.Timestamp(end_date) - pd.Timedelta('30 minutes')
    start_date = pd.Timestamp(start_date) - pd.Timedelta('30 minutes')
    hh_range = pd.date_range(start = start_date, end = end_date, freq = '30T')
    
    # get date timestamp and SP in a tuple
    hh_date = hh_range.date
    respective_hh_sps = (hh_range.hour*2 + hh_range.minute/30)+1
    
    return list(zip(hh_date, respective_hh_sps))

# =============================================================================
# 
# THE PLAN: 
# GET PHYSICAL BM DATA (AVA) BETWEEN TWO DATES, USE IT FROM FPN, MEL, MIL, WRITE TO DB
# GET BOD DATA, CLEAN  AND WRITE TO DB
# 
# USE PHYSICAL BM DATA FOR BOALF
# JOIN IT WITH HISTORIC ACCEPTANCE NUMBERS
# GET ACTUAL HISTORIC CHANGES FROM THESE
# INSTEAD OF BOALF
# 
# create table for BOALF + HISTORIC
# =============================================================================


########################################## HISTORIC ACCEPTANCES ################################################

#def get_historic_acceptances_xml(start_date='2020-10-13',
#                                 end_date = '2020-10-14',
#                                 bm_unit_id=None,
#                                 bm_unit_type=None,
#                                 lead_party_name=None,
#                                 ngc_bm_unit_name=None,
#                                 service_type='xml',
#                                 hours_ahead = 6):
#    '''
#    hist_acceptances gets all BMUs 
#    '''
#    
#    # collect DataFrames from all dates together
#    if start_date == None and end_date == None:
#        logger.info('Querying start and end date times')
#        end_date = 'latest'
#        logger.info('Ending Physical BM data grab at {}'.format(end_date))
#        
#        start_date = min(get_db_date('MAX','FPN'), get_db_date('MAX','MEL'), get_db_date('MAX','MIL'))
#        logger.info('Starting Physical BM data grab at {}'.format(start_date))  
#    
#    date_sp_pairs = get_settlement_date_period_pair(start_date, end_date, hours_ahead = hours_ahead)
#
#    hist_df_list = []
#
#    st = time.time()
#    
#    for d, sp in date_sp_pairs:
#        params = {
#            'APIKey': API_KEY,
#            'ServiceType': service_type,
#            'SettlementDate' : d.strftime(format = '%Y-%m-%d'),
#            'SettlementPeriod': str(int(sp))
#            }
#
#        #print(params)
#        # perform get request
#        hist_request = requests.get(hist_acc_url, params=params)
#        # parse request xml content 
#        hist_json_tree = xmltodict.parse(hist_request.content)
#        hist_resp = hist_json_tree['response']
#        #print(hist_resp['responseMetadata'])
#        try:
#            hist_object_list = hist_resp['responseBody']['responseList']['item']
#            
#            hist_curr_df = pd.DataFrame(hist_object_list)
#            hist_curr_df['settlement_period'] = int(sp)
#            hist_df_list.append(hist_curr_df)
#            logger.info('Added {} rows from settlement period {}'.format(len(hist_curr_df), sp))
#        except:
#            logger.info('Skipped settlement period {}'.format(sp))
#            pass
#
#    et = time.time()
#    logger.info("Finished all Historic Acceptances in {} seconds".format(et - st))
#            
#    hist_df = pd.concat(hist_df_list)
#    
#    return hist_df


############################### PHYSICAL BM DATA (AVA) #########################################################

# FPN, MELs, BOALF etc 
# From PHYSICAL BM data 
def get_availability_metrics_xml(start_date = None,
                                 end_date = None,
                                 service_type='xml', 
                                 settlement_period = None,
                                 hours_ahead = 2):
    global skip_list
    
    if start_date == None and end_date == None:
        logger.info('Querying start and end date times')
        end_date = 'latest'
        logger.info('Ending Physical BM data grab at {}'.format(end_date))
        
        start_date = str(min(get_db_date('MAX','BOALF'), get_db_date('MAX','PN_New'), get_db_date('MAX','MEL_New'),get_db_date('MAX','MIL_New')))
        logger.info('Starting Physical BM data grab at {}'.format(start_date))  
    ava_df_list = []
    if settlement_period == None:
        date_sp_pairs = get_settlement_date_period_pair(start_date, end_date, hours_ahead = hours_ahead)
        
        # collect DataFrames from all dates together
        st = time.time()
        for d, sp in date_sp_pairs:
            curr_date = d.strftime(format = '%Y-%m-%d')
            curr_sp = int(sp)
            params = {
                    'APIKey': API_KEY,
                    'ServiceType': service_type,
                    'SettlementDate' : curr_date,
                    'SettlementPeriod': curr_sp
                    }  
    
            #time.sleep(2)
    
            # perform get request
            ava_request = requests.get(phybmdata_url, params = params) 
            # parse request xml content 
            ava_json_tree = xmltodict.parse(ava_request.content)
            
            try:
                ava_response = ava_json_tree['response']
                ava_object_list = ava_response['responseBody']['responseList']['item']
                
                ava_curr_df = pd.DataFrame(ava_object_list)
                ava_df_list.append(ava_curr_df)
                logger.info('Added {} rows from SP {} for {}'.format(len(ava_curr_df), curr_sp, curr_date))
            except Exception as e:
                skip_list.append((int(sp), d.strftime(format = '%Y-%m-%d')))
                logger.info('Skipped Settlement Period {} for {}'.format(curr_sp, curr_date))
                logger.info('Error: {}'.format(e))
                continue
                    
        et = time.time()
        logger.info("Finished all in {} seconds".format(et - st))

    else:
        print('Handling skipped settlement periods')
        sp = settlement_period
        params = {
                'APIKey': API_KEY,
                'ServiceType': service_type,
                'SettlementDate' : start_date,
                'SettlementPeriod': sp
                }
        # perform get request
        ava_request = requests.get(phybmdata_url, params = params) 
        # parse request xml content 
        ava_json_tree = xmltodict.parse(ava_request.content)
        
        try:
            ava_response = ava_json_tree['response']
            ava_object_list = ava_response['responseBody']['responseList']['item']
            
            ava_curr_df = pd.DataFrame(ava_object_list)
            ava_df_list.append(ava_curr_df)
            print('Added {} rows from SP {} for {}'.format(len(ava_curr_df), sp, start_date))
        except Exception as e:
            print('Skipped Settlement Period {} for {}'.format(sp, start_date))
            print(e)         
            skip_list.append((sp, start_date))
    # get all the days together
    ava_df = pd.concat(ava_df_list)
    # fix date types
    ava_df['timeFrom'] = pd.to_datetime(ava_df['timeFrom'], format = '%Y-%m-%d %H:%M:%S')
    ava_df['timeTo'] = pd.to_datetime(ava_df['timeTo'], format = '%Y-%m-%d %H:%M:%S')

    # fix numeric types to float so NaN is handled
    level_columns = ['pnLevelFrom', 'pnLevelTo', 'melLevelFrom', 'melLevelTo', 'milLevelFrom','milLevelTo', 'bidOfferLevelFrom', 'bidOfferLevelTo']
    
    for l in level_columns:
        ava_df[l] = ava_df[l].astype(float)
    
    return ava_df

#def get_tables(ava_df, metric = 'PN'):
#    df = ava_df[ava_df['recordType'] == metric] # 'bidOfferLevelFrom', 'bidOfferLevelTo',
#    df = df[['recordType','bmUnitID', 'settlementPeriod', 'timeFrom', metric.lower() +'LevelFrom', 'timeTo', metric.lower() +'LevelTo']]
#
#    df = df.rename(columns = {'recordType' : 'record_type',
#                                      'bmUnitID' : 'bmunit_id', 
#                                      'settlementPeriod': 'settlement_period',
#                                      'timeFrom' : 'from_time',
#                                      metric.lower() +'LevelFrom' : 'from_level', 
#                                      'timeTo' : 'to_time',
#                                      metric.lower() +'LevelTo' : 'to_level'
#                                      })
#    #df = df[(df['from_level'] != 0) | (df['to_level'] != 0)]
#    df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M")    
#    return df


def get_boalf_table(ava_df):
    boalf_df = ava_df[ava_df['recordType'] == 'BOALF']
    boalf_df = boalf_df[['bmUnitID', 'settlementPeriod','bidOfferAcceptanceNumber', 'acceptanceTime', 
                         'timeFrom','bidOfferLevelFrom', 'timeTo', 'bidOfferLevelTo', 'activeFlag',
                         'deemedBidOfferFlag', 'soFlag', 'storProviderFlag', 'rrInstructionFlag','rrScheduleFlag']]
    boalf_df['timeFrom'] = pd.to_datetime(boalf_df['timeFrom'], format = '%Y-%m-%d %H:%M:%S')
    boalf_df['timeTo'] = pd.to_datetime(boalf_df['timeTo'], format = '%Y-%m-%d %H:%M:%S')
    boalf_df['acceptanceTime'] = pd.to_datetime(boalf_df['acceptanceTime'], format = '%Y-%m-%d %H:%M:%S')
    boalf_df = boalf_df.rename(columns = {
                                      'bmUnitID' : 'bmunit_id', 
                                      'settlementPeriod': 'settlement_period',
                                      'bidOfferAcceptanceNumber' : 'acceptance_number',
                                      'acceptanceTime' : 'acceptance_time',
                                      'timeFrom' : 'from_time',
                                      'bidOfferLevelFrom' : 'from_level', 
                                      'timeTo' : 'to_time',
                                      'bidOfferLevelTo' : 'to_level',
                                      'activeFlag': 'active_flag',
                                      'deemedBidOfferFlag': 'deemed_bid_offer_flag', 
                                      'soFlag': 'so_flag', 
                                      'storProviderFlag':'stor_provider_flag', 
                                      'rrInstructionFlag':'rr_instruction_flag',
                                      'rrScheduleFlag': 'rr_schedule_flag'
                                      })
    boalf_df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return boalf_df

############################################# DB HANDLER ###################################################


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
        df.to_sql(table_name, engine, if_exists = 'append', chunksize = 100, index  = False, method='multi')
        et = time.time()
        logger.info('Written {} rows to into {} in {:4.2f} seconds.'.format(len(df), table_name, et-st))


############################### Joining BOALF and Historic Acceptances #######################################

#def get_actual_boalf(bh):
#
#    bh_df = bh.copy()
#    bmus = list(bh.bmunit_id.unique())
#    print(len(bmus))
#    # get acceptance num as int for sorting
#    bh_df['acceptance_number'] = bh_df['acceptance_number'].astype(int)
#    bh_df = bh_df.sort_values(by = ['acceptance_number', 'from_time'], ascending = False).reset_index(drop = True)
#    bh_df.reset_index(drop = True, inplace = True)
#    all_dfs_list = []
#    
#    for bmu in bmus:
#        # get all acceptance ids for one bmu
#        curr_df = bh_df[bh_df['bmunit_id'] == bmu]
#        # sort in descending time order
#        curr_df = curr_df.sort_values(by = ['acceptance_number', 'from_time'], ascending = False).reset_index(drop = True)
#        curr_df['comp_flag'] = False
#        # start at the top
#        idx = 0
#        # last available index is greater or equal to the next index we look at
#        while len(curr_df) - 1 >= idx + 1 :
#            # if from time of the latest acceptance number occurred BEFORE to time of the previous acceptance number 
#            # then the latest acceptance number is a correction, hence we remove an entry with the previous acceptance number 
#            if curr_df.iloc[idx]['from_time'] < curr_df.iloc[idx + 1]['to_time'] and curr_df.iloc[idx]['from_time'] < curr_df.iloc[idx + 1]['from_time']:
#        
#                # reset index to make sure that next entry is idx + 1
#                curr_df = curr_df.drop([idx + 1])
#                curr_df.reset_index(drop = True, inplace = True)
#            elif curr_df.iloc[idx]['from_time'] < curr_df.iloc[idx + 1]['to_time'] and curr_df.iloc[idx]['from_time'] >= curr_df.iloc[idx + 1]['from_time']:
#                curr_df.loc[idx+1, 'to_level'] = curr_df.loc[idx, 'from_level']
#                curr_df.loc[idx+1, 'to_time'] = curr_df.loc[idx, 'from_time']
#                idx += 1
#            else: 
#                idx += 1
#        # write cleaned version into a new df 
#        # get all final to times for each of acceptance numbers so that all values are in one timeline
#        to_times = curr_df.loc[curr_df.groupby(['bmunit_id', 'acceptance_number']).to_time.idxmax()][['bmunit_id', 'acceptance_number', 'acceptance_time', 'to_time', 'to_level', 'timestamp']]
#        # rename for concatenation
#        to_times = to_times.rename(columns = {'to_level' : 'from_level', 'to_time':'from_time'})
#        
#        curr_df = pd.concat([curr_df, to_times])
#        curr_df = curr_df.sort_values(by= ['bmunit_id' , 'acceptance_number', 'from_time'])  
#        curr_df.drop_duplicates(inplace = True)
#        curr_df = curr_df.set_index('from_time').tz_localize('utc').tz_convert('Europe/London')
#        
#        curr_df.reset_index(inplace = True)
#        all_dfs_list.append(curr_df)
#        
#    cleaned_actuals = pd.concat(all_dfs_list)
#    cleaned_actuals = cleaned_actuals.reset_index(drop = True)
#    
#    bmus_list = list(cleaned_actuals.bmunit_id.unique())
#    all_acceptances = []
#    
#    for bmu in bmus_list:
#        bmu_df = cleaned_actuals[cleaned_actuals['bmunit_id'] == bmu]
#        acceptances_list  = list(bmu_df.acceptance_number.unique())
#        for acc in acceptances_list:
#            acc_df = bmu_df[bmu_df['acceptance_number'] == acc]
#            acc_df['time'] = acc_df['from_time']
#            acc_df.set_index('time', inplace = True)
#     
#            acc_minutes = pd.DataFrame(acc_df.resample('1T').sum().index).set_index('time')
#    
#            acc_join = acc_minutes.merge(acc_df, right_index = True, left_index = True, how = 'outer')
#    
#            acc_join = acc_join[(acc_join.index == acc_join.from_time) | (acc_join.index.minute == 0) | (acc_join.index.minute == 30)]
#    
#            acc_join = acc_join.rename_axis('DateTime')
#            acc_join['from_level'] = acc_join['from_level'].interpolate()
#            acc_join['acceptance_number'] = acc_join['acceptance_number'].ffill()
#            acc_join['bmunit_id'] = acc_join['bmunit_id'].ffill()
#            acc_join['acceptance_time'] = acc_join['acceptance_time'].ffill()
#            acc_join['timestamp'] = acc_join['timestamp'].ffill()
#            acc_join['settlement_period'] = 1 + (acc_join.index.hour * 2) + (acc_join.index.minute // 30)
#            acc_join = acc_join.tz_convert('utc')
#            acc_join.reset_index(inplace = True)
#    
#            all_acceptances.append(acc_join)
#        
#    final = pd.concat(all_acceptances)
#    final = final[['DateTime', 'bmunit_id', 'acceptance_number', 'acceptance_time', 'from_level', 'timestamp', 'settlement_period']]
#    final = final.rename(columns = {'from_level' : 'level'})
#
#    final.reset_index(inplace = True, drop = True)
#    logger.info('Cleaned BOALF into a sequence')
#    return final
############################################## DO THE THING ##################################################

download_start_date = date(2018,12,31)

download_end_date = date(2019,12,31)
delta = download_end_date - download_start_date  # as timedelta
logger.info('Connecting to DB..')
engine = connect_to_db()
logger.info('Connected to DB..')
# Query the API
for i in range(delta.days):
    start_date = str(download_start_date + timedelta(days=i))
    end_date = str(download_start_date + timedelta(days=i+1))
    try:
        ava_df = get_availability_metrics_xml(start_date = start_date, end_date = end_date)
        #ava_df = get_availability_metrics_xml()
        logger.info('REST API request was successful')
        # try for boalf
        try:
            boalf = get_boalf_table(ava_df)
            #bh = get_actual_boalf(boalf)
            boalf = boalf.drop_duplicates(subset = ['from_time', 'bmunit_id', 'acceptance_number', 'settlement_period'])
            write_to_db(engine, boalf, 'BMRS_BOALF_New')
        except Exception as e:
            logger.info('Failed to write to DB!')
            logger.info('Error: {}'.format(e))
            sd = str(get_db_date('MAX','BOALF_New'))
            send_error_mail('Failed to write into BMRS_BOALF_New!. Error body: {}. Last date in DB is {}. Time now is {}.'.format(e, sd, time.time()))
        # try for the rest 
    #    for metric in ['PN','MEL','MIL']:
    #        df = get_tables(ava_df, metric = metric)
    #        df = df.drop_duplicates()
    #        m = metric
    #        try:
    #            write_to_db(engine, df, 'BMRS_'+m+'_New')
    #        except Exception as e:
    #            logger.info('Failed to write into BMRS_{}!'.format(m))
    #            logger.info('Error: {}'.format(e))
    #            sd = str(min(get_db_date('MAX','PN_New'), get_db_date('MAX','MEL_New'),get_db_date('MAX','MIL_New')))
    #            send_error_mail('Failed to write into BMRS_{}!. Error body: {}. Last date in DB is {}. Time now is {}.'.format(m, e, sd, time.time()))
    except Exception as e:
        logger.info('REST API request was unsuccessful:(')
        logger.info('Error: {}'.format(e))
        error_list.append(start_date)
#        sd = str(min(get_db_date('MAX','BOALF'), get_db_date('MAX','PN_New'), get_db_date('MAX','MEL_New'),get_db_date('MAX','MIL_New')))
#        send_error_mail('REST API request was unsuccessful. Error body: {}. Last date in DB is {}. Time now is {}.'.format(e, sd, time.time()))

error_list = list(set(error_list))
print(error_list)
count = 0
while error_list and count < 20:
    start_date = error_list.pop(0)
    end_date = str(datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=1))
    try:
        ava_df = get_availability_metrics_xml(start_date = start_date, end_date = end_date)
        #boalf
        boalf = get_boalf_table(ava_df)
        #bh = get_actual_boalf(boalf)
        boalf = boalf.drop_duplicates(subset = ['from_time', 'bmunit_id', 'acceptance_number', 'settlement_period'])
        write_to_db(engine, boalf, 'BMRS_BOALF_New')
        # the rest
#        for metric in ['PN','MEL','MIL']:
#            df = get_tables(ava_df, metric = metric)
#            df = df.drop_duplicates()
#            m = metric
#            write_to_db(engine, df, 'BMRS_'+m+'_New')  
    except Exception as e:
#        error_list.append(start_date)
        count += 1
        #print('Failed to write into BMRS_{}!'.format(m))
        print('Error: {}'.format(e))
        
if error_list:
    logger.info('The following were still skipped and not written into DB:')
    
count = 0
while skip_list and count < 10:
    sp, start_date = skip_list.pop(0)
    try:
        ava_df = get_availability_metrics_xml(start_date = start_date, settlement_period = sp)
        #boalf
        boalf = get_boalf_table(ava_df)
        #bh = get_actual_boalf(boalf)
        boalf = boalf.drop_duplicates(subset = ['from_time', 'bmunit_id', 'acceptance_number', 'settlement_period'])
        write_to_db(engine, boalf, 'BMRS_BOALF_New')
        # the rest
#        for metric in ['PN','MEL','MIL']:
#            df = get_tables(ava_df, metric = metric)
#            df = df.drop_duplicates()
#            m = metric
#            write_to_db(engine, df, 'BMRS_'+m+'_New')        
    except Exception as e:
        skip_list.append((sp,start_date))
        count += 1
        #logger.info('Failed to write to DB!')
        #print('Error: {}'.format(e))
        
if skip_list:
    logger.info('The following were still skipped and not written into DB:')
    logger.info(list(set(skip_list)))
    send_error_mail('The following were still skipped and not written into DB {}. Last date in DB is {}. Time now is {}.'.format(list(set(skip_list))[0], sd, time.time()))
else:
    logger.info('Skip list is empty')

try:
    st = time.time()
    remove_duplicates = '''
                        Delete from [sandbox].[dbo].[BMRS_BOALF_New] WHERE [id] in 
                        (
                        SELECT max([id])
                        FROM  [sandbox].[dbo].[BMRS_BOALF_New]
                        GROUP BY [bmunit_id], [acceptance_number], [from_time], [settlement_period]
                        HAVING Count(*) >1
                        )
                        '''
    connection = engine.connect()
    duplicates = connection.execute(remove_duplicates)
    et = time.time()
    logger.info('Removed ' + str(duplicates.rowcount) + ' duplicates from BMRS_BOALF_New in {:4.2f} seconds.'.format(et-st))
except:
    logger.info('Could not remove duplicates from BOALF_New.')
    send_error_mail('Could not remove duplicates from BMRS_BOALF_New. Time now is {}'.format(time.time()))
#for metric in ['PN', 'MEL', 'MIL']:
#    try:
#        st = time.time()
#        remove_duplicates = '''
#                            Delete from [sandbox].[dbo].[BMRS_{}_New] WHERE [id] in 
#                            (
#                            SELECT max([id])
#                            FROM  [sandbox].[dbo].[BMRS_{}_New]
#                            GROUP BY [bmunit_id], [from_time],[from_level], [to_time], [to_level], [settlement_period]
#                            HAVING Count(*) >1
#                            )
#                            '''.format(metric, metric)
#        connection = engine.connect()
#        duplicates = connection.execute(remove_duplicates)
#        et = time.time()
#        logger.info('Removed ' + str(duplicates.rowcount) + ' duplicates from BMRS_{}_New in {:4.2f} seconds.'.format(metric, et-st))
#    except:
#        logger.info('Could not remove duplicates from BMRS_{}_New.'.format(metric))
#        send_error_mail('Could not remove duplicates from BMRS_{}_New. Time now is {}'.format(metric, time.time()))
logger.info('Done')