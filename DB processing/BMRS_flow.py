# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 12:26:26 2020

@author: oorishko
"""

from sumo.io import sql
from sumo.data import gas_data as gd
import pandas as pd
import numpy as np
from sklearn import metrics
import time
import random
import datetime
from tqdm import tqdm
from dateutil import relativedelta
import math

#start_date = '2017-01-01'
#end_date = '2021-03-16'
#units = ['T_KEAD-1','T_PEHE-1','T_MRWD-1','T_MEDP-1', 'T_LAGA-1']

def bmrs_flow(units, start_date, end_date):

    units_str = "'" + "','".join(units)+ "'"

    #end_date = str(pd.Timestamp.now().date() - pd.Timedelta('1 day'))
    print(units_str)
    '''
    Multiple BM Levels Examples:
    https://enbm.netareports.com/#bmofferprofile?country=uk&start=202012160108&end=202012180119&filter=fuel&fuel=ccgt&bmunitid=T_MRWD-1
    https://enbm.netareports.com/#bmofferprofile?country=uk&start=202012070009&end=202012072200&filter=fuel&fuel=ccgt&bmunitid=T_RYHPS-1
    
    https://enbm.netareports.com/#bmofferprofile?country=uk&start=202011131319&end=202011160858&filter=none&none=null&bmunitid=T_HUMR-1
    '''
    def get_fpn_profile():
        '''
        gets cleaned sequence of FPN
        outputs both minutely and HH time series for a sequence of BMUs
        '''
        combine_fpn_min = []
        combine_fpn_hh = []
        if end_date:
            sql_query = '''SELECT bmunit_id, from_time, to_time, from_level, to_level, settlement_period 
                        FROM BMRS_PN_New
                        WHere bmunit_id IN ({})
                    	and from_time > '{} 00:00:00.000'
                    	and from_time <= '{} 00:00:00.000'
                        '''.format(units_str,start_date, end_date)
        else:
            sql_query = '''SELECT bmunit_id, from_time, to_time, from_level, to_level, settlement_period 
                        FROM BMRS_PN_New
                        WHere bmunit_id IN ({})
                    	and from_time > '{} 00:00:00.000'
                        '''.format(units_str,start_date)            
                        
        fpn_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        for bmu in list(fpn_data.bmunit_id.unique()):
            fpn = fpn_data[fpn_data.bmunit_id == bmu].copy()
            fpn['from_time'] = pd.to_datetime(fpn['from_time'], format = '%Y-%m-%d %H:%M:%S')
            fpn['to_time'] = pd.to_datetime(fpn['to_time'], format = '%Y-%m-%d %H:%M:%S')
            
            fpn = fpn.sort_values(by = 'from_time', ascending = True).reset_index(drop = True)
            
            # resample to hh - just like enapsys!
            fpn['time_delta'] = fpn['to_time'] - fpn['from_time']
            fpn['time_delta'] = fpn['time_delta'].dt.total_seconds() // 60
            fpn['weighting'] = ((fpn['from_level'] + fpn['to_level']) /2) * fpn['time_delta']
            fpn['HH'] = fpn['from_time'].dt.floor('30T')
            fpn_hh = fpn.groupby(by = ['HH', 'settlement_period']).sum().reset_index()
            fpn_hh['FPN Unadjusted'] = fpn_hh['weighting'] / 30
            fpn_hh = fpn_hh[['HH','FPN Unadjusted']]
            fpn_hh = fpn_hh.set_index('HH').rename_axis('DateTime')
            fpn_hh['bmunit_id'] = bmu
            
            # resampling minutely
            fpn_min_df = []
            for i, row in fpn.iterrows():
                temp_df = pd.DataFrame({
                        'DateTime': [row['from_time'], row['to_time']],
                        'FPN Unadjusted': [row['from_level'], row['to_level']],
                        'settlement_period':[row['settlement_period'], row['settlement_period']]
                        })
                temp_df.set_index('DateTime', inplace = True)
                temp_df_min = pd.DataFrame([])
                temp_df_min['FPN Unadjusted'] = temp_df['FPN Unadjusted'].resample('1T').interpolate()
                temp_df_min['settlement_period'] = temp_df['settlement_period'].resample('1T').ffill()
                fpn_min_df.append(temp_df_min)
            fpn_min = pd.concat(fpn_min_df).sort_index(ascending = True)
            fpn_min = fpn_min[~fpn_min.index.duplicated(keep = 'last')]
            fpn_min['bmunit_id'] = bmu

            # oldie  
#            fpn.set_index('from_time', inplace = True)
#            fpn['date'] = fpn.index.date
#            to_times = fpn.loc[fpn.groupby(['date', 'settlement_period']).to_time.idxmax()][['to_level', 'to_time', 'settlement_period']]
#            to_times.reset_index(drop = True, inplace = True)
#            to_times = to_times.rename(columns = {'to_level' : 'from_level', 'to_time':'from_time'})
#            to_times.set_index('from_time', inplace = True)
#            to_times['order'] = 2
#            fpn = fpn[['from_level']]
#            fpn['order'] = 1
#            fpn = pd.concat([fpn, to_times[['from_level', 'order']]])
#            fpn.reset_index(inplace = True)
#            fpn.drop_duplicates(subset = ['from_time', 'from_level'], inplace = True)
#            fpn = fpn.sort_values(by = ['from_time', 'order'], ascending = True)
#            fpn.set_index('from_time', inplace = True)
#            
#            fpn = fpn[~fpn.index.duplicated(keep = 'first')][['from_level']]
#            
#            fpn_min = fpn.resample('1T').interpolate()
#            fpn_min = fpn_min.rename(columns = {'from_level':'FPN Unadjusted'}).rename_axis('DateTime')
#            
#            fpn_min['bmunit_id'] = bmu
            combine_fpn_min.append(fpn_min)
            combine_fpn_hh.append(fpn_hh)
        
        fpn_min = pd.concat(combine_fpn_min)
        fpn_hh = pd.concat(combine_fpn_hh)
        fpn_min = fpn_min.reset_index()
        fpn_hh = fpn_hh.reset_index()
        
        combine_mel_min = []
        combine_mel_hh = []
        
        if end_date:
            mel_sql_query = '''SELECT bmunit_id, from_time, to_time, from_level, to_level, settlement_period 
                        FROM BMRS_MEL_New
                        WHere bmunit_id IN ({})
                    	and from_time > '{} 00:00:00.000'
                    	and from_time <= '{} 00:00:00.000'                   
                        '''.format(units_str,start_date, end_date)
        else:
            mel_sql_query = '''SELECT bmunit_id, from_time, to_time, from_level, to_level, settlement_period 
                        FROM BMRS_MEL_New
                        WHere bmunit_id IN ({})
                    	and from_time > '{} 00:00:00.000'                
                        '''.format(units_str,start_date)   
                        
        mel_data = sql.get_data(mel_sql_query, sandbox_or_production = 'sandbox')
        for bmu in list(mel_data.bmunit_id.unique()):
            mel = mel_data[mel_data.bmunit_id == bmu].copy()
            mel['from_time'] = pd.to_datetime(mel['from_time'], format = '%Y-%m-%d %H:%M:%S')
            mel['to_time'] = pd.to_datetime(mel['to_time'], format = '%Y-%m-%d %H:%M:%S')
            
            mel = mel.sort_values(by = 'from_time', ascending = True).reset_index(drop = True)
            
            # resample to hh - just like enapsys!
            mel['time_delta'] = mel['to_time'] - mel['from_time']
            mel['time_delta'] = mel['time_delta'].dt.total_seconds() // 60
            mel['weighting'] = ((mel['from_level'] + mel['to_level']) /2) * mel['time_delta']
            mel['HH'] = mel['from_time'].dt.floor('30T')
            mel_hh = mel.groupby(by = ['HH', 'settlement_period']).sum().reset_index()
            mel_hh['MEL'] = mel_hh['weighting'] / 30
            mel_hh = mel_hh[['HH','MEL']]
            mel_hh = mel_hh.set_index('HH').rename_axis('DateTime')
            mel_hh['bmunit_id'] = bmu
            
            # resampling minutely
            mel_min_df = []
            for i, row in mel.iterrows():
                temp_df = pd.DataFrame({
                        'DateTime': [row['from_time'], row['to_time']],
                        'MEL': [row['from_level'], row['to_level']]
                        
                        })
                temp_df.set_index('DateTime', inplace = True)
                temp_df_min = pd.DataFrame([])
                temp_df_min['MEL'] = temp_df['MEL'].resample('1T').interpolate()
                mel_min_df.append(temp_df_min)
            mel_min = pd.concat(mel_min_df).sort_index(ascending = True)
            mel_min = mel_min[~mel_min.index.duplicated(keep = 'last')]
            mel_min['bmunit_id'] = bmu
            
            # oldie
#            mel.set_index('from_time', inplace = True)
#            mel['date'] = mel.index.date
#            to_times = mel.loc[mel.groupby(['date', 'settlement_period']).to_time.idxmax()][['to_level', 'to_time', 'settlement_period']]
#            to_times.reset_index(drop = True, inplace = True)
#            to_times = to_times.rename(columns = {'to_level' : 'from_level', 'to_time':'from_time'})
#            to_times.set_index('from_time', inplace = True)
#            to_times['order'] = 2
#            mel = mel[['from_level']]
#            mel['order'] = 1
#            mel = pd.concat([mel, to_times[['from_level', 'order']]])
#            mel.reset_index(inplace = True)
#            mel.drop_duplicates(subset = ['from_time', 'from_level'], inplace = True)
#            mel = mel.sort_values(by = ['from_time', 'order'], ascending = True)
#            mel.set_index('from_time', inplace = True)
#            
#            mel = mel[~mel.index.duplicated(keep = 'first')][['from_level']]
#            
#            mel_min = mel.resample('1T').interpolate()
#            mel_min = mel_min.rename(columns = {'from_level':'MEL'}).rename_axis('DateTime')
#            
#            mel_min['bmunit_id'] = bmu
                        
            combine_mel_min.append(mel_min)
            combine_mel_hh.append(mel_hh)
            
        mel_min = pd.concat(combine_mel_min)
        mel_hh = pd.concat(combine_mel_hh)        
        mel_min = mel_min.reset_index()
        mel_hh = mel_hh.reset_index()  
        
        fpn_hh_adjusted = fpn_hh.merge(mel_hh, on = ['bmunit_id', 'DateTime'], how = 'left')
        
        fpn_hh_adjusted.loc[fpn_hh_adjusted['FPN Unadjusted'] > fpn_hh_adjusted['MEL'],'FPN'] = fpn_hh_adjusted['MEL']
        fpn_hh_adjusted.loc[fpn_hh_adjusted['FPN Unadjusted'] <= fpn_hh_adjusted['MEL'],'FPN'] = fpn_hh_adjusted['FPN Unadjusted']
        fpn_hh_adjusted.loc[fpn_hh_adjusted['FPN'].isna(), 'FPN'] = fpn_hh_adjusted['FPN Unadjusted']
        
        fpn_min_adjusted = fpn_min.merge(mel_min, on = ['bmunit_id', 'DateTime'], how = 'left')
        fpn_min_adjusted.loc[fpn_min_adjusted['FPN Unadjusted'] > fpn_min_adjusted['MEL'],'FPN'] = fpn_min_adjusted['MEL']
        fpn_min_adjusted.loc[fpn_min_adjusted['FPN Unadjusted'] <= fpn_min_adjusted['MEL'],'FPN'] = fpn_min_adjusted['FPN Unadjusted']
        fpn_min_adjusted.loc[fpn_min_adjusted['FPN'].isna(), 'FPN'] = fpn_min_adjusted['FPN Unadjusted']        
        
        return fpn_min_adjusted, fpn_hh_adjusted

    def get_boalf_sequence():
        if end_date:
            sql_query = '''
                        SELECT * 
                        FROM BMRS_BOALF_New
                        WHere bmunit_id IN ({})
                    	and to_time >= '{} 00:00:00.000'
                    	and from_time <= '{} 00:00:00.000'                
                        '''.format(units_str,start_date, end_date)
        else:
            sql_query = '''
                SELECT * 
                FROM BMRS_BOALF_New
                WHere bmunit_id IN ({})
            	and from_time >= '{} 00:00:00.000'            
                '''.format(units_str,start_date)
        boalf_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        boalf_data = boalf_data.drop_duplicates(subset = ['bmunit_id', 'acceptance_number', 'acceptance_time','from_level', 'to_level','from_time', 'to_time'])
        
        boalf_list = []
        for bmu in boalf_data.bmunit_id.unique():
            boalf = boalf_data[boalf_data.bmunit_id == bmu].copy()        
            
            
            month_diff = math.ceil((pd.Timestamp(end_date) - pd.Timestamp(start_date))/np.timedelta64(1, 'M'))
            
            dt_splits = [str((pd.Timestamp(start_date) + pd.Timedelta(weeks=4.5*6*i)).date()) if pd.Timestamp(start_date) + pd.Timedelta(weeks=4.5*6*i) <= pd.Timestamp(end_date) else end_date for i in range(0, month_diff//6 + 1)]
            dt_splits = [dt_splits[0],end_date] if len(dt_splits) == 1 else dt_splits
            
            boalf_split_collect = []
            for j in range(len(dt_splits) - 1):
                print(j)
                boalf_split = boalf_data[(boalf_data.from_time >= dt_splits[j]) & (boalf_data.from_time < dt_splits[j+1])].copy()
                boalf_minutely = []
                for i, row in boalf_split.iterrows():
                    temp_df = pd.DataFrame({
                            'acceptance_number':[row['acceptance_number'], row['acceptance_number']],
                            'DateTime': [row['from_time'], row['to_time']],
                            'BOA Value': [row['from_level'], row['to_level']]
                            })
                    temp_df.set_index('DateTime', inplace = True)
                    temp_df_min = pd.DataFrame([])
                    temp_df_min['BOA Value'] = temp_df['BOA Value'].resample('1T').interpolate()
                    temp_df_min['acceptance_number'] = temp_df['acceptance_number'].resample('1T').ffill()
                    temp_df_min.reset_index(inplace = True)
                    temp_df.sort_values(by = 'acceptance_number', ascending = True, inplace = True)
                    boalf_minutely.append(temp_df_min)
                boalf_min = pd.concat(boalf_minutely).drop_duplicates(subset=['DateTime','acceptance_number']).set_index(['DateTime','acceptance_number'])
                
                boalf_stack = boalf_min.unstack()
                boalf_stack.columns = boalf_stack.columns.map(lambda x: x[1])
                boalf_stack = boalf_stack.T
                acc_num_seq = boalf_stack.notna()[::-1].idxmax()
                acc_num_df = acc_num_seq.to_frame(name = 'acceptance_number')
                acc_num_df.reset_index(inplace = True)
                idx_list = list(acc_num_df.set_index(['DateTime', 'acceptance_number']).index)
                boalf_sequence = boalf_min[boalf_min.index.isin(idx_list)].reset_index().sort_values('DateTime', ascending = True)
                boalf_sequence['bmunit_id'] = bmu
                boalf_split_collect.append(boalf_sequence)
            boalf_split_collect = pd.concat(boalf_split_collect)
            boalf_list.append(boalf_split_collect)
        if len(boalf_list) > 0:
            boalf = pd.concat(boalf_list)
        else:
            boalf = pd.DataFrame([])
        return boalf
    
    def get_bod_profile():
        if end_date:
            sql_query = '''
                        SELECT * 
                        FROM BMRS_BOD_New
                        WHere bmunit_id IN ({})
                    	and from_time >= '{} 00:00:00.000'
                    	and from_time <= '{} 00:00:00.000' 
                        '''.format(units_str, start_date, end_date)
        else:
            sql_query = '''
                        SELECT * 
                        FROM BMRS_BOD_New
                        WHere bmunit_id IN ({})
                    	and from_time >= '{} 00:00:00.000'
                        '''.format(units_str, start_date)
                        
        bod_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        
        bod_list = []
        for bmu in list(bod_data.bmunit_id.unique()):
            bod = bod_data[bod_data.bmunit_id == bmu].copy()
            bod['from_time'] = pd.to_datetime(bod['from_time'], format = '%Y-%m-%d %H:%M:%S')
            bod['to_time'] = pd.to_datetime(bod['to_time'], format = '%Y-%m-%d %H:%M:%S')
            bod = bod.sort_values(by = ['from_time', 'bm_offer_pair_number'], ascending = True)
            
            bod['Date'] = bod.from_time.dt.date
            bod['Abs BOPN'] = abs(bod['bm_offer_pair_number'])
            bod.loc[bod['bm_offer_pair_number'] > 0,'BOPN Sign'] = 'pos'
            bod.loc[bod['bm_offer_pair_number']< 0,'BOPN Sign'] = 'neg'
            bod_clean = bod.groupby(by = ['Date', 'settlement_period', 'BOPN Sign']).agg({
                    'from_time': 'first',
                    'to_time': 'first',
                    'bmunit_id': 'first',
                    'bm_offer_pair_number': list, 
                    'from_level': list,
                    'to_level': list,
                    'bid_price': list,
                    'offer_price' : list})
            bod_clean.reset_index(inplace = True)
            bod_clean['bm_offer_pair_merged'] = bod_clean.apply(lambda row: {row['BOPN Sign']:row['bm_offer_pair_number']}, axis=1)
            bod_clean['from_level_merged'] = bod_clean.apply(lambda row: {row['BOPN Sign']:row['from_level']}, axis=1)
            bod_clean = bod_clean.groupby(by = ['Date', 'settlement_period']).agg({
                    'from_time': 'first',
                    'to_time': 'first',
                    'bmunit_id': 'first',
                    'bm_offer_pair_merged': list, 
                    'from_level_merged': list,
                    'to_level': list,
                    'bid_price': 'first',
                    'offer_price' : 'last'})
            bod_clean.reset_index(inplace = True)        
            bod_clean['from_level_merged'] = bod_clean.apply(lambda x: {**x['from_level_merged'][0], **x['from_level_merged'][1]} if len(x['from_level_merged']) == 2 else x['from_level_merged'][0], axis = 1 )
            bod_clean.set_index('from_time',inplace = True)

            bod_clean['bid_levels'] = bod_clean.apply(lambda x: x['from_level_merged']['neg'] if 'neg' in x['from_level_merged'].keys() else [], axis = 1)
            bod_clean['offer_levels'] = bod_clean.apply(lambda x: x['from_level_merged']['pos'] if 'pos' in x['from_level_merged'].keys() else [], axis = 1)
            
            bod_clean['bid_price-5'] = bod_clean.apply(lambda x: x['bid_price'][-5] if len(x['bid_price']) > 4 else np.nan, axis = 1)
            bod_clean['bid_price-4'] = bod_clean.apply(lambda x: x['bid_price'][-4] if len(x['bid_price']) > 3 else np.nan, axis = 1)
            bod_clean['bid_price-3'] = bod_clean.apply(lambda x: x['bid_price'][-3] if len(x['bid_price']) > 2 else np.nan, axis = 1)
            bod_clean['bid_price-2'] = bod_clean.apply(lambda x: x['bid_price'][-2] if len(x['bid_price']) > 1 else np.nan, axis = 1)
            bod_clean['bid_price-1'] = bod_clean.apply(lambda x: x['bid_price'][-1], axis = 1)

            bod_clean['offer_price-1'] = bod_clean.apply(lambda x: x['offer_price'][0], axis = 1)
            bod_clean['offer_price-2'] = bod_clean.apply(lambda x: x['offer_price'][1] if len(x['offer_price']) > 1 else np.nan, axis = 1)
            bod_clean['offer_price-3'] = bod_clean.apply(lambda x: x['offer_price'][2] if len(x['offer_price']) > 2 else np.nan, axis = 1)
            bod_clean['offer_price-4'] = bod_clean.apply(lambda x: x['offer_price'][3] if len(x['offer_price']) > 3 else np.nan, axis = 1)
            bod_clean['offer_price-5'] = bod_clean.apply(lambda x: x['offer_price'][4] if len(x['offer_price']) > 4 else np.nan, axis = 1)

            bod_clean['bl-5'] = bod_clean.apply(lambda x: x['bid_levels'][-5] if len(x['bid_levels']) > 4 else np.nan, axis = 1)
            bod_clean['bl-4'] = bod_clean.apply(lambda x: x['bid_levels'][-4] if len(x['bid_levels']) > 3 else np.nan, axis = 1)
            bod_clean['bl-3'] = bod_clean.apply(lambda x: x['bid_levels'][-3] if len(x['bid_levels']) > 2 else np.nan, axis = 1)
            bod_clean['bl-2'] = bod_clean.apply(lambda x: x['bid_levels'][-2] if len(x['bid_levels']) > 1 else np.nan, axis = 1)
            bod_clean['bl-1'] = bod_clean.apply(lambda x: x['bid_levels'][-1] if len(x['bid_levels']) > 0 else np.nan, axis = 1)
   
            bod_clean['ol-1'] = bod_clean.apply(lambda x: x['offer_levels'][0] if len(x['offer_levels']) > 0 else np.nan, axis = 1)
            bod_clean['ol-2'] = bod_clean.apply(lambda x: x['offer_levels'][1] if len(x['offer_levels']) > 1 else np.nan, axis = 1)
            bod_clean['ol-3'] = bod_clean.apply(lambda x: x['offer_levels'][2] if len(x['offer_levels']) > 2 else np.nan, axis = 1)
            bod_clean['ol-4'] = bod_clean.apply(lambda x: x['offer_levels'][3] if len(x['offer_levels']) > 3 else np.nan, axis = 1)
            bod_clean['ol-5'] = bod_clean.apply(lambda x: x['offer_levels'][4] if len(x['offer_levels']) > 4 else np.nan, axis = 1)
                
            bod_clean.drop(['bm_offer_pair_merged', 'from_level_merged', 'to_level', 'bid_price', 'offer_price'], axis = 1, inplace = True)
            bod_list.append(bod_clean)
        if len(bod_list) > 0:
            bod_final = pd.concat(bod_list)
        else:
            bod_final = pd.DataFrame([])
        return bod_final
    
    def get_bm_and_co_volumes():
        # get fpn
        fpn_min, fpn_hh = get_fpn_profile()
        # get boalf
        boalf = get_boalf_sequence()
        bod = get_bod_profile()        
        # TODO: handle edge cases better: no FPN/no BOA/ no BOD ???
        if (len(boalf) == 0) and len(bod) != 0:
            bod =bod.reset_index()
            df = fpn_hh.merge(bod, how = 'left', left_on = ['bmunit_id', 'DateTime'], right_on = ['bmunit_id', 'from_time'])
            # TODO: fix the number of columns to be grabbed depending on the fuel type
            
#            df = df[['DateTime','bmunit_id','FPN Unadjusted', 'MEL', 'FPN',
#                       'bid_price-2', 'bid_price-1', 'offer_price-1',
#                       'offer_price-2', 'bl-2', 'bl-1', 'ol-1', 'ol-2']]
        elif (len(boalf) == 0) and len(bod) == 0:
            df = fpn_hh
        else:
            fpn_and_boalf = boalf.merge(fpn_min, how = 'outer', on = ['DateTime','bmunit_id'])
            fpn_and_boalf['Date'] = fpn_and_boalf['DateTime'].dt.date
         
            # joing bod with fpn and boalf
            all_bb = fpn_and_boalf.merge(bod, how = 'left', on = ['bmunit_id', 'Date', 'settlement_period'])
            
            # loop through and then have to merge left on level 1
            collect_bmus_hh = []
            #collect_bmus_all = []
            for bmu in tqdm(list(all_bb.bmunit_id.unique())):
                
                all_bmu_data = all_bb[(all_bb.bmunit_id == bmu)].copy()
                all_bmu_data = all_bmu_data.sort_values(by = ['DateTime', 'acceptance_number'], ascending = True)
                all_bmu_data = all_bmu_data.reset_index(drop = True)
                all_bmu_data.loc[all_bmu_data['BOA Value'] > all_bmu_data['MEL'],'BOA Value'] = all_bmu_data['MEL']
                all_bmu_data['BM Delta'] = all_bmu_data['BOA Value'] - all_bmu_data['FPN']
                
                # Bid Offers are MWh - get time difference between consecutive rows as fraction of an hour
                all_bmu_data['Time Delta'] = all_bmu_data[['DateTime']].diff(periods = -1)
                all_bmu_data['Time Delta'] = all_bmu_data.apply(lambda x: abs(x['Time Delta'].total_seconds() // 60), axis = 1)
                all_bmu_data['Hour Frac'] = all_bmu_data['Time Delta'] / 60
        
                # get difference between two consecutive values in BM
                all_bmu_data['BM Diff on time'] = all_bmu_data['BOA Value'].diff(periods = -1)
                all_bmu_data['BM Next'] = all_bmu_data['BOA Value'] - all_bmu_data['BM Diff on time'] 
                # volume calc considers a ramp up or down
                all_bmu_data['BM min'] = all_bmu_data[['BM Next', 'BOA Value']].min(axis = 1)
                all_bmu_data.loc[all_bmu_data['BM Next'].isna(),'BM min'] = np.nan
                all_bmu_data['BM max'] = all_bmu_data[['BM Next', 'BOA Value']].max(axis = 1)
                all_bmu_data['BM Volume'] = all_bmu_data.apply(lambda x: ((x['BM min'] + x['BM max']) /2) * x['Hour Frac'] if x['BM min'] != x['BM max'] else x['BM max'] * x['Hour Frac'], axis = 1)    
                
                # get difference between two consecutive values in FPN
                all_bmu_data['FPN Diff on time'] = all_bmu_data['FPN'].diff(periods = -1)
                all_bmu_data['FPN Next'] = all_bmu_data['FPN'] - all_bmu_data['FPN Diff on time'] 
                # volume calc considers a ramp up or down
                all_bmu_data['FPN min'] = all_bmu_data[['FPN Next', 'FPN']].min(axis = 1)
                all_bmu_data['FPN max'] = all_bmu_data[['FPN Next', 'FPN']].max(axis = 1)
                all_bmu_data['FPN Volume'] = all_bmu_data.apply(lambda x: (( x['FPN min'] + x['FPN max']) /2) * x['Hour Frac'] if x['FPN min'] != x['FPN max'] else x['FPN max'] * x['Hour Frac'], axis = 1)   
                
                # offer and bid volume in MWh
                # this logic to get volumes is CORRECT AND NECESSARY
                all_bmu_data.loc[:, 'CO Volume'] = all_bmu_data['FPN Volume']
                all_bmu_data.loc[(all_bmu_data['BOA Value'] < all_bmu_data['FPN']) | (all_bmu_data['BM Next'] < all_bmu_data['FPN Next']), 'Bid Volume'] = all_bmu_data['FPN Volume'] - all_bmu_data['BM Volume']
                all_bmu_data.loc[(all_bmu_data['BOA Value'] > all_bmu_data['FPN']) | (all_bmu_data['BM Next'] > all_bmu_data['FPN Next']), 'Offer Volume'] = all_bmu_data['BM Volume'] - all_bmu_data['FPN Volume']
                
                all_bmu_data.loc[(all_bmu_data['BOA Value'] < all_bmu_data['FPN']) | (all_bmu_data['BM Next'] < all_bmu_data['FPN Next']), 'CO Volume'] = all_bmu_data['FPN Volume'] - all_bmu_data['Bid Volume']        
                all_bmu_data.loc[(all_bmu_data['BOA Value'] > all_bmu_data['FPN']) | (all_bmu_data['BM Next'] > all_bmu_data['FPN Next']), 'CO Volume'] = all_bmu_data['BM Volume'] - all_bmu_data['Offer Volume']
                # this is needed to get correct offer/bid volumes
                all_bmu_data.loc[(all_bmu_data['BM Volume'].isna()) & (all_bmu_data['FPN Volume'] > 0), 'CO Volume'] = all_bmu_data['FPN Volume']
    
                # some of the above conditions qualifies for both offers and bids, but the incorrect volume will be negative
                # remove all negative values
                all_bmu_data.loc[all_bmu_data['Offer Volume'] < 0, 'Offer Volume'] = 0 
                all_bmu_data.loc[all_bmu_data['Bid Volume'] < 0, 'Bid Volume'] = 0 
                #lastly, make Bid Volume negative as it indicates reduction in volume
                all_bmu_data['Bid Volume'] = -1 * all_bmu_data['Bid Volume']
                # assume these things never happen
                all_bmu_data.loc[(all_bmu_data['bid_price-1'] == -9999) | (all_bmu_data['bid_price-1'] == -999), 'Bid Volume'] = 0
                
                #  get level volumes and volume weighted prices
                
                # Offers
                all_bmu_data.loc[:,'Offer Eval @ 1'] = all_bmu_data.apply(lambda x: min(x['BM Delta'], x['ol-1']), axis = 1)
                all_bmu_data.loc[:, 'Offer Eval @ 2'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'], x['ol-2']), axis = 1)
                all_bmu_data.loc[:, 'Offer Eval @ 3'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'], x['ol-3']), axis = 1)
                all_bmu_data.loc[:, 'Offer Eval @ 4'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'] - x['Offer Eval @ 3'], x['ol-4']), axis = 1)
                all_bmu_data.loc[:, 'Offer Eval @ 5'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'] - x['Offer Eval @ 3'] - x['Offer Eval @ 4'], x['ol-5']), axis = 1)
                
                all_bmu_data['Next Offer Eval @ 1'] = all_bmu_data['Offer Eval @ 1'].shift(-1)
                all_bmu_data['Next Offer Eval @ 2'] = all_bmu_data['Offer Eval @ 2'].shift(-1)
                all_bmu_data['Next Offer Eval @ 3'] = all_bmu_data['Offer Eval @ 3'].shift(-1)
                all_bmu_data['Next Offer Eval @ 4'] = all_bmu_data['Offer Eval @ 4'].shift(-1)
                all_bmu_data['Next Offer Eval @ 5'] = all_bmu_data['Offer Eval @ 5'].shift(-1)
                
                all_bmu_data['Offer Vol @ 1'] = all_bmu_data.apply(lambda x: ((x['Offer Eval @ 1'] + x['Next Offer Eval @ 1']) /2) * x['Hour Frac'] if x['Offer Eval @ 1'] != x['Next Offer Eval @ 1'] else x['Offer Eval @ 1'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Offer Vol @ 2'] = all_bmu_data.apply(lambda x: ((x['Offer Eval @ 2'] + x['Next Offer Eval @ 2']) /2) * x['Hour Frac'] if x['Offer Eval @ 2'] != x['Next Offer Eval @ 2'] else x['Offer Eval @ 2'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Offer Vol @ 3'] = all_bmu_data.apply(lambda x: ((x['Offer Eval @ 3'] + x['Next Offer Eval @ 3']) /2) * x['Hour Frac'] if x['Offer Eval @ 3'] != x['Next Offer Eval @ 3'] else x['Offer Eval @ 3'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Offer Vol @ 4'] = all_bmu_data.apply(lambda x: ((x['Offer Eval @ 4'] + x['Next Offer Eval @ 4']) /2) * x['Hour Frac'] if x['Offer Eval @ 4'] != x['Next Offer Eval @ 4'] else x['Offer Eval @ 4'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Offer Vol @ 5'] = all_bmu_data.apply(lambda x: ((x['Offer Eval @ 5'] + x['Next Offer Eval @ 5']) /2) * x['Hour Frac'] if x['Offer Eval @ 5'] != x['Next Offer Eval @ 5'] else x['Offer Eval @ 5'] * x['Hour Frac'], axis = 1)
                
                all_bmu_data.loc[all_bmu_data['Offer Vol @ 1'] < 0, 'Offer Vol @ 1'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Offer Vol @ 2'] < 0, 'Offer Vol @ 2'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Offer Vol @ 3'] < 0, 'Offer Vol @ 3'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Offer Vol @ 4'] < 0, 'Offer Vol @ 4'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Offer Vol @ 5'] < 0, 'Offer Vol @ 5'] = np.nan 
                
                
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 5'] > 0) & (all_bmu_data['offer_price-5'].isin([9999]) | all_bmu_data['offer_price-5'].isna()), 'Offer Vol @ 4'] = all_bmu_data['Offer Vol @ 4'] + all_bmu_data['Offer Vol @ 5']
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 5'] > 0) & (all_bmu_data['offer_price-5'].isin([9999]) | all_bmu_data['offer_price-5'].isna()), 'Offer Vol @ 5'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 4'] > 0) & (all_bmu_data['offer_price-4'].isin([9999]) | all_bmu_data['offer_price-4'].isna()), 'Offer Vol @ 3'] = all_bmu_data['Offer Vol @ 3'] + all_bmu_data['Offer Vol @ 4']
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 4'] > 0) & (all_bmu_data['offer_price-4'].isin([9999]) | all_bmu_data['offer_price-4'].isna()), 'Offer Vol @ 4'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 3'] > 0) & (all_bmu_data['offer_price-3'].isin([9999]) | all_bmu_data['offer_price-3'].isna()), 'Offer Vol @ 2'] = all_bmu_data['Offer Vol @ 2'] + all_bmu_data['Offer Vol @ 3']
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 3'] > 0) & (all_bmu_data['offer_price-3'].isin([9999]) | all_bmu_data['offer_price-3'].isna()), 'Offer Vol @ 3'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 2'] > 0) & (all_bmu_data['offer_price-2'].isin([9999]) | all_bmu_data['offer_price-2'].isna()), 'Offer Vol @ 1'] = all_bmu_data['Offer Vol @ 1'] + all_bmu_data['Offer Vol @ 2']
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 2'] > 0) & (all_bmu_data['offer_price-2'].isin([9999]) | all_bmu_data['offer_price-2'].isna()), 'Offer Vol @ 2'] = 0
    
                all_bmu_data.loc[(all_bmu_data['Offer Vol @ 1'] > 0) & (all_bmu_data['offer_price-1'].isin([9999])), 'Offer Vol @ 1'] = 0               

                offer_vol_cols = ['Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'Offer Vol @ 4', 'Offer Vol @ 5']
                all_bmu_data[offer_vol_cols] = all_bmu_data[offer_vol_cols].replace(0,np.nan)
                
#                all_bmu_data['Offer Income @ 1'] = all_bmu_data['offer_price-1'] * all_bmu_data['Offer Vol @ 1']
#                all_bmu_data['Offer Income @ 2'] = all_bmu_data['offer_price-2'] * all_bmu_data['Offer Vol @ 2']
#                all_bmu_data['Offer Income @ 3'] = all_bmu_data['offer_price-3'] * all_bmu_data['Offer Vol @ 3']
#                all_bmu_data['Offer Income @ 4'] = all_bmu_data['offer_price-4'] * all_bmu_data['Offer Vol @ 4']
#                all_bmu_data['Offer Income @ 5'] = all_bmu_data['offer_price-5'] * all_bmu_data['Offer Vol @ 5']
                
                #all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']] = all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']].fillna(0)
                #all_bmu_data['Offer Income'] = all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']].sum(axis = 1)
                
                #  Bids
            
                all_bmu_data.loc[:,'Bid Eval @ 1'] = all_bmu_data.apply(lambda x: max(x['BM Delta'], x['bl-1']), axis = 1)
                all_bmu_data.loc[:,'Bid Eval @ 2'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'], x['bl-2']), axis = 1)
                all_bmu_data.loc[:,'Bid Eval @ 3'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'], x['bl-3']), axis = 1)
                all_bmu_data.loc[:,'Bid Eval @ 4'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'] - x['Bid Eval @ 3'], x['bl-4']), axis = 1)
                all_bmu_data.loc[:,'Bid Eval @ 5'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'] - x['Bid Eval @ 3'] - x['Bid Eval @ 4'], x['bl-5']), axis = 1)
                
                all_bmu_data['Next Bid Eval @ 1'] = all_bmu_data['Bid Eval @ 1'].shift(-1)
                all_bmu_data['Next Bid Eval @ 2'] = all_bmu_data['Bid Eval @ 2'].shift(-1)
                all_bmu_data['Next Bid Eval @ 3'] = all_bmu_data['Bid Eval @ 3'].shift(-1)
                all_bmu_data['Next Bid Eval @ 4'] = all_bmu_data['Bid Eval @ 4'].shift(-1)
                all_bmu_data['Next Bid Eval @ 5'] = all_bmu_data['Bid Eval @ 5'].shift(-1)
                
                all_bmu_data['Bid Vol @ 1'] = all_bmu_data.apply(lambda x: ((x['Bid Eval @ 1'] + x['Next Bid Eval @ 1']) /2) * x['Hour Frac'] if x['Bid Eval @ 1'] != x['Next Bid Eval @ 1'] else x['Bid Eval @ 1'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Bid Vol @ 2'] = all_bmu_data.apply(lambda x: ((x['Bid Eval @ 2'] + x['Next Bid Eval @ 2']) /2) * x['Hour Frac'] if x['Bid Eval @ 2'] != x['Next Bid Eval @ 2'] else x['Bid Eval @ 2'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Bid Vol @ 3'] = all_bmu_data.apply(lambda x: ((x['Bid Eval @ 3'] + x['Next Bid Eval @ 3']) /2) * x['Hour Frac'] if x['Bid Eval @ 3'] != x['Next Bid Eval @ 3'] else x['Bid Eval @ 3'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Bid Vol @ 4'] = all_bmu_data.apply(lambda x: ((x['Bid Eval @ 4'] + x['Next Bid Eval @ 4']) /2) * x['Hour Frac'] if x['Bid Eval @ 4'] != x['Next Bid Eval @ 4'] else x['Bid Eval @ 4'] * x['Hour Frac'], axis = 1)
                all_bmu_data['Bid Vol @ 5'] = all_bmu_data.apply(lambda x: ((x['Bid Eval @ 5'] + x['Next Bid Eval @ 5']) /2) * x['Hour Frac'] if x['Bid Eval @ 5'] != x['Next Bid Eval @ 5'] else x['Bid Eval @ 5'] * x['Hour Frac'], axis = 1)
                
                all_bmu_data.loc[all_bmu_data['Bid Vol @ 1'] > 0, 'Bid Vol @ 1'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Bid Vol @ 2'] > 0, 'Bid Vol @ 2'] = np.nan  
                all_bmu_data.loc[all_bmu_data['Bid Vol @ 3'] > 0, 'Bid Vol @ 3'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Bid Vol @ 4'] > 0, 'Bid Vol @ 4'] = np.nan 
                all_bmu_data.loc[all_bmu_data['Bid Vol @ 5'] > 0, 'Bid Vol @ 5'] = np.nan 
                
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 5'] < 0) & (all_bmu_data['bid_price-5'].isin([-999, -9999]) | all_bmu_data['bid_price-5'].isna()), 'Bid Vol @ 4'] = all_bmu_data['Bid Vol @ 4'] + all_bmu_data['Bid Vol @ 5']
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 5'] < 0) & (all_bmu_data['bid_price-5'].isin([-999, -9999]) | all_bmu_data['bid_price-5'].isna()), 'Bid Vol @ 5'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 4'] < 0) & (all_bmu_data['bid_price-4'].isin([-999, -9999]) | all_bmu_data['bid_price-4'].isna()), 'Bid Vol @ 3'] = all_bmu_data['Bid Vol @ 3'] + all_bmu_data['Bid Vol @ 4']
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 4'] < 0) & (all_bmu_data['bid_price-4'].isin([-999, -9999]) | all_bmu_data['bid_price-4'].isna()), 'Bid Vol @ 4'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 3'] < 0) & (all_bmu_data['bid_price-3'].isin([-999, -9999]) | all_bmu_data['bid_price-3'].isna()), 'Bid Vol @ 2'] = all_bmu_data['Bid Vol @ 2'] + all_bmu_data['Bid Vol @ 3']
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 3'] < 0) & (all_bmu_data['bid_price-3'].isin([-999, -9999]) | all_bmu_data['bid_price-3'].isna()), 'Bid Vol @ 3'] = 0
                
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 2'] < 0) & (all_bmu_data['bid_price-2'].isin([-999, -9999]) | all_bmu_data['bid_price-2'].isna()), 'Bid Vol @ 1'] = all_bmu_data['Bid Vol @ 1'] + all_bmu_data['Bid Vol @ 2']
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 2'] < 0) & (all_bmu_data['bid_price-2'].isin([-999, -9999]) | all_bmu_data['bid_price-2'].isna()), 'Bid Vol @ 2'] = 0
    
                all_bmu_data.loc[(all_bmu_data['Bid Vol @ 1'] < 0) & (all_bmu_data['bid_price-1'].isin([-999, -9999])), 'Bid Vol @ 1'] = 0            
 
                bid_vol_cols = ['Bid Vol @ 1', 'Bid Vol @ 2', 'Bid Vol @ 3', 'Bid Vol @ 4', 'Bid Vol @ 5']
                all_bmu_data[bid_vol_cols] = all_bmu_data[bid_vol_cols].replace(0,np.nan)               
                
#                all_bmu_data['Bid Income @ 1'] = all_bmu_data['bid_price-1'] * all_bmu_data['Bid Vol @ 1']
#                all_bmu_data['Bid Income @ 2'] = all_bmu_data['bid_price-2'] * all_bmu_data['Bid Vol @ 2']
#                all_bmu_data['Bid Income @ 3'] = all_bmu_data['bid_price-3'] * all_bmu_data['Bid Vol @ 3']
#                all_bmu_data['Bid Income @ 4'] = all_bmu_data['bid_price-4'] * all_bmu_data['Bid Vol @ 4']
#                all_bmu_data['Bid Income @ 5'] = all_bmu_data['bid_price-5'] * all_bmu_data['Bid Vol @ 5']
                
                #all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']] = all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']].fillna(0)
                #all_bmu_data['Bid Income'] = all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']].sum(axis = 1)
                
                all_bmu_data['HH'] = all_bmu_data['DateTime'].dt.floor('30T')
                
                
                bmu_hh_summary = all_bmu_data.groupby(by = ['bmunit_id','HH']).agg({
                        'FPN Volume': 'sum',
                        'CO Volume' : 'sum',
                        'Bid Vol @ 5':'sum',
                        'Bid Vol @ 4':'sum',
                        'Bid Vol @ 3':'sum',
                        'Bid Vol @ 2':'sum',
                        'Bid Vol @ 1':'sum',
                        'Bid Volume' :'sum',
                        'Offer Volume': 'sum',
                        'Offer Vol @ 1':'sum',
                        'Offer Vol @ 2':'sum',
                        'Offer Vol @ 3':'sum',
                        'Offer Vol @ 4':'sum',
                        'Offer Vol @ 5':'sum',
                        'bid_price-5': 'mean',
                        'bid_price-4': 'mean',
                        'bid_price-3': 'mean',
                        'bid_price-2': 'mean',
                        'bid_price-1': 'mean',
                        'offer_price-1': 'mean',
                        'offer_price-2': 'mean',
                        'offer_price-3': 'mean',
                        'offer_price-4': 'mean',
                        'offer_price-5': 'mean',
                        'bl-5':'mean',
                        'bl-4':'mean',
                        'bl-3':'mean',
                        'bl-2':'mean',
                        'bl-1':'mean',
                        'ol-1':'mean',
                        'ol-2':'mean',
                        'ol-3':'mean',
                        'ol-4':'mean',
                        'ol-5':'mean',
                        })
                bmu_hh_summary.reset_index(inplace = True) 
#                bmu_hh_summary['VWA Bid Price'] = bmu_hh_summary['Bid Income'] / bmu_hh_summary['Bid Volume']
#                bmu_hh_summary['VWA Offer Price'] = bmu_hh_summary['Offer Income'] / bmu_hh_summary['Offer Volume']
                zero_cols = ['Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'Offer Vol @ 4', 'Offer Vol @ 5', 'Offer Volume',
                             'Bid Vol @ 1', 'Bid Vol @ 2', 'Bid Vol @ 3', 'Bid Vol @ 4', 'Bid Vol @ 5', 'Bid Volume']
                bmu_hh_summary[zero_cols] = bmu_hh_summary[zero_cols].replace(0,np.nan)  
                bmu_hh_summary.rename(columns = {'HH':'DateTime'}, inplace = True)
                collect_bmus_hh.append(bmu_hh_summary)
                #collect_bmus_all.append(all_bmu_data)
                #assert (all_bmu_data[~all_bmu_data['Offer Volume'].isna()]['Offer Volume'] == all_bmu_data[~all_bmu_data['Offer Volume'].isna()][['Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'Offer Vol @ 4', 'Offer Vol @ 5']].sum(axis = 1)).all(), 'Offer Level Volumes do not add up'
            df = pd.concat(collect_bmus_hh)
            df = df.merge(fpn_hh, on = ['bmunit_id','DateTime'], how = 'outer')

        return df

    df = get_bm_and_co_volumes()
    return df


