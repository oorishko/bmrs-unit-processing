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

IDEAL_PPTH_TO_GBP_MWH_MULTIPLIER = 0.341214245
GAS_EFF = 0.42
EMISSIONS_FACTOR =  0.1841639
emissions_factor =  EMISSIONS_FACTOR
pays_EUAs = False
variable_commodity_charge_ppth = 1.184
#peaker_dict = {
#        'Bloxwich Battery':'E_ARNKB-1',
#        'Roundponds Battery': 'V__HHABI001'
#        }

#units = ['T_WBUPS-2','T_CGTHW-1','E_ARNKB-1','T_HUMR-1', 'T_MRWD-1']
def bmrs_flow(units):
    units_str = "'" + "','".join(units)+ "'"
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
        
        sql_query = '''SELECT bmunit_id, from_time, to_time, from_level, to_level, settlement_period 
                    FROM BMRS_PN_New
                    WHERE bmunit_id in ({})
                    '''.format(units_str)
        
        fpn_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        for bmu in list(fpn_data.bmunit_id.unique()):
            fpn = fpn_data[fpn_data.bmunit_id == bmu].copy()
            fpn['from_time'] = pd.to_datetime(fpn['from_time'], format = '%Y-%m-%d %H:%M:%S')
            fpn['to_time'] = pd.to_datetime(fpn['to_time'], format = '%Y-%m-%d %H:%M:%S')
            
            fpn = fpn.sort_values(by = 'from_time', ascending = True).reset_index(drop = True)
        
            fpn.set_index('from_time', inplace = True)
            fpn['date'] = fpn.index.date
            to_times = fpn.loc[fpn.groupby(['date', 'settlement_period']).to_time.idxmax()][['to_level', 'to_time', 'settlement_period']]
            to_times.reset_index(drop = True, inplace = True)
            to_times = to_times.rename(columns = {'to_level' : 'from_level', 'to_time':'from_time'})
            to_times.set_index('from_time', inplace = True)
            to_times['order'] = 2
            fpn = fpn[['from_level']]
            fpn['order'] = 1
            fpn = pd.concat([fpn, to_times[['from_level', 'order']]])
            fpn.reset_index(inplace = True)
            fpn.drop_duplicates(subset = ['from_time', 'from_level'], inplace = True)
            fpn = fpn.sort_values(by = ['from_time', 'order'], ascending = True)
            fpn.set_index('from_time', inplace = True)
            
            fpn = fpn[~fpn.index.duplicated(keep = 'first')][['from_level']]
            
            #time_range = pd.date_range(start = str(fpn.index[0]), end = str(fpn.index[-1]), freq = '30T')
            fpn_min = fpn.resample('1T').interpolate()
            fpn_hh = fpn_min.resample('30T').mean()
            
            fpn_hh = fpn_hh.rename(columns = {'from_level':'FPN'}).rename_axis('DateTime')
            fpn_min = fpn_min.rename(columns = {'from_level':'FPN'}).rename_axis('DateTime')
            fpn_hh['bmunit_id'] = bmu
            fpn_min['bmunit_id'] = bmu
            combine_fpn_min.append(fpn_min)
            combine_fpn_hh.append(fpn_hh)
        
        fpn_min = pd.concat(combine_fpn_min)
        fpn_hh = pd.concat(combine_fpn_hh)
        return fpn_min, fpn_hh
    
    def get_boalf_profile():
        sql_query = '''
                    SELECT * 
                    FROM BMRS_BOALF
                    WHERE bmunit_id in ({})
                    '''.format(units_str)
    
        boalf_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        boalf_list = []
        for bmu in boalf_data.bmunit_id.unique():
            boalf = boalf_data[boalf_data.bmunit_id == bmu].copy()
            boalf['DateTime'] = pd.to_datetime(boalf['DateTime'])
            boalf = boalf.sort_values(by = 'DateTime', ascending = True)
            
            boalf.set_index('DateTime', inplace = True)
            boalf['Date'] = boalf.index.date
            start = boalf.iloc[[0]].index[0].floor('30T')
            end = boalf.iloc[[len(boalf)-1]].index[0].ceil('30T')
            boalf_range = pd.date_range(start = start, end = end, freq= '30T')   
            boalf_hh = pd.DataFrame({'DateTime': boalf_range, 'level': 0})
            boalf_hh.set_index('DateTime', inplace = True)
        
            df = boalf.merge(boalf_hh, how = 'outer', left_index = True, right_index = True, suffixes = ['', '0'])
            df['Date'] = df.index.date
            df.rename(columns = {'level':'BOA Value', 'level0': 'BOA 0 Ref'}, inplace = True)
            df['bmunit_id'] = bmu
            boalf_list.append(df)
            
        boalf = pd.concat(boalf_list)
        return boalf
    
    def get_bod_profile():
        sql_query = '''
                    SELECT * 
                    FROM BMRS_BOD
                    WHERE bmunit_id in ({})
                    '''.format(units_str)
                    
        bod_data = sql.get_data(sql_query, sandbox_or_production = 'sandbox')
        
        bod_list = []
        for bmu in list(bod_data.bmunit_id.unique()):
            print(bmu)
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
            bod_clean = bod_clean.groupby(by = ['Date', 'settlement_period']).agg({
                    'from_time': 'first',
                    'to_time': 'first',
                    'bmunit_id': 'first',
                    'bm_offer_pair_number': list, 
                    'from_level': list,
                    'to_level': list,
                    'bid_price': 'first',
                    'offer_price' : 'first'})
            bod_clean.reset_index(inplace = True)        
            bod_clean.set_index('from_time',inplace = True)
            try:
                bod_clean['bid_levels'] = bod_clean.apply(lambda x: x['from_level'][0], axis = 1)
            except:
                print('BMU {} did not submit Bid Levels'.format(bmu))
            try:   
                bod_clean['offer_levels'] = bod_clean.apply(lambda x: x['from_level'][1], axis = 1)
            except:
                print('BMU {} did not submit Offer Levels'.format(bmu))         
            if 'bid_price' in bod_clean.columns:
                bod_clean['bid_price-5'] = bod_clean.apply(lambda x: x['bid_price'][-5] if len(x['bid_price']) > 4 else np.nan, axis = 1)
                bod_clean['bid_price-4'] = bod_clean.apply(lambda x: x['bid_price'][-4] if len(x['bid_price']) > 3 else np.nan, axis = 1)
                bod_clean['bid_price-3'] = bod_clean.apply(lambda x: x['bid_price'][-3] if len(x['bid_price']) > 2 else np.nan, axis = 1)
                bod_clean['bid_price-2'] = bod_clean.apply(lambda x: x['bid_price'][-2] if len(x['bid_price']) > 1 else np.nan, axis = 1)
                bod_clean['bid_price-1'] = bod_clean.apply(lambda x: x['bid_price'][-1], axis = 1)
            else:
                bod_clean['bid_price-5', 'bid_price-4', 'bid_price-3', 'bid_price-2', 'bid_price-1'] = np.nan
            if 'offer_price' in bod_clean.columns:
                bod_clean['offer_price-1'] = bod_clean.apply(lambda x: x['offer_price'][0], axis = 1)
                bod_clean['offer_price-2'] = bod_clean.apply(lambda x: x['offer_price'][1] if len(x['offer_price']) > 1 else np.nan, axis = 1)
                bod_clean['offer_price-3'] = bod_clean.apply(lambda x: x['offer_price'][2] if len(x['offer_price']) > 2 else np.nan, axis = 1)
                bod_clean['offer_price-4'] = bod_clean.apply(lambda x: x['offer_price'][3] if len(x['offer_price']) > 3 else np.nan, axis = 1)
                bod_clean['offer_price-5'] = bod_clean.apply(lambda x: x['offer_price'][4] if len(x['offer_price']) > 4 else np.nan, axis = 1)
            else:
                bod_clean['offer_price-1','offer_price-2','offer_price-3','offer_price-4','offer_price-5'] = np.nan
                
            if 'bid_levels' in bod_clean.columns:
                bod_clean['bl-5'] = bod_clean.apply(lambda x: x['bid_levels'][-5] if len(x['bid_levels']) > 4 else np.nan, axis = 1)
                bod_clean['bl-4'] = bod_clean.apply(lambda x: x['bid_levels'][-4] if len(x['bid_levels']) > 3 else np.nan, axis = 1)
                bod_clean['bl-3'] = bod_clean.apply(lambda x: x['bid_levels'][-3] if len(x['bid_levels']) > 2 else np.nan, axis = 1)
                bod_clean['bl-2'] = bod_clean.apply(lambda x: x['bid_levels'][-2] if len(x['bid_levels']) > 1 else np.nan, axis = 1)
                bod_clean['bl-1'] = bod_clean.apply(lambda x: x['bid_levels'][-1], axis = 1)
            else:
                bod_clean[['bl-5', 'bl-4', 'bl-3', 'bl-2','bl-1']] = np.nan
                
            if 'offer_levels' in bod_clean.columns:    
                bod_clean['ol-1'] = bod_clean.apply(lambda x: x['offer_levels'][0], axis = 1)
                bod_clean['ol-2'] = bod_clean.apply(lambda x: x['offer_levels'][1] if len(x['offer_levels']) > 1 else np.nan, axis = 1)
                bod_clean['ol-3'] = bod_clean.apply(lambda x: x['offer_levels'][2] if len(x['offer_levels']) > 2 else np.nan, axis = 1)
                bod_clean['ol-4'] = bod_clean.apply(lambda x: x['offer_levels'][3] if len(x['offer_levels']) > 3 else np.nan, axis = 1)
                bod_clean['ol-5'] = bod_clean.apply(lambda x: x['offer_levels'][4] if len(x['offer_levels']) > 4 else np.nan, axis = 1)
            else:
                bod_clean['ol-1', 'ol-2', 'ol-3', 'ol-4', 'ol-5'] = np.nan
                
            bod_clean.drop(['bm_offer_pair_number', 'from_level', 'to_level', 'bid_price', 'offer_price'], axis = 1, inplace = True)
            bod_list.append(bod_clean)
        bod_final = pd.concat(bod_list)
        return bod_final
    
    def get_bm_and_co_volumes():
        # get fpn
        fpn_min, fpn_hh = get_fpn_profile()
        fpn_min = fpn_min.reset_index()
        fpn_hh = fpn_hh.reset_index()
        
        # get boalf
        boalf = get_boalf_profile()
        boalf = boalf.reset_index()
        # get fpn and boalf together
        
        # when no BM is in play, using HH resampled FPN is more accurate
        # separate them out into merging minutely and HH
        df_wo_boalf = boalf.loc[boalf.acceptance_number.isna(), :]
        df_with_boalf = boalf.loc[boalf.acceptance_number.notna(), :]
        fpn_with_boalf = df_with_boalf.merge(fpn_min, how = 'left', on = ['DateTime','bmunit_id'])
        fpn_wo_boalf = df_wo_boalf.merge(fpn_hh, how = 'left', on = ['DateTime','bmunit_id'])
        fpn_and_boalf = pd.concat([fpn_with_boalf, fpn_wo_boalf])
        fpn_and_boalf.sort_values(inplace = True, by = ['bmunit_id','DateTime'])
        # keep Dates
        fpn_and_boalf.reset_index(inplace = True)
        
        # get bod
        bod = get_bod_profile()
        bod.reset_index(inplace = True)
        
        # joing bod with fpn and boalf
        all_bb = fpn_and_boalf.merge(bod, how = 'left', on = ['bmunit_id', 'Date', 'settlement_period'])
        
        # I think here is the place where you can split data by bid/offer levels + nans and do the thing
        # the final output should be HH volumes for each level
        
        # loop through and then have to merge left on level 1
        collect_bmus_hh = []
        #collect_bmus_all = []
        for bmu in tqdm(list(all_bb.bmunit_id.unique())):
            all_bmu_data = all_bb[(all_bb.bmunit_id == bmu)].copy()
            all_bmu_data = all_bmu_data.sort_values(by = ['DateTime', 'acceptance_number'], ascending = True)
            all_bmu_data = all_bmu_data.reset_index(drop = True)
            all_bmu_data.drop(['index'], axis = 1, inplace = True)
            all_bmu_data['BM Delta'] = all_bmu_data['BOA Value'] - all_bmu_data['FPN']
            
                # flag if BM period finished - if it did, the period that follows is back to FPN
        #        all_bmu_data['Go by Boalf'] = all_bmu_data[['level']].diff(periods = -1)
        #        all_bmu_data.loc[all_bmu_data['Go by Boalf'].notna(), 'Go by Boalf'] = 1
        #        all_bmu_data.loc[(all_bmu_data['Go by Boalf'].isna()) & (all_bmu_data['level'].notna()), 'Go by Boalf'] = 0
            
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
            all_bmu_data.loc[(all_bmu_data['BOA Value'] > all_bmu_data['FPN']) | (all_bmu_data['BM Next'] > all_bmu_data['FPN Next']), 'Offer Volume'] = all_bmu_data['BM Volume'] - all_bmu_data['FPN Volume']
            all_bmu_data.loc[(all_bmu_data['BOA Value'] > all_bmu_data['FPN']) | (all_bmu_data['BM Next'] > all_bmu_data['FPN Next']), 'CO Volume'] = all_bmu_data['BM Volume'] - all_bmu_data['Offer Volume']
            
            all_bmu_data.loc[(all_bmu_data['BOA Value'] < all_bmu_data['FPN']) | (all_bmu_data['BM Next'] < all_bmu_data['FPN Next']), 'Bid Volume'] = all_bmu_data['FPN Volume'] - all_bmu_data['BM Volume']
            all_bmu_data.loc[(all_bmu_data['BOA Value'] < all_bmu_data['FPN']) | (all_bmu_data['BM Next'] < all_bmu_data['FPN Next']), 'CO Volume'] = all_bmu_data['FPN Volume'] - all_bmu_data['Bid Volume']
            
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
            all_bmu_data.loc[:, 'Offer Eval @ 2'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'], x['ol-2'] - x['Offer Eval @ 1']), axis = 1)
            all_bmu_data.loc[:, 'Offer Eval @ 3'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'], x['ol-3'] - x['Offer Eval @ 2']), axis = 1)
            all_bmu_data.loc[:, 'Offer Eval @ 4'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'] - x['Offer Eval @ 3'], x['ol-4'] - x['Offer Eval @ 3']), axis = 1)
            all_bmu_data.loc[:, 'Offer Eval @ 5'] = all_bmu_data.apply(lambda x: min(x['BM Delta'] - x['Offer Eval @ 1'] - x['Offer Eval @ 2'] - x['Offer Eval @ 3'] - x['Offer Eval @ 4'], x['ol-5'] - x['Offer Eval @ 4']), axis = 1)
            
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
            
            all_bmu_data.loc[all_bmu_data['Offer Vol @ 1'] < 0, 'Offer Vol @ 1'] = 0 
            all_bmu_data.loc[all_bmu_data['Offer Vol @ 2'] < 0, 'Offer Vol @ 2'] = 0 
            all_bmu_data.loc[all_bmu_data['Offer Vol @ 3'] < 0, 'Offer Vol @ 3'] = 0 
            all_bmu_data.loc[all_bmu_data['Offer Vol @ 4'] < 0, 'Offer Vol @ 4'] = 0 
            all_bmu_data.loc[all_bmu_data['Offer Vol @ 5'] < 0, 'Offer Vol @ 5'] = 0 
            
            all_bmu_data['Offer Income @ 1'] = all_bmu_data['offer_price-1'] * all_bmu_data['Offer Vol @ 1']
            all_bmu_data['Offer Income @ 2'] = all_bmu_data['offer_price-2'] * all_bmu_data['Offer Vol @ 2']
            all_bmu_data['Offer Income @ 3'] = all_bmu_data['offer_price-3'] * all_bmu_data['Offer Vol @ 3']
            all_bmu_data['Offer Income @ 4'] = all_bmu_data['offer_price-4'] * all_bmu_data['Offer Vol @ 4']
            all_bmu_data['Offer Income @ 5'] = all_bmu_data['offer_price-5'] * all_bmu_data['Offer Vol @ 5']
            
            all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']] = all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']].fillna(0)
            all_bmu_data['Offer Income'] = all_bmu_data[['Offer Income @ 1', 'Offer Income @ 2', 'Offer Income @ 3', 'Offer Income @ 4', 'Offer Income @ 5']].sum(axis = 1)
            
            #  Bids
        
            all_bmu_data.loc[:,'Bid Eval @ 1'] = all_bmu_data.apply(lambda x: max(x['BM Delta'], x['bl-1']), axis = 1)
            all_bmu_data.loc[:,'Bid Eval @ 2'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'], x['bl-2'] - x['Bid Eval @ 1']), axis = 1)
            all_bmu_data.loc[:,'Bid Eval @ 3'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'], x['bl-3'] - x['Bid Eval @ 2']), axis = 1)
            all_bmu_data.loc[:,'Bid Eval @ 4'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'] - x['Bid Eval @ 3'], x['bl-4'] - x['Bid Eval @ 3']), axis = 1)
            all_bmu_data.loc[:,'Bid Eval @ 5'] = all_bmu_data.apply(lambda x: max(x['BM Delta'] - x['Bid Eval @ 1'] - x['Bid Eval @ 2'] - x['Bid Eval @ 3'] - x['Bid Eval @ 4'], x['bl-5'] - x['Bid Eval @ 4']), axis = 1)
            
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
            
            all_bmu_data.loc[all_bmu_data['Bid Vol @ 1'] > 0, 'Bid Vol @ 1'] = 0 
            all_bmu_data.loc[all_bmu_data['Bid Vol @ 2'] > 0, 'Bid Vol @ 2'] = 0 
            all_bmu_data.loc[all_bmu_data['Bid Vol @ 3'] > 0, 'Bid Vol @ 3'] = 0 
            all_bmu_data.loc[all_bmu_data['Bid Vol @ 4'] > 0, 'Bid Vol @ 4'] = 0 
            all_bmu_data.loc[all_bmu_data['Bid Vol @ 5'] > 0, 'Bid Vol @ 5'] = 0 
            
            all_bmu_data['Bid Income @ 1'] = all_bmu_data['bid_price-1'] * all_bmu_data['Bid Vol @ 1']
            all_bmu_data['Bid Income @ 2'] = all_bmu_data['bid_price-2'] * all_bmu_data['Bid Vol @ 2']
            all_bmu_data['Bid Income @ 3'] = all_bmu_data['bid_price-3'] * all_bmu_data['Bid Vol @ 3']
            all_bmu_data['Bid Income @ 4'] = all_bmu_data['bid_price-4'] * all_bmu_data['Bid Vol @ 4']
            all_bmu_data['Bid Income @ 5'] = all_bmu_data['bid_price-5'] * all_bmu_data['Bid Vol @ 5']
            
            all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']] = all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']].fillna(0)
            all_bmu_data['Bid Income'] = all_bmu_data[['Bid Income @ 1', 'Bid Income @ 2', 'Bid Income @ 3', 'Bid Income @ 4', 'Bid Income @ 5']].sum(axis = 1)
            
            all_bmu_data['HH'] = all_bmu_data['DateTime'].dt.floor('30T')
            all_bmu_data = all_bmu_data.fillna(0)
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
                    'Bid Income @ 5':'sum',
                    'Bid Income @ 4':'sum',
                    'Bid Income @ 3':'sum',
                    'Bid Income @ 2':'sum',
                    'Bid Income @ 1':'sum',
                    'Bid Income':'sum',
                    'Offer Income':'sum',
                    'Offer Income @ 1':'sum',
                    'Offer Income @ 2':'sum',
                    'Offer Income @ 3':'sum',
                    'Offer Income @ 4':'sum',
                    'Offer Income @ 5':'sum',
                    })
            bmu_hh_summary.reset_index(inplace = True) 
            bmu_hh_summary['VWA Bid Price'] = bmu_hh_summary['Bid Income'] / bmu_hh_summary['Bid Volume']
            bmu_hh_summary['VWA Offer Price'] = bmu_hh_summary['Offer Income'] / bmu_hh_summary['Offer Volume']
            bmu_hh_summary.rename(columns = {'HH':'ValueDate'}, inplace = True)
            collect_bmus_hh.append(bmu_hh_summary)
            #collect_bmus_all.append(all_bmu_data)
            #assert (all_bmu_data[~all_bmu_data['Offer Volume'].isna()]['Offer Volume'] == all_bmu_data[~all_bmu_data['Offer Volume'].isna()][['Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'Offer Vol @ 4', 'Offer Vol @ 5']].sum(axis = 1)).all(), 'Offer Level Volumes do not add up'
        df = pd.concat(collect_bmus_hh)
        #bmu_level_data = pd.concat(collect_bmus_all)
        return df

    st = time.time()
    df = get_bm_and_co_volumes()
    et = time.time()
    print('This took {} minutes.'.format((et-st)/60))
    return df
#    # BOD LEVELS CHECK SHOULD GO HERE
#    
#    # getting co/sap/da data
#    sd = str(df.loc[0]['index'])
#    ed = str(df.loc[len(df)-1]['index'])
#    co = get_cashout_historic(sd, ed)
#    sap = get_sap(start_date = sd, end_date = ed)
#    da = get_da_hh(start_date = sd, end_date = ed)
#    
#    co.reset_index(inplace = True)
#    sap.reset_index(inplace = True)
#    da.reset_index(inplace = True)
#    
#    df['hh'] = df['index'].dt.floor('30T')
#    co = co.rename(columns = {'Datetime':'hh'})
#    sap = sap.rename(columns = {'Datetime':'hh'})
#    da = da.rename(columns = {'Datetime':'hh', 'APX DA HH (£/MWh)': 'DA (£/MWh)'})
#    
#    df = df.merge(co, on = ['hh'], how = 'left')
#    df = df.merge(sap, on = ['hh'], how = 'left')
#    df = df.merge(da, on = ['hh'], how = 'left')
#    
#    df['SAP (£/MWh)'] = ((df['SAP_ppth'] * IDEAL_PPTH_TO_GBP_MWH_MULTIPLIER) / GAS_EFF)
#    df['Dirty Spark Spread (£/MWh)'] = df['Cashout (£/MWh)'] - df['SAP (£/MWh)']
#    
#    # total gas
#    gas = gd.get_gas_price_per_MWh_pre_o_and_m('',
#                                              sd,
#                                              ed,
#                                              efficiency = GAS_EFF,
#                                              variable_commodity_charge_ppth = variable_commodity_charge_ppth ,
#                                              breakdown = True,
#                                              pays_EUAs = False,
#                                              price_source_EUAs = 'dict1',
#                                              emissions_factor = emissions_factor,
#                                              utc_or_local = 'utc',
#                                              gas_flex_perc = 0)
#    
#    gas.index  = pd.DatetimeIndex([i.replace(tzinfo=None) for i in gas.index])
#    gas = gas.rename_axis('hh').reset_index()
#    df = df.merge(gas[['hh','Total Gas Cost £']], how = 'left', on = ['hh'])
#    
#    # get all incomes and expenses 
#    df['Gross CO Income'] = df['CO Volume'] * df['Cashout (£/MWh)']
#    df['Gross DA Income'] = df['CO Volume'] * df['DA (£/MWh)']
#    df['SAP Expense in BM'] = df['SAP (£/MWh)'] * df['Offer Volume']
#    df['SAP Expense in CO'] = df['SAP (£/MWh)'] * df['CO Volume']
#    df['Total Gas Expense in BM'] = df['Total Gas Cost £'] * df['Offer Volume']
#    df['Total Gas Expense in CO'] = df['Total Gas Cost £'] * df['CO Volume']
#    for col in ['Offer Volume', 'Bid Volume', 'CO Volume','Gross Offer Income', 'Gross Bid Income', 'Gross CO Income', 'Gross DA Income', 'SAP Expense in BM', 'SAP Expense in CO', 'Total Gas Expense in BM', 'Total Gas Expense in CO']:
#        df[col] = df[col].fillna(0)
#    
#    df['HH diff'] = df['hh'].diff(periods = -1)
#    df['HH diff'] = df.apply(lambda x: abs(x['HH diff'].total_seconds() // 60), axis = 1)
#    df['CO'] = df.apply(lambda x: x['Cashout (£/MWh)'] if x['HH diff'] != 0 else np.nan, axis = 1)
#    df['BP'] = df.apply(lambda x: x['bid_price'] if x['HH diff'] != 0 else np.nan, axis = 1)
#    df['OP'] = df.apply(lambda x: x['offer_price'] if x['HH diff'] != 0 else np.nan, axis = 1)
#    
#    hh_summary = df.groupby(by = 'hh').agg({
#            'Offer Volume': 'sum',
#            'Bid Volume' :'sum',
#            'CO Volume' : 'sum',
#            'Gross Offer Income': 'sum',
#            'Gross Bid Income': 'sum',
#            'Gross CO Income': 'sum',
#            'Gross DA Income': 'sum',
#            'SAP Expense in BM': 'sum',
#            'SAP Expense in CO' : 'sum',
#            'Total Gas Expense in BM': 'sum',
#            'Total Gas Expense in CO' : 'sum',
#            'Dirty Spark Spread (£/MWh)': 'mean',
#            'Cashout (£/MWh)': 'mean', 
#            'DA (£/MWh)' : 'mean'
#            })
#    
#    return df, hh_summary
#
#all_start = time.time()
#dfs = []
#for bmu_name in list(peaker_dict.keys()):
#    bmu_id = peaker_dict[bmu_name]
#    print(bmu_name)
#    df, money_df = get_approx_income(bmu_id)
#    money_df['bmunit_id'] = bmu_id
#    money_df['mws'] = max(money_df['CO Volume'].max(), money_df['Bid Volume'].max(), money_df['Offer Volume'].max())
#    print(bmu_id)
#    dfs.append(money_df)
#    print(bmu_name)
#dfs = pd.concat(dfs)
#dfs.to_csv(r'S:\Power Trading\Ostap\BMRS\Batteries_{}.csv'.format(random.randint(10**6, 10**7)))
#all_end = time.time()
#print("This took {:.2f} minutes".format((all_end - all_start)//60))

#
#def get_sap(start_date, end_date):
#    
#    sql_string = '''--SAP ppth
#                Select
#                ValueDate as [Datetime],
#                Value as [SAP_ppth]
#                from V_RawTimeseries x
#                Where x.CurveID = 100659
#                AND ValueDate >= '2011-01-01' --want history for average going forward; trimmed for return at end
#                AND ValueDate <= 'end_date'
#                AND ScenarioID = 0
#                ORDER BY ValueDate'''
#    
#    sql_string = sql_string.replace('start_date', start_date)
#    sql_string = sql_string.replace('end_date', end_date)
#    
#    actuals = sql.get_data(sql_string, 'production', 'KElliott@Hartreepartners.com', 'KElliott@h163')
#    actuals['Datetime'] = pd.to_datetime(actuals['Datetime'])
#    actuals.set_index('Datetime', inplace = True)
#    actuals = actuals.resample('30T').mean().ffill()
#    
#    f = actuals[start_date : end_date]
#             
#    return f
#
#def get_da_hh(start_date='2013-01-01', end_date='2025-01-01'):
#    
#    curve_id = 106751421
#    value_name = 'APX DA HH (£/MWh)'
#    utc_or_local = 'utc'
#    f = sql.get_fdm_curve(curve_id = curve_id,
#                          start_date = start_date,
#                          end_date = end_date,
#                          utc_or_local = utc_or_local,
#                          value_name = value_name)
#    
#    f = f.resample('30T').ffill()
#        
#    return f
#
#def get_cashout_historic(sd, ed):
#    sql_string = '''--Cashout
#                    SELECT
#                    ValueDate as [Datetime],
#                    [910000345] AS 'DF',
#                    [910000344] AS 'RF',
#                    [910000343] AS 'R3',
#                    [910000342] AS 'R2',
#                    [910000341] AS 'R1',
#                    [910000340] AS 'SF',
#                    [910000339] AS 'II',
#                    [910000338] AS 'InitialOutturn',
#                    COALESCE([910000345], [910000344], [910000343], [910000342], [910000341], [910000340], [910000339], [910000338]) AS [Cashout (£/MWh)]
#                    FROM
#                        (
#                          SELECT
#                            ValueDate,
#                            CurveID,
#                            Value
#                          FROM V_RawTimeseries x
#                          WHERE x.CurveID IN (910000345, 910000344, 910000343, 910000342, 910000341, 910000340, 910000339, 910000338)
#                          AND x.ValueDate >= 'start_date'
#                          AND x.ValueDate <= 'end_date'
#                          AND x.ScenarioID = 0
#                        ) p
#                      PIVOT
#                      (
#                        Avg([Value])
#                      FOR [CurveID] IN ([910000345], [910000344], [910000343], [910000342], [910000341], [910000340], [910000339], [910000338])
#                      ) AS pvt
#                    ORDER BY ValueDate
#                    '''
#    
#    sql_string = sql_string.replace('start_date', sd)
#    sql_string = sql_string.replace('end_date', ed)
#      
#    f = sql.get_data(sql_string)
#    f = f[['Datetime','Cashout (£/MWh)']]
#    f['Datetime'] = pd.to_datetime(f['Datetime'], format = '%Y-%m-%d %H:%M:%S')
#    f.set_index('Datetime', inplace = True)
#    return f

#bmunit_id = 'E_LSTWY-1'
#all_bmu_data = get_bm_and_co_volumes(bmunit_id)



