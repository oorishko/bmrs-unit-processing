# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 16:46:46 2021

@author: oorishko
"""

import pandas as pd
import seaborn as sns
wb = pd.read_csv(r'S:\Power Trading\Ostap\BMRS\WhiteleeWindTest.csv')
wb['DateTime'] = pd.to_datetime(wb['Start Time (BST)'], format = '%d/%m/%Y %H:%M')
wb.set_index('DateTime', inplace = True)
wb.index = wb.index.tz_localize('Europe/London', ambiguous = 'infer').tz_convert('UTC').tz_localize(None)

# price comparison
df.set_index('DateTime', inplace = True)
price_comp = df[['FPN Unadjusted','MEL', 'FPN','VWA Bid Price', 'VWA Offer Price','Bid Income', 'Offer Income','Bid Vol @ 5','Bid Vol @ 4','Bid Vol @ 3','Bid Vol @ 2','Bid Vol @ 1','Bid Volume', 'Offer Volume', 'Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'Offer Vol @ 4', 'Offer Vol @ 5', 'bid_price-3', 'bid_price-2', 'bid_price-1','offer_price-1', 'offer_price-2','offer_price-3', 'bl-3','bl-2','bl-1','ol-1','ol-2','ol-3']].merge(wb[['Pre-Balancing Dispatch (FPN) (MW)','Total Accepted Bid Level (MW)', 'Total Accepted Offer Level (MW)', 'Accepted Offer Price (?/MWh)', 'Accepted Bid Price (?/MWh)', 'Total Accepted Undo Offer Volume (MW)', 'Total Accepted Undo Bid Volume (MW)']], how = 'outer', left_index = True, right_index = True)

price_comp['OV via Levels'] = price_comp['Offer Vol @ 1'] + price_comp['Offer Vol @ 2'] + price_comp['Offer Vol @ 3'] + price_comp['Offer Vol @ 4'] + price_comp['Offer Vol @ 5']
price_comp['BV via Levels'] = price_comp['Bid Vol @ 1'] + price_comp['Bid Vol @ 2'] + price_comp['Bid Vol @ 3'] + price_comp['Bid Vol @ 4'] + price_comp['Bid Vol @ 5']
price_comp['VWAOP via Levels'] = price_comp['Offer Income'] / price_comp['OV via Levels']
price_comp['VWABP via Levels'] = price_comp['Bid Income'] / price_comp['BV via Levels']
price_comp['bid price error'] = price_comp['Accepted Bid Price (?/MWh)'] - price_comp['VWA Bid Price']
price_comp['offer price error'] = price_comp['Accepted Offer Price (?/MWh)'] - price_comp['VWA Offer Price']

price_comp['bid price error via levels'] = price_comp['Accepted Bid Price (?/MWh)'] - price_comp['VWABP via Levels']
price_comp['offer price error via levels'] = price_comp['Accepted Offer Price (?/MWh)'] - price_comp['VWAOP via Levels']
price_comp['Bid Delta'] = price_comp['Total Accepted Bid Level (MW)'] + price_comp['Total Accepted Undo Bid Volume (MW)'] - (price_comp['Bid Volume'] * 2)
price_comp['Offer Delta'] = price_comp['Total Accepted Offer Level (MW)'] + price_comp['Total Accepted Undo Offer Volume (MW)'] - (price_comp['Offer Volume'] * 2)
price_comp['Bid Vol Delta via levels'] = price_comp['Total Accepted Bid Level (MW)'] + price_comp['Total Accepted Undo Bid Volume (MW)'] - (price_comp['BV via Levels'] * 2)

price_comp = price_comp.replace([np.inf, -np.inf], np.nan)
price_comp['fpn_delta'] = price_comp['Pre-Balancing Dispatch (FPN) (MW)'] - price_comp['FPN']
# volumes and else
df['fpn_vol_delta'] = df['FPN'] - (df['FPN Volume'] * 2)
df_test = df[['FPN Unadjusted','MEL','FPN', 'FPN Volume','fpn_delta', 'CO Volume', 'Offer Volume','Bid Volume']]

df_test = df_test.merge(wb[['Pre-Balancing Dispatch (FPN) (MW)','Expected Metered Dispatch (MW)',
                            'Total Accepted Offer Level (MW)', 'Total Accepted Undo Offer Volume (MW)',
                            'Total Accepted Bid Level (MW)', 'Total Accepted Undo Bid Volume (MW)']]
                            , how = 'outer', left_index = True, right_index = True)
df_test['Bid Delta'] = df_test['Total Accepted Bid Level (MW)'] + df_test['Total Accepted Undo Bid Volume (MW)'] - (df_test['Bid Volume'] * 2)
df_test['Offer Delta'] = df_test['Total Accepted Offer Level (MW)'] + df_test['Total Accepted Undo Offer Volume (MW)'] - (df_test['Offer Volume'] * 2)

df_test[df_test['Pre-Balancing Dispatch (FPN) (MW)']>df_test['Expected Metered Dispatch (MW)'],'MEL Adjusted Bid Volume'] = df_test['FPN'] - df_test['Expected Metered Dispatch (MW)']

bid_prof = pd.read_csv(r'S:\Power Trading\Ostap\BMRS\BMRS Python\SPLN_Bid_profile1617.csv')
bid_prof['DateTime'] = pd.to_datetime(bid_prof['Start Time (BST)'], format = '%d/%m/%Y %H:%M')
bid_prof.set_index('DateTime', inplace = True)
bid_prof.index = bid_prof.index.tz_localize('Europe/London', ambiguous = 'infer').tz_convert('UTC').tz_localize(None)

bid_comp = df[['MEL', 'FPN','VWA Bid Price', 'Bid Vol @ 5','Bid Vol @ 4','Bid Vol @ 3','Bid Vol @ 2','Bid Vol @ 1','Bid Volume', 'bid_price-5', 'bid_price-4', 'bid_price-3', 'bid_price-2', 'bid_price-1','bl-3','bl-2','bl-1']].merge(bid_prof[['Pre-Balancing Dispatch (FPN) (MW)','Decl. Bid Level +1 (MW)', 'Decl. Bid Level +2 (MW)','Decl. Bid Level +3 (MW)', 'Decl. Bid Level +4 (MW)', 'Decl. Bid Level +5 (MW)', 'Decl. Bid Price +1 (?/MWh)','Decl. Bid Price +2 (?/MWh)', 'Decl. Bid Price +3 (?/MWh)','Decl. Bid Price +4 (?/MWh)', 'Decl. Bid Price +5 (?/MWh)','Accepted Bid Price (?/MWh)']], how = 'outer', left_index = True, right_index = True)
bid_comp['bp3-delta'] = bid_comp['Decl. Bid Price +3 (?/MWh)'] - bid_comp['bid_price-3']
bid_comp['bp2-delta'] = bid_comp['Decl. Bid Price +2 (?/MWh)'] - bid_comp['bid_price-2']
bid_comp['bp1-delta'] = bid_comp['Decl. Bid Price +1 (?/MWh)'] - bid_comp['bid_price-1']

'Decl. Bid Level +3 (MW)'
bid_comp['bl1-delta'] = bid_comp['Decl. Bid Level +1 (MW)'] + bid_comp['Bid Vol @ 1']*2
bid_comp['bl2-delta'] = bid_comp['Decl. Bid Level +2 (MW)'] + bid_comp['bl-2']
bid_comp['bl3-delta'] = bid_comp['Decl. Bid Level +3 (MW)'] + bid_comp['bl-3']


bid_test = price_comp[['Bid Delta', 'BV via Levels','Bid Volume', 'Bid Vol @ 1', 'Bid Vol @ 2','bid_price-1','bid_price-2','bl-1','bl-2','bid price error via levels', 'bid price error', 'VWABP via Levels', 'VWA Bid Price', 'Total Accepted Bid Level (MW)', 'Accepted Bid Price (?/MWh)']]
