# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 16:46:46 2021

@author: oorishko
"""

#import pandas as pd
#import seaborn as sns
#wb = pd.read_csv(r'S:\Power Trading\Ostap\BMRS\BMRS Python\WB_HH_BM_2016.csv')
#wb['DateTime'] = pd.to_datetime(wb['Start Time (BST)'], format = '%d/%m/%Y %H:%M')
#wb.set_index('DateTime', inplace = True)
#wb.index = wb.index.tz_localize('Europe/London', ambiguous = 'infer').tz_convert('UTC').tz_localize(None)
#wb['Pre-Balancing Dispatch (FPN) (MW)']
#
#df.set_index('DateTime', inplace = True)
## fpn comparison
#fpn_comp = df[['FPN', 'FPN Unadjusted', 'FPN Volume']].merge(wb[['Pre-Balancing Dispatch (FPN) (MW)']], how = 'outer', left_index = True, right_index = True)
#fpn_comp['Delta'] = fpn_comp['Pre-Balancing Dispatch (FPN) (MW)'] - fpn_comp['FPN']
#fpn_comp['Unj Delta'] = fpn_comp['Pre-Balancing Dispatch (FPN) (MW)'] - fpn_comp['FPN Unadjusted']
#
##offer volume comp
## Total Accepted Offer Level (MW)
#offer_comp = df[['FPN','FPN Volume', 'MEL','CO Volume','Offer Volume']].merge(wb[['Total Accepted Offer Level (MW)', 'Total Accepted Undo Offer Volume (MW)']], how = 'outer', left_index = True, right_index = True)
#offer_comp['Delta'] = offer_comp['Total Accepted Offer Level (MW)'] +offer_comp['Total Accepted Undo Offer Volume (MW)'] - (offer_comp['Offer Volume'] * 2)
#
##bid volume comp
## Total Accepted Offer Level (MW)
#bid_comp = df[['FPN', 'MEL','CO Volume','Bid Volume']].merge(wb[['Total Accepted Bid Level (MW)', 'Total Accepted Undo Bid Volume (MW)']], how = 'outer', left_index = True, right_index = True)
#bid_comp['Delta'] = bid_comp['Total Accepted Bid Level (MW)'] + bid_comp['Total Accepted Undo Bid Volume (MW)'] - (bid_comp['Bid Volume'] * 2)
#

df.set_index('DateTime', inplace = True)
price_comp = df[['MEL', 'FPN','VWA Bid Price', 'VWA Offer Price','Bid Vol @ 3','Bid Vol @ 2','Bid Vol @ 1', 'Offer Vol @ 1', 'Offer Vol @ 2', 'Offer Vol @ 3', 'bid_price-3', 'bid_price-2', 'bid_price-1','offer_price-1', 'offer_price-2','offer_price-3', 'bl-3','bl-2','bl-1','ol-1','ol-2','ol-3']].merge(wb[['Accepted Offer Price (?/MWh)', 'Accepted Bid Price (?/MWh)', 'Total Accepted Undo Offer Volume (MW)', 'Total Accepted Undo Bid Volume (MW)']], how = 'outer', left_index = True, right_index = True)
price_comp['bid price error'] = price_comp['Accepted Bid Price (?/MWh)'] - price_comp['VWA Bid Price']
price_comp['offer price error'] = price_comp['Accepted Offer Price (?/MWh)'] - price_comp['VWA Offer Price']
price_comp = price_comp.replace([np.inf, -np.inf], np.nan)

import pandas as pd
import seaborn as sns
wb = pd.read_csv(r'S:\Power Trading\Ostap\BMRS\BMRS Python\imm_BM_HH_2016.csv')
wb['DateTime'] = pd.to_datetime(wb['Start Time (BST)'], format = '%d/%m/%Y %H:%M')
wb.set_index('DateTime', inplace = True)
wb.index = wb.index.tz_localize('Europe/London', ambiguous = 'infer').tz_convert('UTC').tz_localize(None)


df['fpn_delta'] = df['FPN'] - (df['FPN Volume'] * 2)
df_test = df[['FPN Unadjusted','MEL','FPN', 'FPN Volume','fpn_delta', 'CO Volume', 'Offer Volume','Bid Volume']]

df_test = df_test.merge(wb[['Pre-Balancing Dispatch (FPN) (MW)','Expected Metered Dispatch (MW)',
                            'Total Accepted Offer Level (MW)', 'Total Accepted Undo Offer Volume (MW)',
                            'Total Accepted Bid Level (MW)', 'Total Accepted Undo Bid Volume (MW)']]
                            , how = 'outer', left_index = True, right_index = True)
df_test['Bid Delta'] = df_test['Total Accepted Bid Level (MW)'] + df_test['Total Accepted Undo Bid Volume (MW)'] - (df_test['Bid Volume'] * 2)
df_test['Offer Delta'] = df_test['Total Accepted Offer Level (MW)'] + df_test['Total Accepted Undo Offer Volume (MW)'] - (df_test['Offer Volume'] * 2)

df_test[df_test['Pre-Balancing Dispatch (FPN) (MW)']>df_test['Expected Metered Dispatch (MW)'],'MEL Adjusted Bid Volume'] = df_test['FPN'] - df_test['Expected Metered Dispatch (MW)']

