'''
Created on Nov 7, 2019

@author: bsana
'''

import pandas as pd
import numpy as np

all_counts = pd.read_csv(r'Q:\CMP\LOS Monitoring 2019\MTA_BikeCounts\Automatic\HOURLYTABLE.csv')
all_counts.columns = ['location','day','hour','month','year','count']
all_counts['count'] = all_counts['count'].str.replace(',','')
all_counts['count'] = all_counts['count'].astype(float)

all_counts['period'] = 'Other'
all_counts.loc[all_counts['hour'].isin([7,8]), 'period'] = 'AM'
all_counts.loc[all_counts['hour'].isin([17,18]), 'period'] = 'PM'

# this is to get common counter locations across years
def get_common_df(df):
    yr_list = np.sort(df['year'].unique())
    first_year = True
    for yr in yr_list:
        if first_year:
            common_df = df.loc[df['year']==yr,['location','period']]
            first_year = False
        else:
            common_df = common_df.merge(df.loc[df['year']==yr,['location','period']])
    return common_df

df = all_counts[(all_counts['day']=='WEEKDAY') & (all_counts['month']=='April')]
common_df = get_common_df(df.drop_duplicates(['location','year','period']))
# print(common_df)

def agg_df(df):
    df = df[['location','period','year','month','count']].groupby(['location','period','year','month']).sum().reset_index()
    df = df.groupby(['location','period','year']).mean().reset_index()
    return df
df = agg_df(common_df.merge(df))
df.to_csv(r'Q:\CMP\LOS Monitoring 2019\MTA_BikeCounts\Automatic\processed_automatic_counts.csv', index=False)

df = all_counts[all_counts['day']=='WEEKDAY']
common_df = get_common_df(df.drop_duplicates(['location','year','period']))
df = agg_df(common_df.merge(df))
print(df.groupby(['period','year']).sum().reset_index())



