'''
Created on Sep 13, 2022

@author: bsana
'''

import pandas as pd

xl_2019 = r'Q:\CMP\LOS Monitoring 2019\Counts\ADT\data2019.csv'

df_id = pd.read_excel(r'Q:\CMP\LOS Monitoring 2017\Iteris\Task C5\ADT\v4\Master CSV file\ADT Locations MetaData.xlsx', 'Sheet1')
df_id = df_id.rename(columns={'2015 Name ': 'Desc',})

df_19 = pd.read_csv(xl_2019)

def proc_df(df):
    df['Date'] = df['Date'].str.replace('.','/')
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.loc[pd.notnull(df['Vol']),].copy()
    df['Hour'] = df['Time'].str.slice(stop=2)
    df['ID'], dummy = df['ID'].str.split('_',1).str
    df['ID'] = df['ID'].astype(int)
    df = df[['ID','Date','Direction','Hour','Vol']].groupby(['ID','Date','Direction','Hour']).sum().reset_index()
    df['dow'] = df['Date'].dt.dayofweek
    # retain only weekdays
    df = df.loc[df['dow']<5,]
    df = df[['ID','Direction','Hour','Vol']].groupby(['ID','Direction','Hour']).mean().reset_index()
    return df

df_19 = proc_df(df_19)

df_19 = df_id[['ID','Desc']].merge(df_19)
df_19['ID'] = df_19['ID'].astype(int)
df_19.to_csv(r'Q:\CMP\LOS Monitoring 2019\Counts\ADT\adt_processed_2019_hourly.csv', header=True, index=False)
