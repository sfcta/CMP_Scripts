
# Use this script to process and summarize multi-year ADT counts

import pandas as pd

xl_2015 = r'Q:\CMP\LOS Monitoring 2017\Iteris\Task C5\ADT\v4\Master CSV file\ADT2015 V4.xlsx'
xl_2017 = r'Q:\CMP\LOS Monitoring 2017\Iteris\Task C5\ADT\v4\Master CSV file\ADT2017 V4.xlsx'
xl_2019 = r'Q:\CMP\LOS Monitoring 2019\Counts\ADT\data2019.csv'
xl_2021 = r'Q:\CMP\LOS Monitoring 2021\Counts\ADT\data2021.csv'

df_id = pd.read_excel(r'Q:\CMP\LOS Monitoring 2017\Iteris\Task C5\ADT\v4\Master CSV file\ADT Locations MetaData.xlsx', 'Sheet1')
df_id = df_id.rename(columns={'2015 Name ': 'Desc',})

df_15 = pd.read_excel(xl_2015, 'Sheet1')
df_17 = pd.read_excel(xl_2017, 'Sheet1')
df_19 = pd.read_csv(xl_2019)
df_21 = pd.read_csv(xl_2021)

AM_vals = ['0700-0715','0715-0730','0730-0745','0745-0800','0800-0815','0815-0830','0830-0845','0845-0900']
PM_vals = ['1630-1645','1645-1700','1700-1715','1715-1730','1730-1745','1745-1800','1800-1815','1815-1830']

def proc_df(df):
    df['Date'] = df['Date'].str.replace('.','/')
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.loc[pd.notnull(df['Vol']),].copy()
    df['Period'] = 'other'
    df.loc[df['Time'].isin(AM_vals), 'Period'] = 'AM'
    df.loc[df['Time'].isin(PM_vals), 'Period'] = 'PM'
    df['ID'], dummy = df['ID'].str.split('_',1).str
    df['ID'] = df['ID'].astype(int)
    df = df[['ID','Date','Direction','Period','Vol']].groupby(['ID','Date','Direction','Period']).sum().reset_index()
    df['dow'] = df['Date'].dt.dayofweek
    # retain only weekdays
    df = df.loc[df['dow']<5,]
    df = df[['ID','Direction','Period','Vol']].groupby(['ID','Direction','Period']).mean().reset_index()
    return df

df_15 = proc_df(df_15)
df_15 = df_15.rename(columns={'Vol': 'vol_15',})
df_17 = proc_df(df_17)
df_17 = df_17.rename(columns={'Vol': 'vol_17',})
df_19 = proc_df(df_19)
df_19 = df_19.rename(columns={'Vol': 'vol_19',})
df_21 = proc_df(df_21)
df_21 = df_21.rename(columns={'Vol': 'vol_21',})

df_all = df_15.merge(df_17)
df_all = df_all.merge(df_19)
df_all = df_all.merge(df_21)
df_all = df_id[['ID','Desc']].merge(df_all)
df_all['ID'] = df_all['ID'].astype(int)
df_all = df_all.sort_values(['ID','Direction','Period'])
df_all.to_csv(r'Q:\CMP\LOS Monitoring 2021\Counts\ADT\adt_processed.csv', header=True, index=False)

df_21 = df_id[['ID','Desc']].merge(df_21)
df_21['ID'] = df_21['ID'].astype(int)
df_21.to_csv(r'Q:\CMP\LOS Monitoring 2021\Counts\ADT\adt_processed_2021.csv', header=True, index=False)