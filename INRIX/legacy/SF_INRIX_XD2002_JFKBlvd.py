'''
Created on Jan 21, 2021

@author: bsana
'''

import pandas as pd
import geopandas as gp
import dask.dataframe as dd
import os
import warnings
warnings.filterwarnings("ignore")
from os.path import join

DATA_DIR = r'Q:\Data\Observed\Streets\INRIX\v2002'

OUT_DIR = r'Q:\CMP\LOS Monitoring 2020\GGPark_Study'
OUT_FILE = 'GGPark_AutoSpeeds.csv'
INPUT_PATHS = [['All_SF_2019-02-01_to_2019-06-01_60_min_part_', 3],
               ['All_SF_2019-06-01_to_2019-09-01_60_min_part_', 2]]

req_seg = pd.read_excel(join(OUT_DIR, 'FultonLincoln_Segments.xlsx'), 'Sheet1')

XD_SHP_DIR = r'Q:\GIS\Transportation\Roads\INRIX\XD\20_02\shapefiles\SF'
xd_file = 'Inrix_XD_2002_SF.shp'
xd_segs = gp.read_file(join(XD_SHP_DIR, xd_file))
xd_segs = xd_segs[['XDSegID', 'FRC', 'RoadName', 'Miles', 'Bearing']]
xd_segs['XDSegID'] = xd_segs['XDSegID'].astype(int)
xd_segs['Miles'] = xd_segs['Miles'].astype(float)
xd_segs = xd_segs[xd_segs['XDSegID'].isin(req_seg['INRIX_SegID'])]
print(xd_segs['RoadName'].value_counts())
xd_segs = xd_segs.rename(columns={'XDSegID': 'Segment ID'})

# Read in the INRIX data using dask to save memory
df_cmp = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1,p[1]):
        df1 = dd.read_csv(join(DATA_DIR, '%s%s\data.csv' %(p[0],i)), assume_missing=True)
        if len(df1)>0:
            df1['Segment ID'] = df1['Segment ID'].astype('int')
            df1 = df1[df1['Segment ID'].isin(req_seg['INRIX_SegID'])]
            df_cmp = dd.concat([df_cmp,df1],axis=0,interleave_partitions=True)

df_cmp['Segment ID'] = df_cmp['Segment ID'].astype('int')
#Create date and time fields for subsequent filtering
df_cmp['Date_Time'] = df_cmp['Date Time'].str[:16]
df_cmp['Date_Time'] = df_cmp['Date_Time'].str.replace('T', " ")
df_cmp['Date'] = df_cmp['Date_Time'].str[:10]
df_cmp['Day']=dd.to_datetime(df_cmp['Date_Time'])
df_cmp['dayofweek']=df_cmp.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df_cmp['Hour']=df_cmp.Day.dt.hour
df_cmp['Minute']=df_cmp.Day.dt.minute
df_cmp['Month']=df_cmp.Day.dt.month
df_cmp['Year']=df_cmp.Day.dt.year

# remove mon and fri
df_cmp = df_cmp[~df_cmp['dayofweek'].isin([0,4])]
# set day of week
df_cmp['dow'] = '1_weekday'
df_cmp['dow'] = df_cmp['dow'].mask(df_cmp['dayofweek']==5, '2_saturday')
df_cmp['dow'] = df_cmp['dow'].mask(df_cmp['dayofweek']==6, '3_sunday')

df_cmp = df_cmp[df_cmp['Speed(miles/hour)']>0]

# set time period
df_cmp['timep'] = ''
df_cmp['timep'] = df_cmp['timep'].mask((df_cmp['Hour']>=8) & (df_cmp['Hour']<13), '1_8a_1p')
df_cmp['timep'] = df_cmp['timep'].mask((df_cmp['Hour']>=13) & (df_cmp['Hour']<15), '2_1p_3p')
df_cmp['timep'] = df_cmp['timep'].mask((df_cmp['Hour']>=15) & (df_cmp['Hour']<20), '3_3p_8p')
df_cmp = df_cmp[df_cmp['timep']!='']

req_seg.columns = ['Segment ID', 'street']
df_cmp = df_cmp.merge(req_seg, how='left')
df_cmp = df_cmp.merge(xd_segs[['Segment ID', 'Miles']], how='left')

df_cmp['ttime'] = df_cmp['Miles']/df_cmp['Speed(miles/hour)']
group_cols = ['street','dow','timep','Month','Year']
cmp_agg = df_cmp.groupby(group_cols).agg({'ttime': ['count', 'sum'], 'Miles': 'sum'}).reset_index().compute()
cmp_agg.columns = ['Street', 'DOW', 'Time_period', 'Month', 'Year', 'Sample_size', 'Travel_time', 'Miles']
cmp_agg['Avg_speed'] = cmp_agg['Miles']/cmp_agg['Travel_time']
cmp_agg['Miles'] = cmp_agg['Miles']/cmp_agg['Sample_size']
cmp_agg[['Avg_speed', 'Travel_time', 'Miles']] = cmp_agg[['Avg_speed', 'Travel_time', 'Miles']].round(1)

cmp_agg.to_csv(join(OUT_DIR, OUT_FILE), index=False)

