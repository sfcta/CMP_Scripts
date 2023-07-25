# Import necessary packages
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
client = dask.distributed.Client()

#Read in the INRIX data using dask
filename1 = 'Z:/SF_CMP/Data/SF_all_rds_2019-02-11_to_2019-03-24_1_min_part_1/data.csv'
df1 = dd.read_csv(filename1, assume_missing=True)

filename2 = 'Z:/SF_CMP/Data/SF_all_rds_2019-02-11_to_2019-03-24_1_min_part_2/data.csv'
df2 = dd.read_csv(filename2, assume_missing=True)

filename3 = 'Z:/SF_CMP/Data/SF_all_rds_2019-02-11_to_2019-03-24_1_min_part_3/data.csv'
df3 = dd.read_csv(filename3, assume_missing=True)

print(('Part 1 record #: %s, Part 2 record #: %s, Part 3 record #: %s') % (len(df1), len(df2), len(df3)))


#Combine three data parts
df = dd.concat([df1,df2],axis=0,interleave_partitions=True)
df = dd.concat([df,df3],axis=0,interleave_partitions=True)
print('Combined record #: ', len(df))

#Create date and time fields for subsequent filtering
df['Date_Time'] = df['Date Time'].str[:16]
df['Date_Time'] = df['Date_Time'].str.replace('T', " ")
df['Date'] = df['Date_Time'].str[:10]
df['Day']=dd.to_datetime(df['Date_Time'])
df['DOW']=df.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df['Hour']=df.Day.dt.hour
df['Minute']=df.Day.dt.minute

#Get AM (7-9am) speeds on Tue, Wed, and Thu
df_am=df[((df['DOW']>=1) & (df['DOW']<=3)) & ((df['Hour']==7) | (df['Hour']==8))]
print('Number of records in AM: ', len(df_am))

#Get PM (4:30-6:30pm) speeds on Tue, Wed, and Thu
df_pm=df[((df['DOW']>=1) & (df['DOW']<=3)) & (((df['Hour']==16) & (df['Minute']>=30)) | (df['Hour']==17) | ((df['Hour']==18) & (df['Minute']<30)))]
print('Number of records in PM: ', len(df_pm))

#Calculate AM sample size for each XD link
df_am_sum=df_am.groupby('Segment ID').Date_Time.agg(['count']).compute()
df_am_sum=df_am_sum.reset_index()
df_am_sum.columns = ['Segment ID','Count_AM']

#Calculate PM sample size for each XD link
df_pm_sum=df_pm.groupby('Segment ID').Date_Time.agg(['count']).compute()
df_pm_sum=df_pm_sum.reset_index()
df_pm_sum.columns = ['Segment ID','Count_PM']

num_days=18 #six weeks
df_am_sum['Pcnt_AM']=round(100*df_am_sum['Count_AM']/num_days/2/60,3)
df_pm_sum['Pcnt_PM']=round(100*df_pm_sum['Count_PM']/num_days/2/60,3)
print('Number of links with speeds in AM: ', len(df_am_sum))
print('Number of links with speeds in PM: ', len(df_pm_sum))

#Read in INRIX XD network shapefile
shp_path='Z:/SF_CMP/Data/inrixXD_SF/inrixXD_SF.shp'
sf_shp = gp.read_file(shp_path)
print('Number of links in SF: ', len(sf_shp))

#Attach the sample size measures to the shapefile
df_am_sum['Segment ID']=df_am_sum['Segment ID'].astype(np.int64)
df_pm_sum['Segment ID']=df_pm_sum['Segment ID'].astype(np.int64)
sf_shp['XDSegID']=sf_shp['XDSegID'].astype(np.int64)
df_samplesize=pd.merge(sf_shp, df_am_sum, left_on='XDSegID', right_on='Segment ID', how='left') #AM
df_samplesize=pd.merge(df_samplesize, df_pm_sum, left_on='XDSegID', right_on='Segment ID', how='left') #PM

#Create adequacy indicators for 10%, 30%, and 50% thresholds
df_samplesize['ADQT_AM_10']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_AM']>10 else 'No', axis=1) #10% threshold for AM
df_samplesize['ADQT_AM_30']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_AM']>30 else 'No', axis=1) #30% threshold for AM
df_samplesize['ADQT_AM_50']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_AM']>50 else 'No', axis=1) #50% threshold for AM

df_samplesize['ADQT_PM_10']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_PM']>10 else 'No', axis=1) #10% threshold for PM
df_samplesize['ADQT_PM_30']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_PM']>30 else 'No', axis=1) #30% threshold for PM
df_samplesize['ADQT_PM_50']=df_samplesize.apply(lambda x: 'Yes' if x['Pcnt_PM']>50 else 'No', axis=1) #50% threshold for PM

#Output the combined shapefile
df_samplesize.to_file('Z:/SF_CMP/Data/SF_INRIX_Sample_Size_SixWeeks.shp')