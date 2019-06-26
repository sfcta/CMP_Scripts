# Import necessary packages
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
import warnings
warnings.filterwarnings("ignore")

# Input CMP segment shapefile
cmp_name = 'S:/CMP/Network Conflation/cmp_roadway_segments.shp'
cmp_segs=gp.read_file(cmp_name)

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}

# Get CMP and INRIX correspondence table
conflation = pd.read_csv('Z:/SF_CMP/INRIX Data/NetworkConflation_CCL/CMP_Segment_INRIX_Links_Correspondence.csv')

conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
conf_len.columns = ['CMP_SegID', 'CMP_Length']

# Read in the INRIX data using dask to save memory
filename1 = 'S:/CMP/Auto LOS/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_1/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_1/data.csv'
df1 = dd.read_csv(filename1, assume_missing=True)
df1['Segment ID'] = df1['Segment ID'].astype('int')
df1_cmp = df1[df1['Segment ID'].isin(conflation['INRIX_SegID'])]

filename2 = 'S:/CMP/Auto LOS/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_2/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_2/data.csv'
df2 = dd.read_csv(filename2, assume_missing=True)
df2['Segment ID'] = df2['Segment ID'].astype('int')
df2_cmp = df2[df2['Segment ID'].isin(conflation['INRIX_SegID'])]

filename3 = 'S:/CMP/Auto LOS/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_3/SF_CMP__2019-04-02_to_2019-05-15_1_min_part_3/data.csv'
df3 = dd.read_csv(filename3, assume_missing=True)
df3['Segment ID'] = df3['Segment ID'].astype('int')
df3_cmp = df3[df3['Segment ID'].isin(conflation['INRIX_SegID'])]

#Combine three data parts
df_cmp = dd.concat([df1_cmp,df2_cmp],axis=0,interleave_partitions=True)
df_cmp = dd.concat([df_cmp,df3_cmp],axis=0,interleave_partitions=True)

#Create date and time fields for subsequent filtering
df_cmp['Date_Time'] = df_cmp['Date Time'].str[:16]
df_cmp['Date_Time'] = df_cmp['Date_Time'].str.replace('T', " ")
df_cmp['Date'] = df_cmp['Date_Time'].str[:10]
df_cmp['Day']=dd.to_datetime(df_cmp['Date_Time'])
df_cmp['DOW']=df_cmp.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df_cmp['Hour']=df_cmp.Day.dt.hour
df_cmp['Minute']=df_cmp.Day.dt.minute

#Get AM (7-9am) speeds on Tue, Wed, and Thu
df_am=df_cmp[((df_cmp['DOW']>=1) & (df_cmp['DOW']<=3)) & ((df_cmp['Hour']==7) | (df_cmp['Hour']==8))]

#Get PM (4:30-6:30pm) speeds on Tue, Wed, and Thu
df_pm=df_cmp[((df_cmp['DOW']>=1) & (df_cmp['DOW']<=3)) & (((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30)))]


# CMP segment level speeds
# Use merge to attach INRIX speeds to CMP segments
df_cmp_am = df_am.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')
df_cmp_pm = df_pm.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')

df_cmp_am['TT'] = df_cmp_am['Length_Matched']/df_cmp_am['Speed(miles/hour)']
df_cmp_pm['TT'] = df_cmp_pm['Length_Matched']/df_cmp_pm['Speed(miles/hour)']

# Get total travel time at a particular date_time on a CMP segment
cmp_am = df_cmp_am.groupby(['CMP_SegID', 'Date_Time']).agg({'Length_Matched': 'sum', 'TT': 'sum'}).reset_index().compute()
cmp_am = pd.merge(cmp_am, conf_len, on='CMP_SegID', how='left')
cmp_am['Speed'] = cmp_am['Length_Matched']/cmp_am['TT']
cmp_am['SpatialCov'] = 100*cmp_am['Length_Matched']/cmp_am['CMP_Length']  #Spatial coverage 

# Calculate AM average speed and sample size with 99% spatial coverage requirement
cmp_am_pcnt99 = cmp_am[cmp_am['SpatialCov']>=99].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt99.columns = ['CMP_SegID', 'AM_SS_P99', 'AM_TT_P99', 'AM_Len_P99']
cmp_am_pcnt99['AM_Spd_P99'] = round(cmp_am_pcnt99['AM_Len_P99']/cmp_am_pcnt99['AM_TT_P99'],3)

# Calculate AM average speed and sample size with 70% spatial coverage requirement
cmp_am_pcnt70 = cmp_am[cmp_am['SpatialCov']>=70].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt70.columns = ['CMP_SegID', 'AM_SS_P70', 'AM_TT_P70', 'AM_Len_P70']
cmp_am_pcnt70['AM_Spd_P70'] = round(cmp_am_pcnt70['AM_Len_P70']/cmp_am_pcnt70['AM_TT_P70'],3)


cmp_pm = df_cmp_pm.groupby(['CMP_SegID', 'Date_Time']).agg({'Length_Matched': 'sum', 'TT': 'sum'}).reset_index().compute()
cmp_pm = pd.merge(cmp_pm, conf_len, on='CMP_SegID', how='left')
cmp_pm['Speed'] = cmp_pm['Length_Matched']/cmp_pm['TT']
cmp_pm['SpatialCov'] = 100*cmp_pm['Length_Matched']/cmp_pm['CMP_Length']

# Calculate PM average speed and sample size with 99% spatial coverage requirement
cmp_pm_pcnt99 = cmp_pm[cmp_pm['SpatialCov']>=99].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt99.columns = ['CMP_SegID', 'PM_SS_P99', 'PM_TT_P99', 'PM_Len_P99']
cmp_pm_pcnt99['PM_Spd_P99'] = round(cmp_pm_pcnt99['PM_Len_P99']/cmp_pm_pcnt99['PM_TT_P99'],3)

# Calculate PM average speed and sample size with 70% spatial coverage requirement
cmp_pm_pcnt70 = cmp_pm[cmp_pm['SpatialCov']>=70].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt70.columns = ['CMP_SegID', 'PM_SS_P70', 'PM_TT_P70', 'PM_Len_P70']
cmp_pm_pcnt70['PM_Spd_P70'] = round(cmp_pm_pcnt70['PM_Len_P70']/cmp_pm_pcnt70['PM_TT_P70'],3)

# Combine calculation results into one file
cmp_segs = cmp_segs.merge(cmp_am_pcnt99, left_on='cmp_segid', right_on='CMP_SegID', how='left')
cmp_segs = cmp_segs.merge(cmp_am_pcnt70, left_on='cmp_segid', right_on='CMP_SegID', how='left')
cmp_segs = cmp_segs.merge(cmp_pm_pcnt99, left_on='cmp_segid', right_on='CMP_SegID', how='left')
cmp_segs = cmp_segs.merge(cmp_pm_pcnt70, left_on='cmp_segid', right_on='CMP_SegID', how='left')

# Decide whether to relax spatial requirement based on minimum sample size of 180 from last CMP cycle
cmp_segs['AM_Speed']=np.where(cmp_segs['AM_SS_P99']<180, 
                              np.where(cmp_segs['AM_SS_P70']<180, None,
                                       cmp_segs['AM_Spd_P70']),
                              cmp_segs['AM_Spd_P99'])
cmp_segs['AM_Samples']=np.where(cmp_segs['AM_SS_P99']<180, cmp_segs['AM_SS_P70'], cmp_segs['AM_SS_P99'])
cmp_segs['AM_Spatial']=np.where(cmp_segs['AM_SS_P99']<180, '70%', '99%')

cmp_segs['PM_Speed']=np.where(cmp_segs['PM_SS_P99']<180, 
                              np.where(cmp_segs['PM_SS_P70']<180, None,
                                       cmp_segs['PM_Spd_P70']),
                              cmp_segs['PM_Spd_P99'])
cmp_segs['PM_Samples']=np.where(cmp_segs['PM_SS_P99']<180, cmp_segs['PM_SS_P70'], cmp_segs['PM_SS_P99'])
cmp_segs['PM_Spatial']=np.where(cmp_segs['PM_SS_P99']<180, '70%', '99%')


# LOS function using 1985 HCM
def los_1985(cls, spd):
    if cls == 'Fwy':   # Freeway
        if spd >=60:
            return 'A'
        elif spd >=55:
            return 'B'
        elif spd >=49:
            return 'C'     
        elif spd >=41:
            return 'D'
        elif spd >=30:
            return 'E'
        elif (spd>0) and (spd<30):
            return 'F'
    elif cls =='1':  # Arterial Class I
        if spd >=35:
            return 'A'
        elif spd >=28:
            return 'B'
        elif spd >=22:
            return 'C'     
        elif spd >=17:
            return 'D'
        elif spd >=13:
            return 'E'
        elif (spd>0) and (spd<13):
            return 'F'
    elif cls =='2':  # Arterial Class II
        if spd >=30:
            return 'A'
        elif spd >=24:
            return 'B'
        elif spd >=18:
            return 'C'     
        elif spd >=14:
            return 'D'
        elif spd >=10:
            return 'E'
        elif (spd>0) and (spd<10):
            return 'F'
    elif cls =='3':  # Arterial Class III
        if spd >=25:
            return 'A'
        elif spd >=19:
            return 'B'
        elif spd >=13:
            return 'C'     
        elif spd >=9:
            return 'D'
        elif spd >=7:
            return 'E'
        elif (spd>0) and (spd<7):
            return 'F'

# LOS function using 2000 HCM
def los_2000(cls, spd):
    if cls == 'Fwy':  # Freeway, no HCM2000 LOS correspondence 
        return ' '
    elif cls =='1':  # Arterial Class I
        if spd > 42:
            return 'A'
        elif spd > 34:
            return 'B'
        elif spd > 27:
            return 'C'     
        elif spd > 21:
            return 'D'
        elif spd > 16:
            return 'E'
        elif (spd>0) and (spd<=16):
            return 'F'
    if cls =='2':   # Arterial Class II
        if spd > 35:
            return 'A'
        elif spd > 28:
            return 'B'
        elif spd > 22:
            return 'C'     
        elif spd > 17:
            return 'D'
        elif spd > 13:
            return 'E'
        elif (spd>0) and (spd<=13):
            return 'F'
    elif cls =='3':  # Arterial Class III
        if spd > 30:
            return 'A'
        elif spd > 24:
            return 'B'
        elif spd > 18:
            return 'C'     
        elif spd > 14:
            return 'D'
        elif spd > 10:
            return 'E'
        elif (spd>0) and (spd<=10):
            return 'F'
    elif cls =='4':  # Arterial Class IV
        if spd > 25:
            return 'A'
        elif spd > 19:
            return 'B'
        elif spd > 13:
            return 'C'     
        elif spd > 9:
            return 'D'
        elif spd > 7:
            return 'E'
        elif (spd>0) and (spd<=7):
            return 'F'

# Assign LOS based on 1985 and 2000 HCM
cmp_segs['AM_LOS_85'] = cmp_segs.apply(lambda x: los_1985(x.cls_hcm85, x.AM_Speed), axis=1)
cmp_segs['AM_LOS_00'] = cmp_segs.apply(lambda x: los_2000(x.cls_hcm00, x.AM_Speed), axis=1)
cmp_segs['PM_LOS_85'] = cmp_segs.apply(lambda x: los_1985(x.cls_hcm85, x.PM_Speed), axis=1)
cmp_segs['PM_LOS_00'] = cmp_segs.apply(lambda x: los_2000(x.cls_hcm00, x.PM_Speed), axis=1)

# Save the results
cmp_segs = cmp_segs.to_crs(wgs84)
cmp_segs.to_file('S:/CMP/Auto LOS/cmp_roadway_segments_auto_los.shp')
