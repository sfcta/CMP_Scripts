# Import necessary packages
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
import warnings
warnings.filterwarnings("ignore")
import sys

#Minimum sample size requirement
ss_threshold = 180 # from last cycle

# Input CMP segment shapefile
cmp_name = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\cmp_roadway_segments.shp'
cmp_segs=gp.read_file(cmp_name)

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}

# Get CMP and INRIX correspondence table
conflation = pd.read_csv(r'Q:\CMP\LOS Monitoring 2017\Iteris\Task C4\CMP_Segment_INRIX_TMC_Correspondence.csv')

conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
conf_len.columns = ['CMP_SegID', 'CMP_Length']

# Read in the INRIX data using dask to save memory
filename1 = r'Q:\Data\Observed\Streets\INRIX\INRIX-Data-CMP-2017-April-May\INRIX-Data-CMP-2017-April-May.csv'
df1 = dd.read_csv(filename1, assume_missing=True)
df_cmp = df1[df1['tmc_code'].isin(conflation['INRIX_SegID'])]
df_cmp = df_cmp[df_cmp['confidence_score']==30]

#Create date and time fields for subsequent filtering
# df_cmp['Date_Time'] = df_cmp['Date Time'].str[:16]
# df_cmp['Date_Time'] = df_cmp['Date_Time'].str.replace('T', " ")
df_cmp['Date_Time'] = df_cmp['measurement_tstamp']
df_cmp['Date'] = df_cmp['Date_Time'].str[:10]
df_cmp['Day']=dd.to_datetime(df_cmp['Date_Time'])
df_cmp['DOW']=df_cmp.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df_cmp['Hour']=df_cmp.Day.dt.hour
df_cmp['Minute']=df_cmp.Day.dt.minute

df_cmp = df_cmp[df_cmp['speed']>0]

#Get AM (7-9am) speeds on Tue, Wed, and Thu
df_am=df_cmp[((df_cmp['DOW']>=1) & (df_cmp['DOW']<=3)) & ((df_cmp['Hour']==7) | (df_cmp['Hour']==8))]
dd.to_csv(df_am[df_am['tmc_code'].isin(['105N04403','105P04403','105P04869','105-04868'])], r'Q:\CMP\LOS Monitoring 2019\Auto_LOS\am*.csv')

#Get PM (4:30-6:30pm) speeds on Tue, Wed, and Thu
df_pm=df_cmp[((df_cmp['DOW']>=1) & (df_cmp['DOW']<=3)) & (((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30)))]


# CMP segment level speeds
# Use merge to attach INRIX speeds to CMP segments
df_cmp_am = df_am.merge(conflation, left_on='tmc_code', right_on='INRIX_SegID', how='outer')
df_cmp_pm = df_pm.merge(conflation, left_on='tmc_code', right_on='INRIX_SegID', how='outer')

df_cmp_am['TT'] = df_cmp_am['Length_Matched']/df_cmp_am['speed']
df_cmp_pm['TT'] = df_cmp_pm['Length_Matched']/df_cmp_pm['speed']

# Get total travel time at a particular date_time on a CMP segment
cmp_am = df_cmp_am.groupby(['CMP_SegID', 'Date_Time']).agg({'Length_Matched': 'sum', 'TT': 'sum'}).reset_index().compute()
cmp_am = pd.merge(cmp_am, conf_len, on='CMP_SegID', how='left')
cmp_am['Speed'] = cmp_am['Length_Matched']/cmp_am['TT']
cmp_am['SpatialCov'] = 100*cmp_am['Length_Matched']/cmp_am['CMP_Length']  #Spatial coverage 

cmp_pm = df_cmp_pm.groupby(['CMP_SegID', 'Date_Time']).agg({'Length_Matched': 'sum', 'TT': 'sum'}).reset_index().compute()
cmp_pm = pd.merge(cmp_pm, conf_len, on='CMP_SegID', how='left')
cmp_pm['Speed'] = cmp_pm['Length_Matched']/cmp_pm['TT']
cmp_pm['SpatialCov'] = 100*cmp_pm['Length_Matched']/cmp_pm['CMP_Length']

# LOS function using 1985 HCM
def los_1985(cls, spd):
    if spd is None:
        return ' '
    else:
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

def los_2000(cls, spd):
    if spd is None:
        return ' '
    else:
        if cls == 'Fwy':  # Freeway
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

# 99% spatial coverage 
cmp_am_agg = cmp_am[cmp_am['SpatialCov']>=99].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_agg.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_agg['avg_speed'] = round(cmp_am_agg['Len']/cmp_am_agg['TT'],3)
cmp_am_agg = cmp_am_agg[cmp_am_agg['sample_size']>=ss_threshold]

# 95% spatial coverage 
cmp_am_pcnt95 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=95)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt95.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt95['avg_speed'] = round(cmp_am_pcnt95['Len']/cmp_am_pcnt95['TT'],3)
cmp_am_pcnt95 = cmp_am_pcnt95[cmp_am_pcnt95['sample_size']>=ss_threshold]
if len(cmp_am_pcnt95)>0:
    cmp_am_pcnt95['comment'] = 'Calculation performed on 95% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt95, ignore_index=True)

# 90% spatial coverage 
cmp_am_pcnt90 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=90)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt90.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt90['avg_speed'] = round(cmp_am_pcnt90['Len']/cmp_am_pcnt90['TT'],3)
cmp_am_pcnt90 = cmp_am_pcnt90[cmp_am_pcnt90['sample_size']>=ss_threshold]
if len(cmp_am_pcnt90)>0:
    cmp_am_pcnt90['comment'] = 'Calculation performed on 90% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt90, ignore_index=True)
    
# 85% spatial coverage 
cmp_am_pcnt85 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=85)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt85.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt85['avg_speed'] = round(cmp_am_pcnt85['Len']/cmp_am_pcnt85['TT'],3)
cmp_am_pcnt85 = cmp_am_pcnt85[cmp_am_pcnt85['sample_size']>=ss_threshold]
if len(cmp_am_pcnt85)>0:
    cmp_am_pcnt85['comment'] = 'Calculation performed on 85% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt85, ignore_index=True)

# 80% spatial coverage 
cmp_am_pcnt80 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=80)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt80.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt80['avg_speed'] = round(cmp_am_pcnt80['Len']/cmp_am_pcnt80['TT'],3)
cmp_am_pcnt80 = cmp_am_pcnt80[cmp_am_pcnt80['sample_size']>=ss_threshold]
if len(cmp_am_pcnt80)>0:
    cmp_am_pcnt80['comment'] = 'Calculation performed on 80% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt80, ignore_index=True)

# 75% spatial coverage 
cmp_am_pcnt75 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=75)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt75.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt75['avg_speed'] = round(cmp_am_pcnt75['Len']/cmp_am_pcnt75['TT'],3)
cmp_am_pcnt75 = cmp_am_pcnt75[cmp_am_pcnt75['sample_size']>=ss_threshold]
if len(cmp_am_pcnt75)>0:
    cmp_am_pcnt75['comment'] = 'Calculation performed on 75% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt75, ignore_index=True)
    
    
# 70% spatial coverage 
cmp_am_pcnt70 = cmp_am[(~cmp_am['CMP_SegID'].isin(cmp_am_agg['cmp_id'])) & (cmp_am['SpatialCov']>=70)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_am_pcnt70.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_am_pcnt70['avg_speed'] = round(cmp_am_pcnt70['Len']/cmp_am_pcnt70['TT'],3)
cmp_am_pcnt70 = cmp_am_pcnt70[cmp_am_pcnt70['sample_size']>=ss_threshold]
if len(cmp_am_pcnt70)>0:
    cmp_am_pcnt70['comment'] = 'Calculation performed on 70% or greater of length'
    cmp_am_agg = cmp_am_agg.append(cmp_am_pcnt70, ignore_index=True)
    
cmp_am_agg['year']=2017
cmp_am_agg['period']='AM'
cmp_am_agg['source']='INRIX'
    
# Assign LOS based on 1985 and 2000 HCM
cmp_am_agg = pd.merge(cmp_am_agg, cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], left_on='cmp_id', right_on='cmp_segid', how='left')
cmp_am_agg['los_hcm85'] = cmp_am_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
cmp_am_agg['los_hcm00'] = cmp_am_agg.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

# Floating car run segments
fcr_segs_am = cmp_segs[~cmp_segs['cmp_segid'].isin(cmp_am_agg['cmp_id'])].cmp_segid.tolist()
cmp_am_fcr = pd.DataFrame(fcr_segs_am)
cmp_am_fcr.columns=['cmp_id']
cmp_am_fcr['year']=2017
cmp_am_fcr['period']='AM'
cmp_am_fcr['source']='Floating Car'

# cmp_am_agg = cmp_am_agg.append(cmp_am_fcr, ignore_index=True)
cmp_am_agg = cmp_am_agg[['cmp_id', 'year', 'source', 'period', 'avg_speed', 'los_hcm85', 'los_hcm00', 'sample_size', 'comment']]


# 99% spatial coverage 
cmp_pm_agg = cmp_pm[cmp_pm['SpatialCov']>=99].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_agg.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_agg['avg_speed'] = round(cmp_pm_agg['Len']/cmp_pm_agg['TT'],3)
cmp_pm_agg = cmp_pm_agg[cmp_pm_agg['sample_size']>=ss_threshold]

# 95% spatial coverage 
cmp_pm_pcnt95 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=95)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt95.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt95['avg_speed'] = round(cmp_pm_pcnt95['Len']/cmp_pm_pcnt95['TT'],3)
cmp_pm_pcnt95 = cmp_pm_pcnt95[cmp_pm_pcnt95['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt95)>0:
    cmp_pm_pcnt95['comment'] = 'Calculation performed on 95% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt95, ignore_index=True)

# 90% spatial coverage 
cmp_pm_pcnt90 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=90)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt90.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt90['avg_speed'] = round(cmp_pm_pcnt90['Len']/cmp_pm_pcnt90['TT'],3)
cmp_pm_pcnt90 = cmp_pm_pcnt90[cmp_pm_pcnt90['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt90)>0:
    cmp_pm_pcnt90['comment'] = 'Calculation performed on 90% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt90, ignore_index=True)

# 85% spatial coverage 
cmp_pm_pcnt85 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=85)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt85.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt85['avg_speed'] = round(cmp_pm_pcnt85['Len']/cmp_pm_pcnt85['TT'],3)
cmp_pm_pcnt85 = cmp_pm_pcnt85[cmp_pm_pcnt85['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt85)>0:
    cmp_pm_pcnt85['comment'] = 'Calculation performed on 85% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt85, ignore_index=True)

# 80% spatial coverage 
cmp_pm_pcnt80 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=80)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt80.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt80['avg_speed'] = round(cmp_pm_pcnt80['Len']/cmp_pm_pcnt80['TT'],3)
cmp_pm_pcnt80 = cmp_pm_pcnt80[cmp_pm_pcnt80['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt80)>0:
    cmp_pm_pcnt80['comment'] = 'Calculation performed on 80% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt80, ignore_index=True)

# 75% spatial coverage 
cmp_pm_pcnt75 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=75)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt75.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt75['avg_speed'] = round(cmp_pm_pcnt75['Len']/cmp_pm_pcnt75['TT'],3)
cmp_pm_pcnt75 = cmp_pm_pcnt75[cmp_pm_pcnt75['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt75)>0:
    cmp_pm_pcnt75['comment'] = 'Calculation performed on 75% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt75, ignore_index=True)

# 70% spatial coverage 
cmp_pm_pcnt70 = cmp_pm[(~cmp_pm['CMP_SegID'].isin(cmp_pm_agg['cmp_id'])) & (cmp_pm['SpatialCov']>=70)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                           'Length_Matched': 'sum'}).reset_index()
cmp_pm_pcnt70.columns = ['cmp_id', 'sample_size', 'TT', 'Len']
cmp_pm_pcnt70['avg_speed'] = round(cmp_pm_pcnt70['Len']/cmp_pm_pcnt70['TT'],3)
cmp_pm_pcnt70 = cmp_pm_pcnt70[cmp_pm_pcnt70['sample_size']>=ss_threshold]
if len(cmp_pm_pcnt70)>0:
    cmp_pm_pcnt70['comment'] = 'Calculation performed on 70% or greater of length'
    cmp_pm_agg = cmp_pm_agg.append(cmp_pm_pcnt70, ignore_index=True)

cmp_pm_agg['year']=2017
cmp_pm_agg['period']='PM'
cmp_pm_agg['source']='INRIX'

cmp_pm_agg = pd.merge(cmp_pm_agg, cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], left_on='cmp_id', right_on='cmp_segid', how='left')
cmp_pm_agg['los_hcm85'] = cmp_pm_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
cmp_pm_agg['los_hcm00'] = cmp_pm_agg.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

# Floating car run segments
fcr_segs_pm = cmp_segs[~cmp_segs['cmp_segid'].isin(cmp_pm_agg['cmp_id'])].cmp_segid.tolist()
cmp_pm_fcr = pd.DataFrame(fcr_segs_pm)
cmp_pm_fcr.columns=['cmp_id']
cmp_pm_fcr['year']=2017
cmp_pm_fcr['period']='PM'
cmp_pm_fcr['source']='Floating Car'

# cmp_pm_agg = cmp_pm_agg.append(cmp_pm_fcr, ignore_index=True)
cmp_pm_agg = cmp_pm_agg[['cmp_id', 'year', 'source', 'period', 'avg_speed', 'los_hcm85', 'los_hcm00', 'sample_size', 'comment']]

cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
cmp_segs_los.to_csv(r'Q:\CMP\LOS Monitoring 2019\Auto_LOS\CMP 2017 Auto LOS v2.csv', index=False)
