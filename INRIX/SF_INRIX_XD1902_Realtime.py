# Import necessary packages
import pandas as pd
import geopandas as gp
import dask.dataframe as dd
import os
import warnings
warnings.filterwarnings("ignore")

# SFCTA Paths
# NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD map 19_02'
# CORR_FILE = 'CMP_Segment_INRIX_XD1902_Links_Correspondence.csv'
NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD_20_01'
CORR_FILE = 'CMP_Segment_INRIX_Links_Correspondence_2001_Manual.csv'

DATA_DIR = r'Q:\Data\Observed\Streets\INRIX'

OUT_DIR = r'Q:\CMP\LOS Monitoring 2019\Auto_LOS'
# OUT_FILE = 'Mar2020_AutoSpeeds.csv'
# INPUT_PATHS = [['All_SF_2020-03-02_to_2020-03-20_1_min_part_', 7],
#                ['All_SF_2020-03-20_to_2020-03-28_1_min_part_', 4]]
OUT_FILE = 'Apr2020_AutoSpeeds.csv'
INPUT_PATHS = [['All_SF_2020-03-30_to_2020-04-12_1_min_part_', 6],
               ['All_SF_2020-04-12_to_2020-04-20_1_min_part_', 4]]

# Minimum sample size per day per peak period
ss_threshold = 10

# Input CMP segment shapefile
cmp_segs=gp.read_file(os.path.join(NETCONF_DIR, 'cmp_roadway_segments.shp'))

# Get CMP and INRIX correspondence table
conflation = pd.read_csv(os.path.join(NETCONF_DIR, CORR_FILE))
conflation[['CMP_SegID','INRIX_SegID']] = conflation[['CMP_SegID','INRIX_SegID']].astype(int)
conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
conf_len.columns = ['CMP_SegID', 'CMP_Length']

# Read in the INRIX data using dask to save memory
df_cmp = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1,p[1]):
        df1 = dd.read_csv(os.path.join(DATA_DIR, '%s%s\data.csv' %(p[0],i)), assume_missing=True)
        if len(df1)>0:
            df1['Segment ID'] = df1['Segment ID'].astype('int')
            df1 = df1[df1['Segment ID'].isin(conflation['INRIX_SegID'])]
            df_cmp = dd.concat([df_cmp,df1],axis=0,interleave_partitions=True)

#Create date and time fields for subsequent filtering
df_cmp['Date_Time'] = df_cmp['Date Time'].str[:16]
df_cmp['Date_Time'] = df_cmp['Date_Time'].str.replace('T', " ")
df_cmp['Date'] = df_cmp['Date_Time'].str[:10]
df_cmp['Day']=dd.to_datetime(df_cmp['Date_Time'])
df_cmp['DOW']=df_cmp.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df_cmp['Hour']=df_cmp.Day.dt.hour
df_cmp['Minute']=df_cmp.Day.dt.minute

#Remove weekends
df_cmp = df_cmp[df_cmp['DOW']<5]
df_cmp = df_cmp[df_cmp['Speed(miles/hour)']>0]

#Get AM (7-9am)
df_am=df_cmp[(df_cmp['Hour']==7) | (df_cmp['Hour']==8)]

#Get PM (4:30-6:30pm)
df_pm=df_cmp[((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30))]

# CMP segment level speeds
# Use merge to attach INRIX speeds to CMP segments
df_cmp_am = df_am.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')
df_cmp_pm = df_pm.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')

# Define processing functions
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
            
# LOS function using 2000 HCM
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

# Calculate CMP segment level average speeds and LOS
def cmp_seg_level_speed_and_los(df_cmp_period, cmp_segs, conf_len, ss_threshold, cur_year, cur_period):
    df_cmp_period['TT'] = df_cmp_period['Length_Matched']/df_cmp_period['Speed(miles/hour)']

    # Get total travel time at a particular date_time on a CMP segment
    cmp_period = df_cmp_period.groupby(['CMP_SegID', 'Date','Date_Time']).agg({'Length_Matched': 'sum',
                                                               'TT': 'sum'}).reset_index().compute()

    cmp_period = pd.merge(cmp_period, conf_len, on='CMP_SegID', how='left')
    cmp_period['Speed'] = cmp_period['Length_Matched']/cmp_period['TT']
    cmp_period['SpatialCov'] = 100*cmp_period['Length_Matched']/cmp_period['CMP_Length']  #Spatial coverage 
    
    group_cols = ['CMP_SegID','Date']
    rename_cols = ['cmp_id', 'date', 'sample_size', 'TT', 'Len']
    # 99% spatial coverage 
    cmp_period_agg = cmp_period[cmp_period['SpatialCov']>=99].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_agg.columns = rename_cols
    cmp_period_agg['avg_speed'] = round(cmp_period_agg['Len']/cmp_period_agg['TT'],3)
    cmp_period_agg = cmp_period_agg[cmp_period_agg['sample_size']>=ss_threshold]

    # 95% spatial coverage 
    cmp_period_pcnt95 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=95)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt95.columns = rename_cols
    cmp_period_pcnt95['avg_speed'] = round(cmp_period_pcnt95['Len']/cmp_period_pcnt95['TT'],3)
    cmp_period_pcnt95 = cmp_period_pcnt95[cmp_period_pcnt95['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt95)>0:
        cmp_period_pcnt95['comment'] = 'Calculation performed on 95% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt95, ignore_index=True)

    # 90% spatial coverage 
    cmp_period_pcnt90 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=90)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt90.columns = rename_cols
    cmp_period_pcnt90['avg_speed'] = round(cmp_period_pcnt90['Len']/cmp_period_pcnt90['TT'],3)
    cmp_period_pcnt90 = cmp_period_pcnt90[cmp_period_pcnt90['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt90)>0:
        cmp_period_pcnt90['comment'] = 'Calculation performed on 90% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt90, ignore_index=True)

    # 85% spatial coverage 
    cmp_period_pcnt85 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=85)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt85.columns = rename_cols
    cmp_period_pcnt85['avg_speed'] = round(cmp_period_pcnt85['Len']/cmp_period_pcnt85['TT'],3)
    cmp_period_pcnt85 = cmp_period_pcnt85[cmp_period_pcnt85['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt85)>0:
        cmp_period_pcnt85['comment'] = 'Calculation performed on 85% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt85, ignore_index=True)

    # 80% spatial coverage 
    cmp_period_pcnt80 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=80)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt80.columns = rename_cols
    cmp_period_pcnt80['avg_speed'] = round(cmp_period_pcnt80['Len']/cmp_period_pcnt80['TT'],3)
    cmp_period_pcnt80 = cmp_period_pcnt80[cmp_period_pcnt80['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt80)>0:
        cmp_period_pcnt80['comment'] = 'Calculation performed on 80% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt80, ignore_index=True)

    # 75% spatial coverage 
    cmp_period_pcnt75 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=75)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt75.columns = rename_cols
    cmp_period_pcnt75['avg_speed'] = round(cmp_period_pcnt75['Len']/cmp_period_pcnt75['TT'],3)
    cmp_period_pcnt75 = cmp_period_pcnt75[cmp_period_pcnt75['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt75)>0:
        cmp_period_pcnt75['comment'] = 'Calculation performed on 75% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt75, ignore_index=True)

    # 70% spatial coverage 
    cmp_period_pcnt70 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=70)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum'}).reset_index()
    cmp_period_pcnt70.columns = rename_cols
    cmp_period_pcnt70['avg_speed'] = round(cmp_period_pcnt70['Len']/cmp_period_pcnt70['TT'],3)
    cmp_period_pcnt70 = cmp_period_pcnt70[cmp_period_pcnt70['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt70)>0:
        cmp_period_pcnt70['comment'] = 'Calculation performed on 70% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt70, ignore_index=True)

    cmp_period_agg['year'] = cur_year
    cmp_period_agg['period'] = cur_period
    cmp_period_agg['source']='INRIX'

    cmp_period_agg = pd.merge(cmp_period_agg, cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], left_on='cmp_id', right_on='cmp_segid', how='left')
    cmp_period_agg['los_hcm85'] = cmp_period_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
    cmp_period_agg['los_hcm00'] = cmp_period_agg.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

    cmp_period_agg = cmp_period_agg[['cmp_id', 'year', 'date', 'source', 'period', 'avg_speed', 'los_hcm85', 'los_hcm00', 'sample_size', 'comment']]
    
    return cmp_period_agg

cmp_am_agg = cmp_seg_level_speed_and_los(df_cmp_am, cmp_segs, conf_len, ss_threshold, cur_year = 2020, cur_period = 'AM')
cmp_pm_agg = cmp_seg_level_speed_and_los(df_cmp_pm, cmp_segs, conf_len, ss_threshold, cur_year = 2020, cur_period = 'PM')

cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
cmp_segs_los.to_csv(os.path.join(OUT_DIR, OUT_FILE), index=False)
