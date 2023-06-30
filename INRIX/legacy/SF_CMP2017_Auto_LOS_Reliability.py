# Import necessary packages
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
import os
import warnings
warnings.filterwarnings("ignore")

# Need to change to SFCTA Paths

NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD map 19_02'
CORR_FILE = 'CMP_Segment_INRIX_XD1902_Links_Correspondence.csv'

DATA_DIR = r'Q:\Data\Observed\Streets\INRIX\v1902'

OUT_DIR = r'Q:\CMP\LOS Monitoring 2021\Auto_LOS'
OUT_FILE = 'CMP2017_Auto_Speeds_Reliability.csv'
INPUT_PATHS = [['SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_', 6]]

# Minimum sample size for the AM/PM monitoring period
ss_threshold = 180

# Minimum spatial coverage for reliability measurement
r_spatial_thrd= 70  # to be consistent with the minimum value used in LOS calculation

# Input CMP segment shapefile
cmp_segs=gp.read_file(os.path.join(r'Q:\CMP\LOS Monitoring 2021\CMP_plus_shp\old_cmp_plus', 'cmp_segments_plus.shp'))

# Get CMP and INRIX correspondence table
conflation = pd.read_csv(os.path.join(NETCONF_DIR, CORR_FILE))
conflation[['CMP_SegID','INRIX_SegID']] = conflation[['CMP_SegID','INRIX_SegID']].astype(int)
conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
conf_len.columns = ['CMP_SegID', 'CMP_Length']

# Read in the INRIX data using dask to save memory
df_cmp = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1,p[1]+1):
        df1 = dd.read_csv(os.path.join(DATA_DIR, '%s%s\data.csv' %(p[0],i)), assume_missing=True)
        df1['Segment ID'] = df1['Segment ID'].astype('int')
        df1 = df1[df1['Segment ID'].isin(conflation['INRIX_SegID'])]
        df_cmp = dd.concat([df_cmp,df1],axis=0,interleave_partitions=True)

# Calculate reference speed for CMP segments
inrix_link_refspd = df_cmp.drop_duplicates(subset=['Segment ID', 'Ref Speed(miles/hour)']).compute()
cmp_link_refspd = pd.merge(conflation, inrix_link_refspd[['Segment ID', 'Ref Speed(miles/hour)']], left_on='INRIX_SegID', right_on='Segment ID', how='left')
cmp_link_refspd = cmp_link_refspd[pd.notnull(cmp_link_refspd['Ref Speed(miles/hour)'])]
cmp_link_refspd['RefTT'] = cmp_link_refspd['Length_Matched']/cmp_link_refspd['Ref Speed(miles/hour)']

cmp_segs_refspd= cmp_link_refspd.groupby(['CMP_SegID']).agg({'Length_Matched': 'sum',
                                                                  'RefTT': 'sum'}).reset_index()
cmp_segs_refspd['refspd_inrix'] = cmp_segs_refspd['Length_Matched']/cmp_segs_refspd['RefTT']
cmp_segs_refspd['cmp_segid'] = cmp_segs_refspd['CMP_SegID']

# Calculate average speeds, los, and reliability metrics
df_cmp['Segment ID'] = df_cmp['Segment ID'].astype('int')
#Create date and time fields for subsequent filtering
df_cmp['Date_Time'] = df_cmp['Date Time'].str[:16]
df_cmp['Date_Time'] = df_cmp['Date_Time'].str.replace('T', " ")
df_cmp['Date'] = df_cmp['Date_Time'].str[:10]
df_cmp['Day']=dd.to_datetime(df_cmp['Date_Time'])
df_cmp['DOW']=df_cmp.Day.dt.dayofweek #Tue-1, Wed-2, Thu-3
df_cmp['Hour']=df_cmp.Day.dt.hour
df_cmp['Minute']=df_cmp.Day.dt.minute

#Remove weekends
df_cmp = df_cmp[(df_cmp['DOW']>0) & (df_cmp['DOW']<4)]
df_cmp = df_cmp[df_cmp['Speed(miles/hour)']>0]

#Get AM (7-9am)
df_am=df_cmp[(df_cmp['Hour']==7) | (df_cmp['Hour']==8)]

#Get PM (4:30-6:30pm)
df_pm=df_cmp[((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30))]

# CMP segment level speeds
# Use merge to attach INRIX speeds to CMP segments
df_cmp_am = df_am.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')
df_cmp_pm = df_pm.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')

# Define percentile funtion to get 5th and 20th percentile speed
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_
    
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
def spatial_coverage(df, cmp_period_agg, group_cols, rename_cols, coverage_threshold):
    cmp_tt_agg = df[(~df['CMP_SegID'].isin(cmp_period_agg['cmp_segid'])) & 
                    (df['SpatialCov']>=coverage_threshold)].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                                     'Length_Matched': 'sum',
                                                                                     'Speed': ['std']}).reset_index()
    cmp_tt_agg.columns = rename_cols
    cmp_tt_agg['avg_speed'] = round(cmp_tt_agg['Len']/cmp_tt_agg['TT'],3)
    cmp_tt_agg['cov'] = round(100*cmp_tt_agg['std_speed']/cmp_tt_agg['avg_speed'],3)
    cmp_tt_agg = cmp_tt_agg[cmp_tt_agg['sample_size_los']>=ss_threshold]
    
    if len(cmp_tt_agg)>0:
        cmp_tt_agg['comment'] = 'LOS calculation performed on ' + str(coverage_threshold) +'% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_tt_agg, ignore_index=True)
        
    return cmp_period_agg


# Calculate CMP segment level average speeds, LOS, and reliability metrics
def cmp_seg_level_speed_and_los(df_cmp_period, ss_threshold, r_spatial_thrd, cur_year, cur_period):
    df_cmp_period['TT'] = df_cmp_period['Length_Matched']/df_cmp_period['Speed(miles/hour)']

    # Get total travel time at a particular date_time on a CMP segment
    cmp_period = df_cmp_period.groupby(['CMP_SegID', 'Date','Date_Time']).agg({'Length_Matched': 'sum',
                                                               'TT': 'sum'}).reset_index().compute()

    cmp_period = pd.merge(cmp_period, conf_len, on='CMP_SegID', how='left')
    cmp_period['Speed'] = cmp_period['Length_Matched']/cmp_period['TT']
    cmp_period['SpatialCov'] = 100*cmp_period['Length_Matched']/cmp_period['CMP_Length']  #Spatial coverage 
    
    group_cols = ['CMP_SegID']
    rename_cols = ['cmp_segid', 'sample_size_los', 'TT', 'Len', 'std_speed']
    cmp_period_agg = pd.DataFrame(columns = rename_cols)
    
    # Use minimum 70% spatial coverage for reliability metric calculation
    cmp_period_r = cmp_period[cmp_period['SpatialCov']>=r_spatial_thrd].groupby(group_cols).agg({'Speed': ['count', percentile(5), percentile(20), percentile(50)]}).reset_index()
    cmp_period_r.columns = ['cmp_segid', 'sample_size_rel', 'pcnt5th', 'pcnt20th', 'pcnt50th']
    cmp_period_r = cmp_period_r[cmp_period_r['sample_size_rel']>=ss_threshold]
    
    # Calculate avg and std of speeds based on different spatial coverage requirements
    # 99% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 99)
    
    # 95% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 95)

    # 90% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 90)

    # 85% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 85)

    # 80% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 80)

    # 75% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 75)

    # 70% spatial coverage 
    cmp_period_agg = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, coverage_threshold = 70)

    cmp_period_agg['year'] = cur_year
    cmp_period_agg['period'] = cur_period
    cmp_period_agg['source']='INRIX'
    
    cmp_period_combine = pd.merge(cmp_period_agg, cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], on='cmp_segid', how='left')
    cmp_period_combine['los_hcm85'] = cmp_period_combine.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
    cmp_period_combine['los_hcm00'] = cmp_period_combine.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

    cmp_period_combine = cmp_period_combine.merge(cmp_period_r, on='cmp_segid', how='left')
    cmp_period_combine = cmp_period_combine[['cmp_segid', 'year', 'source', 'period', 'avg_speed', 'los_hcm85', 'los_hcm00', 'sample_size_los', 'comment', 'std_speed', 'cov', 'pcnt5th', 'pcnt20th', 'pcnt50th', 'sample_size_rel']]
    
    return cmp_period_combine
    

cmp_am_agg = cmp_seg_level_speed_and_los(df_cmp_am, ss_threshold, r_spatial_thrd, cur_year = 2017, cur_period = 'AM')
cmp_pm_agg = cmp_seg_level_speed_and_los(df_cmp_pm, ss_threshold, r_spatial_thrd, cur_year = 2017, cur_period = 'PM')

cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
cmp_segs_los = pd.merge(cmp_segs_los, cmp_segs_refspd[['cmp_segid', 'refspd_inrix']], on='cmp_segid', how='left')
cmp_segs_los['tti95'] = np.maximum(1, round(cmp_segs_los['refspd_inrix']/cmp_segs_los['pcnt5th'], 3))
cmp_segs_los['tti80'] = np.maximum(1, round(cmp_segs_los['refspd_inrix']/cmp_segs_los['pcnt20th'], 3))
cmp_segs_los['bi'] = np.maximum(0, round(cmp_segs_los['avg_speed']/cmp_segs_los['pcnt5th']-1, 3))
cmp_segs_los['lottr'] = np.maximum(1, round(cmp_segs_los['pcnt50th']/cmp_segs_los['pcnt20th'], 3))

cmp_segs_los.to_csv(os.path.join(OUT_DIR, OUT_FILE), index=False)
