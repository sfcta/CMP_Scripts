
# Import necessary packages
import os
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
import warnings
warnings.filterwarnings("ignore")

# UK Paths
MAIN_DIR = r'S:\CMP\Auto LOS\XD map 19_02'
XD_SPEED = r'S:\CMP\Auto LOS\XD speed 19_02'

# SFCTA Paths
#MAIN_DIR = r'Q:\CMP\LOS Monitoring 2019\'
#XD_SPEED = r'Q:\CMP\LOS Monitoring 2019\'


# # Read in input files
# Input CMP segment shapefile
cmp_segs=gp.read_file(os.path.join(MAIN_DIR, 'cmp_roadway_segments.shp'))

# Get CMP and INRIX correspondence table
conflation = pd.read_csv(os.path.join(MAIN_DIR, 'CMP_Segment_INRIX_XD1902_Links_Correspondence.csv'))

conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
conf_len.columns = ['CMP_SegID', 'CMP_Length']

# # Define processing functions
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


def cmp_seg_level_speed_and_los(df_cmp_period, cmp_segs, conf_len, ss_threshold, cur_year, cur_period):
    df_cmp_period['TT'] = df_cmp_period['Length_Matched']/df_cmp_period['Speed(miles/hour)']

    # Get total travel time at a particular date_time on a CMP segment
    cmp_period = df_cmp_period.groupby(['CMP_SegID', 'Date_Time']).agg({'Length_Matched': 'sum',
                                                               'TT': 'sum'}).reset_index().compute()

    cmp_period = pd.merge(cmp_period, conf_len, on='CMP_SegID', how='left')
    cmp_period['Speed'] = cmp_period['Length_Matched']/cmp_period['TT']
    cmp_period['SpatialCov'] = 100*cmp_period['Length_Matched']/cmp_period['CMP_Length']  #Spatial coverage 

    # 99% spatial coverage 
    cmp_period_agg = cmp_period[cmp_period['SpatialCov']>=99].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                                       'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_agg.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_agg['avg_speed'] = round(cmp_period_agg['Len']/cmp_period_agg['TT'],3)
    cmp_period_agg['cv'] = round(100*cmp_period_agg['std']/cmp_period_agg['avg_speed'],3)
    cmp_period_agg = cmp_period_agg[cmp_period_agg['sample_size']>=ss_threshold]

    # 95% spatial coverage 
    cmp_period_pcnt95 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=95)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt95.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt95['avg_speed'] = round(cmp_period_pcnt95['Len']/cmp_period_pcnt95['TT'],3)
    cmp_period_pcnt95['cv'] = round(100*cmp_period_pcnt95['std']/cmp_period_pcnt95['avg_speed'],3)
    cmp_period_pcnt95 = cmp_period_pcnt95[cmp_period_pcnt95['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt95)>0:
        cmp_period_pcnt95['comment'] = 'Calculation performed on 95% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt95, ignore_index=True)

    # 90% spatial coverage 
    cmp_period_pcnt90 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=90)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt90.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt90['avg_speed'] = round(cmp_period_pcnt90['Len']/cmp_period_pcnt90['TT'],3)
    cmp_period_pcnt90['cv'] = round(100*cmp_period_pcnt90['std']/cmp_period_pcnt90['avg_speed'],3)
    cmp_period_pcnt90 = cmp_period_pcnt90[cmp_period_pcnt90['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt90)>0:
        cmp_period_pcnt90['comment'] = 'Calculation performed on 90% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt90, ignore_index=True)

    # 85% spatial coverage 
    cmp_period_pcnt85 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=85)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt85.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt85['avg_speed'] = round(cmp_period_pcnt85['Len']/cmp_period_pcnt85['TT'],3)
    cmp_period_pcnt85['cv'] = round(100*cmp_period_pcnt85['std']/cmp_period_pcnt85['avg_speed'],3)
    cmp_period_pcnt85 = cmp_period_pcnt85[cmp_period_pcnt85['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt85)>0:
        cmp_period_pcnt85['comment'] = 'Calculation performed on 85% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt85, ignore_index=True)

    # 80% spatial coverage 
    cmp_period_pcnt80 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=80)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt80.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt80['avg_speed'] = round(cmp_period_pcnt80['Len']/cmp_period_pcnt80['TT'],3)
    cmp_period_pcnt80['cv'] = round(100*cmp_period_pcnt80['std']/cmp_period_pcnt80['avg_speed'],3)
    cmp_period_pcnt80 = cmp_period_pcnt80[cmp_period_pcnt80['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt80)>0:
        cmp_period_pcnt80['comment'] = 'Calculation performed on 80% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt80, ignore_index=True)

    # 75% spatial coverage 
    cmp_period_pcnt75 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=75)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt75.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt75['avg_speed'] = round(cmp_period_pcnt75['Len']/cmp_period_pcnt75['TT'],3)
    cmp_period_pcnt75['cv'] = round(100*cmp_period_pcnt75['std']/cmp_period_pcnt75['avg_speed'],3)
    cmp_period_pcnt75 = cmp_period_pcnt75[cmp_period_pcnt75['sample_size']>=ss_threshold]
    if len(cmp_period_pcnt75)>0:
        cmp_period_pcnt75['comment'] = 'Calculation performed on 75% or greater of length'
        cmp_period_agg = cmp_period_agg.append(cmp_period_pcnt75, ignore_index=True)

    # 70% spatial coverage 
    cmp_period_pcnt70 = cmp_period[(~cmp_period['CMP_SegID'].isin(cmp_period_agg['cmp_id'])) & (cmp_period['SpatialCov']>=70)].groupby('CMP_SegID').agg({'TT': ['count', 'sum'],
                                                                               'Length_Matched': 'sum',
                                                                                       'Speed': 'std'}).reset_index()
    cmp_period_pcnt70.columns = ['cmp_id', 'sample_size', 'TT', 'Len', 'std']
    cmp_period_pcnt70['avg_speed'] = round(cmp_period_pcnt70['Len']/cmp_period_pcnt70['TT'],3)
    cmp_period_pcnt70['cv'] = round(100*cmp_period_pcnt70['std']/cmp_period_pcnt70['avg_speed'],3)
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

    # Floating car run segments
    fcr_segs_period = cmp_segs[~cmp_segs['cmp_segid'].isin(cmp_period_agg['cmp_id'])].cmp_segid.tolist()
    if len(fcr_segs_period)>0:
        cmp_period_fcr = pd.DataFrame(fcr_segs_period)
        cmp_period_fcr.columns=['cmp_id']
        cmp_period_fcr['year'] = cur_year
        cmp_period_fcr['period'] = cur_period
        cmp_period_fcr['source']='Floating Car'
        cmp_period_agg = cmp_period_agg.append(cmp_period_fcr, ignore_index=True)
    cmp_period_agg = cmp_period_agg[['cmp_id', 'year', 'source', 'period', 'avg_speed', 'std', 'cv', 'los_hcm85', 'los_hcm00', 'sample_size', 'comment']]
    
    return cmp_period_agg


def get_am_pm_speed_los(df_cmp, conflation, cmp_segs, conf_len, ss_threshold, cur_year):
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

    # Use merge to attach INRIX speeds to CMP segments
    df_cmp_am = df_am.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')
    df_cmp_am['Speed(miles/hour)'] = df_cmp_am['Speed(miles/hour)'].astype(float)
    df_cmp_pm = df_pm.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')
    df_cmp_pm['Speed(miles/hour)'] = df_cmp_pm['Speed(miles/hour)'].astype(float)

    cmp_am_agg = cmp_seg_level_speed_and_los(df_cmp_am, cmp_segs, conf_len, ss_threshold, cur_year, cur_period = 'AM')

    cmp_pm_agg = cmp_seg_level_speed_and_los(df_cmp_pm, cmp_segs, conf_len, ss_threshold, cur_year, cur_period = 'PM')

    cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
    
    return cmp_segs_los


# # Read in speed files

# ## 2017 Spring
# Read in the INRIX data using dask to save memory
s17_file1 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_1\data.csv')
s17_df1 = dd.read_csv(s17_file1, assume_missing=True)
s17_df1['Segment ID'] = s17_df1['Segment ID'].astype('int')
s17_df1_cmp = s17_df1[s17_df1['Segment ID'].isin(conflation['INRIX_SegID'])]

s17_file2 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_2\data.csv')
s17_df2 = dd.read_csv(s17_file2, assume_missing=True)
s17_df2['Segment ID'] = s17_df2['Segment ID'].astype('int')
s17_df2_cmp = s17_df2[s17_df2['Segment ID'].isin(conflation['INRIX_SegID'])]

s17_file3 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_3\data.csv')
s17_df3 = dd.read_csv(s17_file3, assume_missing=True)
s17_df3['Segment ID'] = s17_df3['Segment ID'].astype('int')
s17_df3_cmp = s17_df3[s17_df3['Segment ID'].isin(conflation['INRIX_SegID'])]

s17_file4 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_4\data.csv')
s17_df4 = dd.read_csv(s17_file4, assume_missing=True)
s17_df4['Segment ID'] = s17_df4['Segment ID'].astype('int')
s17_df4_cmp = s17_df4[s17_df4['Segment ID'].isin(conflation['INRIX_SegID'])]

s17_file5 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_5\data.csv')
s17_df5 = dd.read_csv(s17_file5, assume_missing=True)
s17_df5['Segment ID'] = s17_df5['Segment ID'].astype('int')
s17_df5_cmp = s17_df5[s17_df5['Segment ID'].isin(conflation['INRIX_SegID'])]

s17_file6 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-04-01_to_2017-05-17_1_min_part_6\data.csv')
s17_df6 = dd.read_csv(s17_file6, assume_missing=True)
s17_df6['Segment ID'] = s17_df6['Segment ID'].astype('int')
s17_df6_cmp = s17_df6[s17_df6['Segment ID'].isin(conflation['INRIX_SegID'])]

#Combine three data parts
df_cmp_2017_spring = dd.concat([s17_df1_cmp,s17_df2_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_spring = dd.concat([df_cmp_2017_spring, s17_df3_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_spring = dd.concat([df_cmp_2017_spring, s17_df4_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_spring = dd.concat([df_cmp_2017_spring, s17_df5_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_spring = dd.concat([df_cmp_2017_spring, s17_df6_cmp],axis=0,interleave_partitions=True)


# ## 2017 Fall 
# Read in the INRIX data using dask to save memory
f17_file1 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_1\data.csv')
f17_df1 = dd.read_csv(f17_file1, assume_missing=True)
f17_df1['Segment ID'] = f17_df1['Segment ID'].astype('int')
f17_df1_cmp = f17_df1[f17_df1['Segment ID'].isin(conflation['INRIX_SegID'])]

f17_file2 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_2\data.csv')
f17_df2 = dd.read_csv(f17_file2, assume_missing=True)
f17_df2['Segment ID'] = f17_df2['Segment ID'].astype('int')
f17_df2_cmp = f17_df2[f17_df2['Segment ID'].isin(conflation['INRIX_SegID'])]

f17_file3 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_3\data.csv')
f17_df3 = dd.read_csv(f17_file3, assume_missing=True)
f17_df3['Segment ID'] = f17_df3['Segment ID'].astype('int')
f17_df3_cmp = f17_df3[f17_df3['Segment ID'].isin(conflation['INRIX_SegID'])]

f17_file4 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_4\data.csv')
f17_df4 = dd.read_csv(f17_file4, assume_missing=True)
f17_df4['Segment ID'] = f17_df4['Segment ID'].astype('int')
f17_df4_cmp = f17_df4[f17_df4['Segment ID'].isin(conflation['INRIX_SegID'])]

f17_file5 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_5\data.csv')
f17_df5 = dd.read_csv(f17_file5, assume_missing=True)
f17_df5['Segment ID'] = f17_df5['Segment ID'].astype('int')
f17_df5_cmp = f17_df5[f17_df5['Segment ID'].isin(conflation['INRIX_SegID'])]

f17_file6 = os.path.join(XD_SPEED, 'SF_All_v1902_2017-09-01_to_2017-10-17_1_min_part_6\data.csv')
f17_df6 = dd.read_csv(f17_file6, assume_missing=True)
f17_df6['Segment ID'] = f17_df6['Segment ID'].astype('int')
f17_df6_cmp = f17_df6[f17_df6['Segment ID'].isin(conflation['INRIX_SegID'])]

#Combine three data parts
df_cmp_2017_fall = dd.concat([f17_df1_cmp,f17_df2_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_fall = dd.concat([df_cmp_2017_fall, f17_df3_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_fall = dd.concat([df_cmp_2017_fall, f17_df4_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_fall = dd.concat([df_cmp_2017_fall, f17_df5_cmp],axis=0,interleave_partitions=True)
df_cmp_2017_fall = dd.concat([df_cmp_2017_fall, f17_df6_cmp],axis=0,interleave_partitions=True)


# ## 2019 Spring 
# Read in the INRIX data using dask to save memory
s19_file1 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_1/data.csv'
s19_df1 = dd.read_csv(s19_file1, assume_missing=True)
s19_df1['Segment ID'] = s19_df1['Segment ID'].astype('int')
s19_df1_cmp = s19_df1[s19_df1['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file2 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_2/data.csv'
s19_df2 = dd.read_csv(s19_file2, assume_missing=True)
s19_df2['Segment ID'] = s19_df2['Segment ID'].astype('int')
s19_df2_cmp = s19_df2[s19_df2['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file3 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_3/data.csv'
s19_df3 = dd.read_csv(s19_file3, assume_missing=True)
s19_df3['Segment ID'] = s19_df3['Segment ID'].astype('int')
s19_df3_cmp = s19_df3[s19_df3['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file4 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_4/data.csv'
s19_df4 = dd.read_csv(s19_file4, assume_missing=True)
s19_df4['Segment ID'] = s19_df4['Segment ID'].astype('int')
s19_df4_cmp = s19_df4[s19_df4['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file5 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_5/data.csv'
s19_df5 = dd.read_csv(s19_file5, assume_missing=True)
s19_df5['Segment ID'] = s19_df5['Segment ID'].astype('int')
s19_df5_cmp = s19_df5[s19_df5['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file6 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_6/data.csv'
s19_df6 = dd.read_csv(s19_file6, assume_missing=True)
s19_df6['Segment ID'] = s19_df6['Segment ID'].astype('int')
s19_df6_cmp = s19_df6[s19_df6['Segment ID'].isin(conflation['INRIX_SegID'])]

s19_file7 = 'S:/CMP/Auto LOS/XD speed 19_02/SF_All_v1902_2019-04-01_to_2019-05-17_1_min_part_7/data.csv'
s19_df7 = dd.read_csv(s19_file7, assume_missing=True)
s19_df7['Segment ID'] = s19_df7['Segment ID'].astype('int')
s19_df7_cmp = s19_df7[s19_df7['Segment ID'].isin(conflation['INRIX_SegID'])]

#Combine three data parts
df_cmp_2019_spring = dd.concat([s19_df1_cmp,s19_df2_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_spring = dd.concat([df_cmp_2019_spring,s19_df3_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_spring = dd.concat([df_cmp_2019_spring,s19_df4_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_spring = dd.concat([df_cmp_2019_spring,s19_df5_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_spring = dd.concat([df_cmp_2019_spring,s19_df6_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_spring = dd.concat([df_cmp_2019_spring,s19_df7_cmp],axis=0,interleave_partitions=True)


# ## 2019 Fall 
# Read in the INRIX data using dask to save memory
f19_file1 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_1\data.csv')
f19_df1 = dd.read_csv(f19_file1, assume_missing=True)
f19_df1['Segment ID'] = f19_df1['Segment ID'].astype('int')
f19_df1_cmp = f19_df1[f19_df1['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file2 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_2\data.csv')
f19_df2 = dd.read_csv(f19_file2, assume_missing=True)
f19_df2['Segment ID'] = f19_df2['Segment ID'].astype('int')
f19_df2_cmp = f19_df2[f19_df2['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file3 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_3\data.csv')
f19_df3 = dd.read_csv(f19_file3, assume_missing=True)
f19_df3['Segment ID'] = f19_df3['Segment ID'].astype('int')
f19_df3_cmp = f19_df3[f19_df3['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file4 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_4\data.csv')
f19_df4 = dd.read_csv(f19_file4, assume_missing=True)
f19_df4['Segment ID'] = f19_df4['Segment ID'].astype('int')
f19_df4_cmp = f19_df4[f19_df4['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file5 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_5\data.csv')
f19_df5 = dd.read_csv(f19_file5, assume_missing=True)
f19_df5['Segment ID'] = f19_df5['Segment ID'].astype('int')
f19_df5_cmp = f19_df5[f19_df5['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file6 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_6\data.csv')
f19_df6 = dd.read_csv(f19_file6, assume_missing=True)
f19_df6['Segment ID'] = f19_df6['Segment ID'].astype('int')
f19_df6_cmp = f19_df6[f19_df6['Segment ID'].isin(conflation['INRIX_SegID'])]

f19_file7 = os.path.join(XD_SPEED, 'SF_All_v1902_2019-09-01_to_2019-10-17_1_min_part_7\data.csv')
f19_df7 = dd.read_csv(f19_file7, assume_missing=True)
f19_df7['Segment ID'] = f19_df7['Segment ID'].astype('int')
f19_df7_cmp = f19_df7[f19_df7['Segment ID'].isin(conflation['INRIX_SegID'])]

#Combine three data parts
df_cmp_2019_fall = dd.concat([f19_df1_cmp, f19_df2_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_fall = dd.concat([df_cmp_2019_fall, f19_df3_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_fall = dd.concat([df_cmp_2019_fall, f19_df4_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_fall = dd.concat([df_cmp_2019_fall, f19_df5_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_fall = dd.concat([df_cmp_2019_fall, f19_df6_cmp],axis=0,interleave_partitions=True)
df_cmp_2019_fall = dd.concat([df_cmp_2019_fall, f19_df7_cmp],axis=0,interleave_partitions=True)


# # Calculate average speed and LOS

ss_threshold = 180 #Minimum sample size requirement used in last cycle

cmp_segs_los_2017_spring =  get_am_pm_speed_los(df_cmp_2017_spring, conflation, cmp_segs, conf_len, ss_threshold, cur_year = 2017)

cmp_segs_los_2017_fall =  get_am_pm_speed_los(df_cmp_2017_fall, conflation, cmp_segs, conf_len, ss_threshold, cur_year = 2017)

cmp_segs_los_2019_spring =  get_am_pm_speed_los(df_cmp_2019_spring, conflation, cmp_segs, conf_len, ss_threshold, cur_year = 2019)

cmp_segs_los_2019_fall =  get_am_pm_speed_los(df_cmp_2019_fall, conflation, cmp_segs, conf_len, ss_threshold, cur_year = 2019)

cmp_segs_los_2017_spring.to_csv(os.path.join(MAIN_DIR, 'CMP 2017 Auto XD Reliability v1.csv'), index=False)

cmp_segs_los_2019_spring.to_csv(os.path.join(MAIN_DIR, 'CMP 2019 Auto XD Reliability v1.csv'), index=False)