# Import necessary packages
import pandas as pd
import geopandas as gp
import dask.dataframe as dd
import os, sys
import warnings
warnings.filterwarnings("ignore")

# SFCTA Paths
# NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD map 19_02'
# CORR_FILE = 'CMP_Segment_INRIX_XD1902_Links_Correspondence.csv'
NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD_20_01'
CORR_FILE = 'CMP_Segment_INRIX_Links_Correspondence_2001_Manual.csv'

FWY_NAMES = ['BAY BRG','GOLDEN GATE BRG','101 / US-101 S','101 / US-101 N','80 / I-80 E','80 / I-80 W',
             '280 / I-280 N','280 / I-280 S','CENTRAL FWY'
    ]

CLS1_NAMES = ['19TH AVE','BROADWAY','BROADWAY TUNNEL','GUERRERO ST','JUNIPERO SERRA BLVD','SLOAT BLVD'
    ]

DATA_DIR = r'Q:\Data\Observed\Streets\INRIX'

OUT_DIR = r'Q:\CMP\LOS Monitoring 2020\Auto_LOS'
# OUT_FILE = 'Mar2020_AutoSpeeds.csv'
# INPUT_PATHS = [['All_SF_2020-03-02_to_2020-03-20_1_min_part_', 7],
#                ['All_SF_2020-03-20_to_2020-03-28_1_min_part_', 4]]

OUT_FILE = 'Apr2020_allXD_1.csv'
INPUT_PATHS = [['All_SF_2020-03-30_to_2020-04-12_1_min_part_', 6],
               ['All_SF_2020-04-12_to_2020-04-20_1_min_part_', 4]]

# OUT_FILE = 'Apr2020_allXD_2.csv'
# INPUT_PATHS = [['All_SF_2020-04-20_to_2020-05-16_1_min_part_', 11]]

# OUT_FILE = 'May2020_allXD_1.csv'
# INPUT_PATHS = [['All_SF_2020-05-16_to_2020-05-24_1_min_part_', 4]]

# OUT_FILE = 'May2020_allXD_2.csv'
# INPUT_PATHS = [['All_SF_2020-05-24_to_2020-06-07_1_min_part_', 7]]

# Minimum sample size per day per peak period
ss_threshold = 10

# Input CMP segment shapefile
cmp_segs = gp.read_file(os.path.join(NETCONF_DIR, 'cmp_roadway_segments.shp'))
cmp_segs = cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']]

# Get CMP and INRIX correspondence table
conflation = pd.read_csv(os.path.join(NETCONF_DIR, CORR_FILE))
conflation = conflation[['CMP_SegID','INRIX_SegID']].astype(int) 
conflation.columns = ['cmp_segid', 'seg_id']
conflation = conflation.merge(cmp_segs, how='left', on='cmp_segid')

# Test conflation for duplicate road type records
test_df = conflation[['seg_id','cls_hcm85']].drop_duplicates()
test_df = test_df.groupby(['seg_id']).count().reset_index()
test_df = test_df.loc[test_df['cls_hcm85']>1,]
if len(test_df)>0:
    print('FATAL ERROR: CMP-INRIX correspondence has XD segments associated with multiple road types.')
    print(test_df)
    sys.exit(-1)

# Get FRC from XD shapefile
xd_segs = gp.read_file(os.path.join(r'Q:\GIS\Transportation\Roads\INRIX\XD\20_01\SF', 'Inrix_XD_2001_SF.shp'))
xd_segs = xd_segs[['XDSegID','FRC']]
xd_segs.columns = ['seg_id', 'frc']
xd_segs = xd_segs.astype(int)

# Read in the INRIX data using dask to save memory
df_cmp = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1,p[1]):
        df1 = dd.read_csv(os.path.join(DATA_DIR, '%s%s\data.csv' %(p[0],i)), assume_missing=True)
        meta_df = dd.read_csv(os.path.join(DATA_DIR, '%s%s\metadata.csv' %(p[0],i)))
        if len(df1)>0:
            df1['Segment ID'] = df1['Segment ID'].astype('int')
            meta_df['Segment ID'] = meta_df['Segment ID'].astype('int')
            df1 = df1.merge(meta_df[['Segment ID','Road','Intersection','Segment Length(Miles)']], on='Segment ID', how='left')
            df_cmp = dd.concat([df_cmp,df1],axis=0,interleave_partitions=True)

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
df_cmp = df_cmp[df_cmp['DOW']<5]
df_cmp = df_cmp[df_cmp['Speed(miles/hour)']>0]

#Get AM (7-9am)
df_am=df_cmp[(df_cmp['Hour']==7) | (df_cmp['Hour']==8)]

#Get PM (4:30-6:30pm)
df_pm=df_cmp[((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30))]


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
def cmp_seg_level_speed_and_los(df_cmp_period, cur_year, cur_period):
    df_cmp_period['TT'] = df_cmp_period['Segment Length(Miles)']/df_cmp_period['Speed(miles/hour)']
    group_cols = ['Segment ID','Road','Intersection','Date']
    
    cmp_period_agg = df_cmp_period.groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                            'Segment Length(Miles)': 'sum'}).reset_index().compute()
    rename_cols = ['seg_id', 'road', 'intersection', 'date', 'sample_size', 'TT', 'Len']
    cmp_period_agg.columns = rename_cols
    
    cmp_period_agg['avg_speed'] = round(cmp_period_agg['Len']/cmp_period_agg['TT'],3)
    cmp_period_agg = cmp_period_agg[cmp_period_agg['sample_size']>=ss_threshold]

    cmp_period_agg['year'] = cur_year
    cmp_period_agg['period'] = cur_period
    cmp_period_agg['source']='INRIX'
    cmp_period_agg['comment']=''

    cmp_period_agg = cmp_period_agg.merge(conflation, how='left', on='seg_id')
    cmp_period_agg = cmp_period_agg.merge(cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], on='cmp_segid', how='left')
    
    cmp_period_agg = cmp_period_agg.merge(xd_segs, how='left', on='seg_id')
    
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm85']) & cmp_period_agg['road'].isin(FWY_NAMES)), 'cls_hcm85'] = 'Fwy'
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm00']) & cmp_period_agg['road'].isin(FWY_NAMES)), 'cls_hcm00'] = 'Fwy'
    
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm85']) & cmp_period_agg['road'].isin(CLS1_NAMES)), 'cls_hcm85'] = '1'
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm00']) & cmp_period_agg['road'].isin(CLS1_NAMES)), 'cls_hcm00'] = '2'
    
    cmp_period_agg.loc[pd.isna(cmp_period_agg['cls_hcm85']), 'cls_hcm85'] = '3'
    cmp_period_agg.loc[pd.isna(cmp_period_agg['cls_hcm00']), 'cls_hcm00'] = '4'
    
    cmp_period_agg['los_hcm85'] = cmp_period_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
    cmp_period_agg['los_hcm00'] = cmp_period_agg.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

    cmp_period_agg = cmp_period_agg[['seg_id', 'year', 'date', 'source', 'period',
                                     'road', 'intersection', 'frc', 'cls_hcm85', 'cls_hcm00',
                                     'avg_speed', 'los_hcm85', 'los_hcm00', 'sample_size', 'comment']]
    
    return cmp_period_agg

cmp_am_agg = cmp_seg_level_speed_and_los(df_am, cur_year = 2020, cur_period = 'AM')
cmp_pm_agg = cmp_seg_level_speed_and_los(df_pm, cur_year = 2020, cur_period = 'PM')

cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
cmp_segs_los.to_csv(os.path.join(OUT_DIR, OUT_FILE), index=False)
