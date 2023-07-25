# Import necessary packages
import pandas as pd
import geopandas as gp
import dask.dataframe as dd
from os.path import join
from XD_agg_corr import AGG_DICT
import warnings
warnings.filterwarnings("ignore")


FWY_NAMES = ['I-80 / BAY BRG','US-101 / GOLDEN GATE BRG','US-101','US-101 / CENTRAL FWY','I-280','I-80',
             
             'BAY BRG','GOLDEN GATE BRG','101 / US-101 S','101 / US-101 N','80 / I-80 E','80 / I-80 W',
             '280 / I-280 N','280 / I-280 S','CENTRAL FWY']

CLS1_NAMES = ['CA-1 / 19TH AVE','CA-35 / SLOAT BLVD','CA-1 / JUNIPERO SERRA BLVD',
              
              '19TH AVE','BROADWAY','BROADWAY TUNNEL','GUERRERO ST','JUNIPERO SERRA BLVD','SLOAT BLVD']

# SFCTA Paths
DATA_DIR = r'Q:\Data\Observed\Streets\INRIX\v2002'
XD_SHP_DIR = r'Q:\GIS\Transportation\Roads\INRIX\XD\20_02\shapefiles\SF'

# OUT_DIR = r'Q:\CMP\LOS Monitoring 2020\Auto_LOS\allxd'
# xd_file = 'Inrix_XD_2001_SF_agg.shp'

OUT_DIR = r'Q:\CMP\LOS Monitoring 2020\Auto_LOS\locxd'
xd_file = 'Inrix_XD_2002_SF.shp'

# OUT_FILE = 'Precovid_2020_allXD.csv'
# INPUT_PATHS = [['All_SF_2020-02-16_to_2020-03-08_1_min_part_', 10]]

# OUT_FILE = 'Covid_2020_allXD.csv'
# INPUT_PATHS = [['All_SF_2020-03-08_to_2020-04-05_1_min_part_', 12]]

# OUT_FILE = 'Jul2020_allXD_3.csv'
# INPUT_PATHS = [['All_SF_2020-07-19_to_2020-07-26_1_min_part_', 4]]

# OUT_FILE = 'Sep2020_allXD_4.csv'
# INPUT_PATHS = [['All_SF_2020-09-20_to_2020-09-27_1_min_part_', 4]]

OUT_FILE = 'Oct2020_allXD_4.csv'
INPUT_PATHS = [['All_SF_2020-10-18_to_2020-10-25_1_min_part_', 4]]

# OUT_FILE = 'Sep2020_allXD_4.csv'
# INPUT_PATHS = [['All_SF_2020-09-20_to_2020-09-27_1_min_part_', 4]]

# OUT_FILE = 'Oct2020_allXD_1.csv'
# INPUT_PATHS = [['All_SF_2020-09-27_to_2020-10-04_1_min_part_', 4]]

# OUT_FILE = 'Oct2020_allXD_2.csv'
# INPUT_PATHS = [['All_SF_2020-10-04_to_2020-10-11_1_min_part_', 4]]

# OUT_FILE = 'Oct2020_allXD_3.csv'
# INPUT_PATHS = [['All_SF_2020-10-11_to_2020-10-18_1_min_part_', 4]]
 
# OUT_FILE = 'Oct2020_allXD_4.csv'
# INPUT_PATHS = [['All_SF_2020-10-18_to_2020-10-25_1_min_part_', 4]]

# OUT_FILE = 'Jan2021_allXD_2.csv'
# INPUT_PATHS = [['All_SF_2021-01-10_to_2021-01-17_1_min_part_', 4]]

# OUT_FILE = 'Jan2021_allXD_3.csv'
# INPUT_PATHS = [['All_SF_2021-01-17_to_2021-01-24_1_min_part_', 4]]

# OUT_FILE = 'Jan2021_allXD_4.csv'
# INPUT_PATHS = [['All_SF_2021-01-24_to_2021-01-31_1_min_part_', 4]]

# OUT_FILE = 'Feb2021_allXD_1.csv'
# INPUT_PATHS = [['All_SF_2021-01-31_to_2021-02-07_1_min_part_', 4]]

# OUT_FILE = 'Feb2021_allXD_2.csv'
# INPUT_PATHS = [['All_SF_2021-02-07_to_2021-02-14_1_min_part_', 4]]

# OUT_FILE = 'Feb2021_allXD_3.csv'
# INPUT_PATHS = [['All_SF_2021-02-14_to_2021-02-21_1_min_part_', 4]]


# Minimum sample size per day per peak period
ss_threshold = 10

# Get FRC from XD shapefile
xd_segs = gp.read_file(join(XD_SHP_DIR, xd_file))
xd_segs = xd_segs.loc[~pd.isna(xd_segs['RoadName']), ]
xd_segs = xd_segs[['XDSegID','FRC']]
xd_segs.columns = ['seg_id', 'frc']
xd_segs = xd_segs.astype(int)

# Read in the INRIX data using dask to save memory
df_cmp = pd.DataFrame()
for p in INPUT_PATHS:
    for i in range(1,p[1]):
        df1 = dd.read_csv(join(DATA_DIR, '%s%s\data.csv' %(p[0],i)), assume_missing=True)
        meta_df = dd.read_csv(join(DATA_DIR, '%s%s\metadata.csv' %(p[0],i)))
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

df_cmp['Intersection'] = df_cmp['Intersection'].fillna('')

#Remove weekends
df_cmp = df_cmp[df_cmp['DOW']<5]
df_cmp = df_cmp[df_cmp['Speed(miles/hour)']>0]

# Combine XD segments
for k, l in AGG_DICT.items():
    df_cmp[df_cmp['Segment ID'].isin(l)]['Segment ID']=k

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
    
    cmp_period_agg.loc[cmp_period_agg['road'].isin(FWY_NAMES), 'cls_hcm85'] = 'Fwy'
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm85']) & cmp_period_agg['road'].isin(CLS1_NAMES)), 'cls_hcm85'] = '1'
    
    cmp_period_agg = cmp_period_agg.merge(xd_segs, how='right', on='seg_id')
    cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm85']) & cmp_period_agg['frc'].isin([0])), 'cls_hcm85'] = 'Fwy'
#     cmp_period_agg.loc[(pd.isna(cmp_period_agg['cls_hcm85']) & cmp_period_agg['frc'].isin([])), 'cls_hcm85'] = '1'
    cmp_period_agg.loc[pd.isna(cmp_period_agg['cls_hcm85']), 'cls_hcm85'] = '3'
    
    cmp_period_agg['los_hcm85'] = cmp_period_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)

    cmp_period_agg = cmp_period_agg[['seg_id', 'year', 'date', 'source', 'period',
                                     'road', 'intersection', 'frc', 'cls_hcm85',
                                     'avg_speed', 'los_hcm85', 'sample_size', 'comment']]
    
    return cmp_period_agg

cmp_am_agg = cmp_seg_level_speed_and_los(df_am, cur_year = 2020, cur_period = 'AM')
cmp_pm_agg = cmp_seg_level_speed_and_los(df_pm, cur_year = 2020, cur_period = 'PM')

cmp_segs_los = cmp_am_agg.append(cmp_pm_agg, ignore_index=True)
cmp_segs_los.to_csv(join(OUT_DIR, OUT_FILE), index=False)
