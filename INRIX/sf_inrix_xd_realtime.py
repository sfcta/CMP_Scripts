from glob import glob
from pprint import pprint
import re
import pandas as pd
import numpy as np
import geopandas as gp
import dask.dataframe as dd
import os

from zipfile import ZipFile

NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2023\Network_Conflation\v2301'
CORR_FILE = r'CMP_Segment_INRIX_Links_Correspondence_2301_Manual-expandednetwork.csv'
CMP_SHP = os.path.join(r'Q:\CMP\LOS Monitoring 2022\CMP_exp_shp', 'cmp_segments_exp_v2201.shp')
DATA_DIR = r'Q:\Data\Observed\Streets\INRIX\v2301'
OUT_DIR = r'Q:\CMP\Congestion_Tracker\viz_data\v2301'
YEAR=2023

# OUT_FILES = ['202303_AutoSpeeds']
# INPUT_GLOBS = [
#     r'2023\03\sf_2023-03-01_to_2023-04-01_1_min_part_*.zip'
# ]

# OUT_FILES = ['202304_AutoSpeeds']
# INPUT_GLOBS = [
#     r'2023\04\sf_2023-04-01_to_2023-05-01_1_min_part_*.zip'
# ]

OUT_FILES = ['202305_AutoSpeeds']
INPUT_GLOBS = [
    r'2023\05\sf_2023-05-01_to_2023-06-01_1_min_part_*.zip'
]

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

# Define percentile funtion to get 5th and 20th percentile speed
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


# Calculate CMP segment level average speeds and LOS
def spatial_coverage(df, cmp_period_agg, group_cols, rename_cols, ss_threshold, coverage_threshold):
    # select the segment IDs that are not in cmp_period_agg yet (i.e. not
    # covered by all the previous coverage thresholds yet), but that are covered
    # under the current coverage threshold
    select = (~df['CMP_SegID'].isin(cmp_period_agg['cmp_segid'])) & (df['SpatialCov']>=coverage_threshold)
    cmp_tt_agg = pd.DataFrame()
    if select.sum() > 0:
        cmp_tt_agg = df[select].groupby(group_cols).agg({'TT': ['count', 'sum'],
                                                                                         'Length_Matched': 'sum',
                                                                                         'Speed': ['std', percentile(5), percentile(20)]}).reset_index()
        cmp_tt_agg.columns = rename_cols
        cmp_tt_agg['avg_speed'] = round(cmp_tt_agg['Len']/cmp_tt_agg['TT'],3)
        cmp_tt_agg['cov'] = round(100*cmp_tt_agg['std_speed']/cmp_tt_agg['avg_speed'],3)
        cmp_tt_agg = cmp_tt_agg[cmp_tt_agg['sample_size']>=ss_threshold]
    
    if len(cmp_tt_agg)>0:
        cmp_tt_agg['comment'] = 'Calculation performed on ' + str(coverage_threshold) +'% or greater of length'
        return cmp_tt_agg
    else:
        # might be better to return an empty DataFrame?
        # A maybe monad would've been useful
        return None


# Calculate CMP segment level average speeds, LOS, and reliability metrics
def cmp_seg_level_speed_and_los(df_cmp_period, ss_threshold, cur_year, cur_period):
    df_cmp_period['TT'] = df_cmp_period['Length_Matched']/df_cmp_period['Speed(miles/hour)']

    # Get total travel time at a particular date_time on a CMP segment
    cmp_period = df_cmp_period.groupby(['CMP_SegID', 'Date','Date_Time']).agg({'Length_Matched': 'sum',
                                                               'TT': 'sum'}).reset_index()

    cmp_period = pd.merge(cmp_period, conf_len, on='CMP_SegID', how='left')
    cmp_period['Speed'] = cmp_period['Length_Matched']/cmp_period['TT']
    cmp_period['SpatialCov'] = 100*cmp_period['Length_Matched']/cmp_period['CMP_Length']  #Spatial coverage 
    
    group_cols = ['CMP_SegID','Date']
    rename_cols = ['cmp_segid', 'date', 'sample_size', 'TT', 'Len', 'std_speed', 'pcnt5th', 'pcnt20th']

    coverage_thresholds = [99, 95, 90, 85, 80, 75, 70]
    cmp_period_agg = pd.DataFrame(columns = rename_cols)
    for coverage_threshold in coverage_thresholds:
        coverage = spatial_coverage(cmp_period, cmp_period_agg, group_cols, rename_cols, ss_threshold, coverage_threshold)
        if coverage is not None:
            pd.concat((cmp_period_agg, coverage), ignore_index=True)

    cmp_period_agg['year'] = cur_year
    cmp_period_agg['period'] = cur_period
    
    cmp_period_agg['cmp_segid'] = cmp_period_agg['cmp_segid'].astype('int64') # MAX ADDED (Cast to int for merging)

    cmp_period_agg = pd.merge(cmp_period_agg, cmp_segs[['cmp_segid', 'cls_hcm85', 'cls_hcm00']], on='cmp_segid', how='left')
    cmp_period_agg['los_hcm85'] = cmp_period_agg.apply(lambda x: los_1985(x.cls_hcm85, x.avg_speed), axis=1)
    cmp_period_agg['los_hcm00'] = cmp_period_agg.apply(lambda x: los_2000(x.cls_hcm00, x.avg_speed), axis=1)

    cmp_period_agg = cmp_period_agg[['cmp_segid', 'year', 'date', 'period', 'avg_speed', 'los_hcm85', 'los_hcm00', 'std_speed', 'pcnt5th', 'pcnt20th', 'cov', 'sample_size', 'comment']]
    
    return cmp_period_agg


def _dirs_in_zip(zip_filepath: str) -> set:
    with ZipFile(zip_filepath, "r") as z:
        return set([os.path.dirname(f) for f in z.namelist()])


def _zip_hashdir(zip_filepath: str) -> str:
    dirs = _dirs_in_zip(zip_filepath)
    if len(dirs) > 1:
        raise RuntimeError(
            f"Multiple directories found inside {zip_filepath}. There should "
            "only be one directory inside the zip files downloaded from INRIX."
        )
    return next(iter(dirs))  # return the next/only item in dirs

            
if __name__ == "__main__":
    for OUT_FILE, INPUT_GLOB in zip(OUT_FILES, INPUT_GLOBS):

        # Sample size thresholds
        ss_threshold_peaks = 10 # Minimum sample size for the AM/PM monitoring period
        ss_threshold_hourly = 10 # Miniumum sample size for hourly

        # Input CMP segment shapefile
        cmp_segs=gp.read_file(CMP_SHP)

        # Get CMP and INRIX correspondence table
        conflation = pd.read_csv(os.path.join(NETCONF_DIR, CORR_FILE))
        conflation[['CMP_SegID','INRIX_SegID']] = conflation[['CMP_SegID','INRIX_SegID']].astype(int)
        conf_len=conflation.groupby('CMP_SegID').Length_Matched.sum().reset_index()
        conf_len.columns = ['CMP_SegID', 'CMP_Length']

        # Read in the INRIX data using dask to save memory
        zip_filepaths = sorted(
            glob(os.path.join(DATA_DIR, INPUT_GLOB)),
            # natural sort of part numbers
            key=lambda f: [int(x) for x in re.findall(r'\d+', f)]
        )
        print("inputs files:")
        pprint(zip_filepaths)
        input_dfs = []
        for zip_filepath in zip_filepaths:
            print(f'Reading input file {zip_filepath}...')
            # read_csv of zip handled through fsspec
            input_df = dd.read_csv(f"zip://{_zip_hashdir(zip_filepath)}/data.csv::file://{zip_filepath}", assume_missing=True)
            if len(input_df):  # if > 0
                input_df['Segment ID'] = input_df['Segment ID'].astype('int')
                input_df = input_df[input_df['Segment ID'].isin(conflation['INRIX_SegID'])]
                input_dfs.append(input_df)
        df_cmp = dd.concat(input_dfs, axis=0, interleave_partitions=True)

        df_cmp['Segment ID'] = df_cmp['Segment ID'].astype('int')

        # Calculate reference speed for CMP segments
        inrix_link_refspd = df_cmp.drop_duplicates(subset=['Segment ID', 'Ref Speed(miles/hour)']).compute()
        cmp_link_refspd = pd.merge(conflation, inrix_link_refspd[['Segment ID', 'Ref Speed(miles/hour)']], left_on='INRIX_SegID', right_on='Segment ID', how='left')
        cmp_link_refspd = cmp_link_refspd[pd.notnull(cmp_link_refspd['Ref Speed(miles/hour)'])]
        cmp_link_refspd['RefTT'] = cmp_link_refspd['Length_Matched']/cmp_link_refspd['Ref Speed(miles/hour)']
        cmp_segs_refspd= cmp_link_refspd.groupby(['CMP_SegID']).agg({'Length_Matched': 'sum',
                                                                        'RefTT': 'sum'}).reset_index()
        cmp_segs_refspd['refspd_inrix'] = cmp_segs_refspd['Length_Matched']/cmp_segs_refspd['RefTT']
        cmp_segs_refspd['cmp_segid'] = cmp_segs_refspd['CMP_SegID']

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

        # Get inrix_segid : cmp_segid mapping
        df_cmp = df_cmp.merge(conflation, left_on='Segment ID', right_on='INRIX_SegID', how='outer')

        # convert to pandas dataframe for speed
        df_cmp = df_cmp.compute()

        print('Processing hourly & AM/PM periods...')

        cmp_segs_los_dfs = []

        # Hourly
        for hour in range(24):
            print(f'Hour = {hour}')
            subset = df_cmp[(df_cmp['Hour']==hour)]
            cmp_segs_los_dfs.append(cmp_seg_level_speed_and_los(subset, ss_threshold_hourly, cur_year=YEAR, cur_period=hour))

        # AM (7-9am)
        print('AM Period')
        subset = df_cmp[(df_cmp['Hour']==7) | (df_cmp['Hour']==8)]
        cmp_segs_los_dfs.append(cmp_seg_level_speed_and_los(subset, ss_threshold_peaks, cur_year=YEAR, cur_period='AM'))

        # PM (4:30-6:30pm)
        print('PM Period')
        subset = df_cmp[((df_cmp['Hour']==16) & (df_cmp['Minute']>=30)) | (df_cmp['Hour']==17) | ((df_cmp['Hour']==18) & (df_cmp['Minute']<30))]
        cmp_segs_los_dfs.append(cmp_seg_level_speed_and_los(subset, ss_threshold_peaks, cur_year=YEAR, cur_period='PM'))

        cmp_segs_los = pd.concat(cmp_segs_los_dfs, ignore_index=True)

        print('Finished processing periods.')

        # Calculate reliability metrics
        cmp_segs_los = pd.merge(cmp_segs_los, cmp_segs_refspd[['cmp_segid', 'refspd_inrix']], on='cmp_segid', how='left')
        cmp_segs_los['tti95'] = np.maximum(1, cmp_segs_los['refspd_inrix']/cmp_segs_los['pcnt5th'])
        cmp_segs_los['tti80'] = np.maximum(1, cmp_segs_los['refspd_inrix']/cmp_segs_los['pcnt20th'])
        cmp_segs_los['bi'] = np.maximum(0, cmp_segs_los['avg_speed']/cmp_segs_los['pcnt5th']-1)

        # Write out df for both hourly & AM/PM peak
        peaks = cmp_segs_los[cmp_segs_los['period'].isin(['AM','PM'])]
        hourly = cmp_segs_los[~cmp_segs_los['period'].isin(['AM','PM'])]
        hourly.rename(columns={'period':'hour'}, inplace=True)
        hourly.to_csv(os.path.join(OUT_DIR, OUT_FILE + '_Hourly' + '.csv' ), index=False)
        peaks.to_csv(os.path.join(OUT_DIR, OUT_FILE + '_AMPM'+ '.csv' ), index=False)