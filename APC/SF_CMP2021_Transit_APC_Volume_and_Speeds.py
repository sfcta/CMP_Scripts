import pandas as pd
import numpy as np
import geopandas as gp
import math
import warnings
warnings.filterwarnings("ignore")
import os

# UK Paths
APC_DIR = r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\APC'

CMPplus_DIR = r'Z:\SF_CMP\CMP2021\CMP_plus_shp'
INRIX_DIR = r'Z:\SF_CMP\CMP2021\Inrix_XD_2101_SF_manualedit'  # Add INRIX street names to be more comprehensive
NETCONF_DIR = r'Z:\SF_CMP\CMP2021'

# Read in transit stop file downloaded from GTFS
stops = gp.read_file(os.path.join(APC_DIR, 'GTFS_stops.shp')

# Read in transit APC data
apc = pd.read_csv(os.path.join(APC_DIR, 'APC_STOP_BOARDINGS_FACTS_SFCTACMP2021.csv')

# Read in the list of stop pairs manually identified that are patially overlap with cmp segments
overlap_pairs = pd.read_csv(os.path.join(APC_DIR, 'Postprocessing_overlapping_transit_segments.csv'))
                  
apc_notnull = apc[pd.notnull(apc['CLOSE_DATE_TIME'])]
print('Percent of records with null value: ', round(100 - 100*(len(apc_notnull)/len(apc)),2))

apc_notnull['Date']=pd.to_datetime(apc_notnull['ACTUALDATE'])
apc_notnull['DOW']=apc_notnull.Date.dt.dayofweek #Mon-0
apc_notnull['DayType'] = np.where(apc_notnull['DOW']<=4, 'Weekdays', np.where(apc_notnull['DOW']==5, 'Saturday', 'Sunday'))
apc_notnull['Month']=apc_notnull.Date.dt.month

apc_notnull['Close_Hour'] = apc_notnull['CLOSE_DATE_TIME'].str[10:12].astype(int)
apc_notnull['Close_Minute'] = apc_notnull['CLOSE_DATE_TIME'].str[13:15].astype(int)
apc_notnull['Close_Second'] = apc_notnull['CLOSE_DATE_TIME'].str[16:18].astype(int)
apc_notnull['Close_Period'] = apc_notnull['CLOSE_DATE_TIME'].str[-2:]
apc_notnull['Close_Time'] = apc_notnull['Date'].astype('str') + ' ' + apc_notnull['Close_Hour'].astype('str') + ':' + apc_notnull['Close_Minute'].astype('str') + ':' + apc_notnull['Close_Second'].astype('str') + ' ' + apc_notnull['Close_Period']
apc_notnull['Close_Time'] = pd.to_datetime(apc_notnull['Close_Time'])
apc_notnull['Epoch'] = 2*apc_notnull['Close_Time'].dt.hour + apc_notnull['Close_Time'].dt.minute//30 

apc_notnull['Open_Hour'] = apc_notnull['OPEN_DATE_TIME'].str[10:12].astype(int)
apc_notnull['Open_Minute'] = apc_notnull['OPEN_DATE_TIME'].str[13:15].astype(int)
apc_notnull['Open_Second'] = apc_notnull['OPEN_DATE_TIME'].str[16:18].astype(int)
apc_notnull['Open_Period'] = apc_notnull['OPEN_DATE_TIME'].str[-2:]
apc_notnull['Open_Time'] = apc_notnull['Date'].astype('str') + ' ' + apc_notnull['Open_Hour'].astype('str') + ':' + apc_notnull['Open_Minute'].astype('str') + ':' + apc_notnull['Open_Second'].astype('str') + ' ' + apc_notnull['Open_Period']
apc_notnull['Open_Time'] = pd.to_datetime(apc_notnull['Open_Time'])

# ------------------------------ 1. Calculate transit boardings, alightings, and loads ------------------------------
vol_sum = apc_notnull.groupby(['BS_ID', 'Date', 'Epoch']).agg({'ONS': 'sum', 'OFFS': 'sum', 'MAX_LOAD': 'sum', 'Close_Period':'count'}).reset_index()
vol_sum.columns = ['stopid', 'date', 'epoch', 'boardings', 'alightings', 'loads', 'samples']

vol_sum['date']=pd.to_datetime(vol_sum['date'])
vol_sum['dow']=vol_sum.date.dt.dayofweek #Mon-0
vol_sum['daytype'] = np.where(vol_sum['dow']<=4, 'Weekdays', np.where(vol_sum['dow']==5, 'Saturday', 'Sunday'))

# Remove Mondays and Fridays to get typical weekdays
vol_sum = vol_sum[(vol_sum['dow']!=0) & (vol_sum['dow']!=4)]

vol_daytype_avg_stops = vol_sum.groupby(['stopid', 'daytype', 'epoch']).agg({'boardings': 'mean', 
                                                                             'alightings': 'mean',
                                                                            'loads': 'mean'}).reset_index()
vol_daytype_avg_stops.columns= ['stopid', 'daytype', 'epoch', 'boardings', 'alightings', 'loads']

# Create empty dataframe with continuous time periods
df_epoch = pd.DataFrame([ep for ep in range(48)], columns=['epoch'])
df_epoch['hour'] = df_epoch['epoch']//2
df_epoch['minute'] = np.where(df_epoch['epoch']%2 ==0, '00', '30')
df_epoch['period'] = df_epoch['hour'].astype(str) + ':' + df_epoch['minute']

daytypes = ['Weekdays', 'Saturday', 'Sunday']
df_day_epoch = pd.DataFrame()
for day_type in daytypes:
    df_epoch['daytype'] = day_type
    df_day_epoch = df_day_epoch.append(df_epoch, ignore_index=True)

stop_list = stops.stop_id.unique().tolist()
stop_vol_complete = pd.DataFrame()
for stop_id in stop_list:
    df_day_epoch['stopid'] = stop_id
    stop_vol_complete = stop_vol_complete.append(df_day_epoch, ignore_index=True)
    
stop_vol_complete = pd.merge(stop_vol_complete, vol_daytype_avg_stops, on=['stopid', 'daytype', 'epoch'], how='left')

# Save volume output file
stop_vol_complete.to_csv(os.path.join(APC_DIR, 'CMP2021_APC_Transit_Volume.csv'), index=False)


# ------------------------------ 2. Calculate transit speed and reliability ------------------------------

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}
#Define NAD 1983 StatePlane California III
cal3 = {'proj': 'lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002', 'ellps': 'GRS80', 'datum': 'NAD83', 'no_defs': True}

# CMP network
cmp_segs_org = gp.read_file(os.path.join(CMPplus_DIR, 'cmp_segments_plus.shp'))
cmp_segs_prj = cmp_segs_org.to_crs(cal3)
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.replace('/ ','/')
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.lower()
cmp_segs_prj['Length'] = cmp_segs_prj.geometry.length 
cmp_segs_prj['Length'] = cmp_segs_prj['Length'] * 3.2808  #meters to feet

# INRIX network
inrix_net=gp.read_file(os.path.join(INRIX_DIR, 'Inrix_XD_2101_SF_manualedit.shp'))
inrix_net['RoadName'] = inrix_net['RoadName'].str.lower()

cmp_inrix_corr = pd.read_csv(os.path.join(NETCONF_DIR, 'CMP_Segment_INRIX_Links_Correspondence_2101_Manual_PLUS_Updated.csv'))

# Convert transit stops original coordinate system to state plane
stops = stops.to_crs(cal3)
stops['stop_name'] = stops['stop_name'].str.lower()
stops[['street_1','street_2']] = stops.stop_name.str.split('&', expand=True)  #split stop name into operating street and intersecting street


# Create a buffer zone for each cmp segment
ft=160   # According to the memo from last CMP cycle
mt=round(ft/3.2808,4)
stops_buffer=stops.copy()
stops_buffer['geometry'] = stops_buffer.geometry.buffer(mt)
#stops_buffer.to_file(os.path.join(MAIN_DIR, 'stops_buffer.shp'))

# cmp segments intersecting transit stop buffer zone
cmp_segs_intersect=gp.sjoin(cmp_segs_prj, stops_buffer, op='intersects').reset_index()

stops['near_cmp'] = 0
cmp_segs_intersect['name_match']=0
for stop_idx in range(len(stops)):
    stop_id = stops.loc[stop_idx, 'stop_id']
    stop_geo = stops.loc[stop_idx]['geometry']
    stop_names = stops.loc[stop_idx]['street_1'].split('/')
    if 'point lobos' in stop_names:
        stop_names = stop_names + ['geary']
    if stop_id == 7357:
        stop_names = stop_names + ['third st']
    
    cmp_segs_int = cmp_segs_intersect[cmp_segs_intersect['stop_id']==stop_id]
    cmp_segs_idx = cmp_segs_intersect.index[cmp_segs_intersect['stop_id']==stop_id].tolist()
    
    near_dis = 5000
    if ~cmp_segs_int.empty:
        for seg_idx in cmp_segs_idx:
            cmp_seg_id = cmp_segs_int.loc[seg_idx, 'cmp_segid']
            cmp_seg_geo = cmp_segs_int.loc[seg_idx]['geometry']
            cmp_seg_names = cmp_segs_int.loc[seg_idx]['cmp_name'].split('/')
            if 'bayshore' in cmp_seg_names:
                cmp_seg_names = cmp_seg_names + ['bay shore']
            if '3rd st' in cmp_seg_names:
                cmp_seg_names = cmp_seg_names + ['third st']
            if '19th ave' in cmp_seg_names:
                cmp_seg_names = cmp_seg_names + ['19th avenue']
            if 'geary' in cmp_seg_names:
                cmp_seg_names = cmp_seg_names + ['point lobos']
                
            
            # Add INRIX street name to be comprehensive
            inrix_links = cmp_inrix_corr[cmp_inrix_corr['CMP_SegID']==cmp_seg_id]
            inrix_link_names = inrix_net[inrix_net['XDSegID'].isin(inrix_links['INRIX_SegID'])]['RoadName'].tolist()
            inrix_link_names = list(filter(None, inrix_link_names)) 
            if len(inrix_link_names) > 0:
                inrix_link_names = list(set(inrix_link_names))
                cmp_seg_link_names = cmp_seg_names + inrix_link_names
            else:
                cmp_seg_link_names = cmp_seg_names
            
            matched_names= [stop_name for stop_name in stop_names if any(cname in stop_name for cname in cmp_seg_link_names)]
            if len(matched_names)>0:
                stops.loc[stop_idx, 'near_cmp']=1
                cmp_segs_intersect.loc[seg_idx, 'name_match']=1
                cur_dis = stop_geo.distance(cmp_seg_geo)
                cmp_segs_intersect.loc[seg_idx, 'distance']=cur_dis
                near_dis = min(near_dis, cur_dis)
                stops.loc[stop_idx, 'near_dis']=near_dis

stops_near_cmp = stops[stops['near_cmp']==1]
stops_near_cmp = stops_near_cmp.to_crs(wgs84)

stops_near_cmp_list = stops_near_cmp['stop_id'].unique().tolist()

cmp_segs_near = cmp_segs_intersect[cmp_segs_intersect['name_match']==1]

# Remove mismatched stops based on manual review
remove_cmp_stop = [(175, 5546), (175, 5547), (175, 7299), (175, 5544),
                  (66, 7744),
                  (214, 7235),
                  (107, 4735),
                  (115, 4275),
                  (143, 4824),
                  (172, 5603)]
for remove_idx in range(len(remove_cmp_stop)):
    rmv_cmp_id = remove_cmp_stop[remove_idx][0]
    rmv_stop_id = remove_cmp_stop[remove_idx][1]
    remove_df_idx = cmp_segs_near.index[(cmp_segs_near['cmp_segid']==rmv_cmp_id) & (cmp_segs_near['stop_id']==rmv_stop_id)].tolist()
    if len(remove_df_idx) > 0:
        cmp_segs_near = cmp_segs_near.drop([remove_df_idx[0]], axis=0)

apc_cmp = apc_notnull[(apc_notnull['DOW'] ==1) | (apc_notnull['DOW'] ==2) | (apc_notnull['DOW'] ==3)]  # only Tue, Wed, and Thu
apc_cmp['Day']=apc_cmp.Date.dt.day
apc_cmp = apc_cmp[((apc_cmp['Month']==4) & (apc_cmp['Day'] > 5)) | ((apc_cmp['Month']==5) & (apc_cmp['Day'] < 21))] # CMP monitoring dates
apc_cmp['Open_Time_float'] = apc_cmp['Open_Hour'] + apc_cmp['Open_Minute']/60 + apc_cmp['Open_Second']/3600
apc_cmp['Close_Time_float'] = apc_cmp['Close_Hour'] + apc_cmp['Close_Minute']/60 + apc_cmp['Close_Second']/3600

# # Match AM&PM transit stops to CMP segments
angle_thrd = 10
def match_stop_pairs_to_cmp(apc_cmp_df, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd):
    apc_pairs_df = pd.DataFrame()
    pair_cnt = 0
    for cur_stop_idx in range(len(apc_cmp_df)-2):  #Enumerate the records in apc_cmp_df
        # Check if the stop_id is in the previously matched list
        if apc_cmp_df.loc[cur_stop_idx, 'stop_id'] in stops_near_cmp_list:
            next_stop_match = 0
            if apc_cmp_df.loc[cur_stop_idx + 1, 'stop_id'] in stops_near_cmp_list:
                next_stop_idx = cur_stop_idx + 1 
                next_stop_match = 1
            # This is to ensure the stop_id associated with cur_stop_idx + 2 is also checked when the stop_id associated with cur_stop_idx + 1 is not found in the matched list
            elif apc_cmp_df.loc[cur_stop_idx + 2, 'stop_id'] in stops_near_cmp_list:
                next_stop_idx = cur_stop_idx +2
                next_stop_match = 2

            if next_stop_match >0:
                # Transit stop of interest
                cur_stop_trip_id = apc_cmp_df.loc[cur_stop_idx, 'TRIP_ID_EXTERNAL']
                cur_stop_date = apc_cmp_df.loc[cur_stop_idx, 'Date']
                cur_stop_veh_id = apc_cmp_df.loc[cur_stop_idx, 'VEHICLE_ID']

                # Succeeding candidate stop in the dataframe
                next_stop_trip_id = apc_cmp_df.loc[next_stop_idx, 'TRIP_ID_EXTERNAL']
                next_stop_date = apc_cmp_df.loc[next_stop_idx, 'Date']
                next_stop_veh_id = apc_cmp_df.loc[next_stop_idx, 'VEHICLE_ID']

                # Check if two stops share the same trip id, date, and vehicle id
                if (cur_stop_trip_id == next_stop_trip_id) & (cur_stop_date == next_stop_date) & (cur_stop_veh_id == next_stop_veh_id):

                    cur_stop_id = apc_cmp_df.loc[cur_stop_idx, 'stop_id']
                    cur_stop_open_time = apc_cmp_df.loc[cur_stop_idx, 'Open_Time']
                    cur_stop_close_time = apc_cmp_df.loc[cur_stop_idx, 'Close_Time']
                    cur_stop_dwell_time = apc_cmp_df.loc[cur_stop_idx, 'DWELL_TIME']

                    next_stop_id = apc_cmp_df.loc[next_stop_idx, 'stop_id']
                    next_stop_open_time = apc_cmp_df.loc[next_stop_idx, 'Open_Time']
                    next_stop_close_time = apc_cmp_df.loc[next_stop_idx, 'Close_Time']
                    next_stop_dwell_time = apc_cmp_df.loc[next_stop_idx, 'DWELL_TIME']

                    # Matched CMP segments for the current stop
                    cur_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==cur_stop_id]['cmp_segid'])
                    # Matched CMP segments for the succeeding stop
                    next_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==next_stop_id]['cmp_segid'])

                    # Find the common CMP segments in two sets
                    common_segs = list(set(cur_stop_near_segs) & set(next_stop_near_segs))
                    
                    if len(common_segs)>0:
                        cur_stop_geo = apc_cmp_df.loc[cur_stop_idx]['geometry']   # location geometry of current stop
                        next_stop_geo = apc_cmp_df.loc[next_stop_idx]['geometry']  # location geometry of succeeding stop

                        # Traveling direction from current stop to succeeding stop
                        stop_degree=(180 / math.pi) * math.atan2(next_stop_geo.x - cur_stop_geo.x, next_stop_geo.y - cur_stop_geo.y)

                        for common_seg_id in common_segs: # Iterate through the common segments set
                            common_seg_geo = cmp_segs_prj[cmp_segs_prj['cmp_segid']==common_seg_id]['geometry']  # line geometry of the cmp segment

                            cur_stop_dis = common_seg_geo.project(cur_stop_geo)  # distance between the beginning point of cmp segment and the projected transit stop
                            cur_stop_projected = common_seg_geo.interpolate(cur_stop_dis) #project current stop onto cmp segment
                            cur_stop_loc = cur_stop_dis * 3.2808  #meters to feet

                            next_stop_dis = common_seg_geo.project(next_stop_geo)
                            next_stop_projected = common_seg_geo.interpolate(next_stop_dis) #project next stop onto cmp segment
                            next_stop_loc = next_stop_dis * 3.2808  #meters to feet

                            # Ensure the degree is calculated following the traveling direction
                            if float(next_stop_loc) > float(cur_stop_loc):
                                cmp_degree=(180 / math.pi) * math.atan2(next_stop_projected.x - cur_stop_projected.x, next_stop_projected.y - cur_stop_projected.y)
                            else:
                                cmp_degree=(180 / math.pi) * math.atan2(cur_stop_projected.x - next_stop_projected.x, cur_stop_projected.y - next_stop_projected.y)

                            stop_cmp_angle = abs(cmp_degree-stop_degree)
                            if stop_cmp_angle>270:
                                stop_cmp_angle=360-stop_cmp_angle

                            if stop_cmp_angle < angle_thrd:   # Angle between CMP segment and transit stop pair meets the requirement
                                cur_next_traveltime = (next_stop_open_time-cur_stop_open_time).total_seconds()
                                cur_next_loc_dis = abs(float(next_stop_loc) - float(cur_stop_loc)) / 5280  #feet to miles

                                # Remove records deemed erroneous according to memo from last cycle
                                if (cur_next_loc_dis>0) & (cur_next_traveltime>0):
                                    if 3600*cur_next_loc_dis/cur_next_traveltime<=55:
                                        # Add matched stop pairs to the new dataframe for subsequent speed calculation
                                        apc_pairs_df.loc[pair_cnt, 'cmp_segid'] = common_seg_id
                                        apc_pairs_df.loc[pair_cnt, 'trip_id'] = cur_stop_trip_id
                                        apc_pairs_df.loc[pair_cnt, 'trip_date'] = cur_stop_date
                                        apc_pairs_df.loc[pair_cnt, 'vehicle_id'] = cur_stop_veh_id

                                        apc_pairs_df.loc[pair_cnt, 'cur_stop_id'] = cur_stop_id
                                        apc_pairs_df.loc[pair_cnt, 'cur_stop_open_time'] = cur_stop_open_time
                                        apc_pairs_df.loc[pair_cnt, 'cur_stop_close_time'] = cur_stop_close_time
                                        apc_pairs_df.loc[pair_cnt, 'cur_stop_dwell_time'] = cur_stop_dwell_time
                                        apc_pairs_df.loc[pair_cnt, 'cur_stop_loc'] = float(cur_stop_loc)

                                        apc_pairs_df.loc[pair_cnt, 'next_stop_id'] = next_stop_id
                                        apc_pairs_df.loc[pair_cnt, 'next_stop_open_time'] = next_stop_open_time
                                        apc_pairs_df.loc[pair_cnt, 'next_stop_close_time'] = next_stop_close_time
                                        apc_pairs_df.loc[pair_cnt, 'next_stop_dwell_time'] = next_stop_dwell_time
                                        apc_pairs_df.loc[pair_cnt, 'next_stop_loc'] = float(next_stop_loc)

                                        apc_pairs_df.loc[pair_cnt, 'cur_next_time'] = cur_next_traveltime
                                        apc_pairs_df.loc[pair_cnt, 'cur_next_loc_dis'] = cur_next_loc_dis
                                        if next_stop_match == 1: 
                                            apc_pairs_df.loc[pair_cnt, 'cur_next_rev_dis'] = apc_cmp_df.loc[cur_stop_idx, 'REV_DISTANCE']
                                        else:
                                            apc_pairs_df.loc[pair_cnt, 'cur_next_rev_dis'] = apc_cmp_df.loc[cur_stop_idx, 'REV_DISTANCE'] + apc_cmp_df.loc[cur_stop_idx + 1, 'REV_DISTANCE']

                                        pair_cnt = pair_cnt + 1

        if cur_stop_idx % 50000 == 0:
            print('Processed %s percent' % round(100*cur_stop_idx/len(apc_cmp_df), 2))
    return apc_pairs_df
    
# ## AM 
apc_cmp_am = apc_cmp[(apc_cmp['Open_Hour']<9) & (apc_cmp['Close_Hour']>6)]

apc_cmp_am = apc_cmp_am.merge(stops, left_on='BS_ID', right_on='stop_id', how='left')
apc_cmp_am = apc_cmp_am.sort_values(by=['TRIP_ID_EXTERNAL', 'Date', 'VEHICLE_ID', 'Open_Time']).reset_index()

print('------------Start processing AM trips------------')
apc_pairs_am = match_stop_pairs_to_cmp(apc_cmp_am, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd)

# Update the stop location for CMP 175 due to its irregular geometry
apc_pairs_am['cur_stop_loc'] = np.where(apc_pairs_am['cmp_segid']==175,
                                     np.where(apc_pairs_am['cur_stop_id']==5543, 173.935,
                                             np.where(apc_pairs_am['cur_stop_id']==5545, 1020.629,
                                                     np.where(apc_pairs_am['cur_stop_id']==5836, 1804.72,
                                                             np.where(apc_pairs_am['cur_stop_id']==5835, 2685.807, apc_pairs_am['cur_stop_loc'])))),
                                      apc_pairs_am['cur_stop_loc'])
apc_pairs_am['next_stop_loc'] = np.where(apc_pairs_am['cmp_segid']==175,
                                     np.where(apc_pairs_am['next_stop_id']==5543, 173.935,
                                             np.where(apc_pairs_am['next_stop_id']==5545, 1020.629,
                                                     np.where(apc_pairs_am['next_stop_id']==5836, 1804.72,
                                                             np.where(apc_pairs_am['next_stop_id']==5835, 2685.807, apc_pairs_am['next_stop_loc'])))),
                                      apc_pairs_am['next_stop_loc'])
apc_pairs_am['cur_next_loc_dis'] = np.where(apc_pairs_am['cmp_segid']==175,
                                     abs(apc_pairs_am['next_stop_loc'] - apc_pairs_am['cur_stop_loc']) / 5280,
                                      apc_pairs_am['next_stop_loc'])
                                      
# ## PM 
apc_cmp_pm = apc_cmp[(apc_cmp['Close_Period']=='PM') & ((apc_cmp['Open_Time_float']<=6.5) & (apc_cmp['Close_Time_float']>=4.5))]

apc_cmp_pm = apc_cmp_pm.merge(stops, left_on='BS_ID', right_on='stop_id', how='left')
apc_cmp_pm = apc_cmp_pm.sort_values(by=['TRIP_ID_EXTERNAL', 'Date', 'VEHICLE_ID', 'Open_Time']).reset_index()

print('------------Start processing PM trips------------')
apc_pairs_pm = match_stop_pairs_to_cmp(apc_cmp_pm, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd)

# Update the stop location for CMP 175 due to its irregular geometry
apc_pairs_pm['cur_stop_loc'] = np.where(apc_pairs_pm['cmp_segid']==175,
                                     np.where(apc_pairs_pm['cur_stop_id']==5543, 173.935,
                                             np.where(apc_pairs_pm['cur_stop_id']==5545, 1020.629,
                                                     np.where(apc_pairs_pm['cur_stop_id']==5836, 1804.72,
                                                             np.where(apc_pairs_pm['cur_stop_id']==5835, 2685.807, apc_pairs_pm['cur_stop_loc'])))),
                                      apc_pairs_pm['cur_stop_loc'])
apc_pairs_pm['next_stop_loc'] = np.where(apc_pairs_pm['cmp_segid']==175,
                                     np.where(apc_pairs_pm['next_stop_id']==5543, 173.935,
                                             np.where(apc_pairs_pm['next_stop_id']==5545, 1020.629,
                                                     np.where(apc_pairs_pm['next_stop_id']==5836, 1804.72,
                                                             np.where(apc_pairs_pm['next_stop_id']==5835, 2685.807, apc_pairs_pm['next_stop_loc'])))),
                                      apc_pairs_pm['next_stop_loc'])
apc_pairs_pm['cur_next_loc_dis'] = np.where(apc_pairs_pm['cmp_segid']==175,
                                            abs(apc_pairs_pm['next_stop_loc'] - apc_pairs_pm['cur_stop_loc']) / 5280,
                                            apc_pairs_pm['next_stop_loc'])

# # Transit Speeds on CMP Segment 
def match_intermediate_apc_stops(apc_pairs, apc_cmp, overlap_pairs, cmp_segs_prj, timep):
    # ### Auto Matched Pairs 
    apc_pairs['cmp_segid'] = apc_pairs['cmp_segid'].astype(int)
    apc_pairs['cur_stop_id'] = apc_pairs['cur_stop_id'].astype(int)
    apc_pairs['next_stop_id'] = apc_pairs['next_stop_id'].astype(int)
    
    # ### Manually Matched Pairs 
    pair_cnt = len(apc_pairs)
    for cur_stop_idx in range(len(overlap_pairs)):
        cur_stopid = overlap_pairs.loc[cur_stop_idx, 'pre_stopid']
        next_stopid = overlap_pairs.loc[cur_stop_idx, 'next_stopid']
        cur_stop_trips = apc_cmp.index[apc_cmp['BS_ID']==cur_stopid].tolist()
        for cur_stop_trip_idx in cur_stop_trips:
            if apc_cmp.loc[cur_stop_trip_idx + 1, 'BS_ID'] == next_stopid:
    
                cur_stop_trip_id = apc_cmp.loc[cur_stop_trip_idx, 'TRIP_ID_EXTERNAL']
                cur_stop_date = apc_cmp.loc[cur_stop_trip_idx, 'Date']
                cur_stop_veh_id = apc_cmp.loc[cur_stop_trip_idx, 'VEHICLE_ID']
                cur_stop_open_time = apc_cmp.loc[cur_stop_trip_idx, 'Open_Time']
                cur_stop_close_time = apc_cmp.loc[cur_stop_trip_idx, 'Close_Time']
                cur_stop_dwell_time = apc_cmp.loc[cur_stop_trip_idx, 'DWELL_TIME']
    
                next_stop_trip_id = apc_cmp.loc[cur_stop_trip_idx + 1, 'TRIP_ID_EXTERNAL']
                next_stop_date = apc_cmp.loc[cur_stop_trip_idx + 1, 'Date']
                next_stop_veh_id = apc_cmp.loc[cur_stop_trip_idx + 1, 'VEHICLE_ID']
                next_stop_open_time = apc_cmp.loc[cur_stop_trip_idx + 1, 'Open_Time']
                next_stop_close_time = apc_cmp.loc[cur_stop_trip_idx + 1, 'Close_Time']
                next_stop_dwell_time = apc_cmp.loc[cur_stop_trip_idx + 1, 'DWELL_TIME']
    
                # Check if two stops share the same trip id, date, and vehicle id
                if (cur_stop_trip_id == next_stop_trip_id) & (cur_stop_date == next_stop_date) & (cur_stop_veh_id == next_stop_veh_id):
                    cur_next_traveltime = (next_stop_open_time-cur_stop_open_time).total_seconds()
                    cur_next_loc_dis = overlap_pairs.loc[cur_stop_idx, 'cmp_overlap_len']/5280/overlap_pairs.loc[cur_stop_idx, 'cmp_overlap_ratio']
                    
                    # Remove records deemed erroneous according to memo from last cycle
                    if (cur_next_loc_dis>0) & (cur_next_traveltime>0):
                        if 3600 * cur_next_loc_dis/cur_next_traveltime<=55:
                            # Add matched stop pairs to the dataframe for subsequent speed calculation
                            apc_pairs.loc[pair_cnt, 'cmp_segid'] = overlap_pairs.loc[cur_stop_idx, 'cmp_segid']
                            apc_pairs.loc[pair_cnt, 'trip_id'] = cur_stop_trip_id
                            apc_pairs.loc[pair_cnt, 'trip_date'] = cur_stop_date
                            apc_pairs.loc[pair_cnt, 'vehicle_id'] = cur_stop_veh_id
    
                            apc_pairs.loc[pair_cnt, 'cur_stop_id'] = cur_stopid
                            apc_pairs.loc[pair_cnt, 'cur_stop_open_time'] = cur_stop_open_time
                            apc_pairs.loc[pair_cnt, 'cur_stop_close_time'] = cur_stop_close_time
                            apc_pairs.loc[pair_cnt, 'cur_stop_dwell_time'] = cur_stop_dwell_time
    
                            apc_pairs.loc[pair_cnt, 'next_stop_id'] = next_stopid
                            apc_pairs.loc[pair_cnt, 'next_stop_open_time'] = next_stop_open_time
                            apc_pairs.loc[pair_cnt, 'next_stop_close_time'] = next_stop_close_time
                            apc_pairs.loc[pair_cnt, 'next_stop_dwell_time'] = next_stop_dwell_time
                            
                            apc_pairs.loc[pair_cnt, 'cur_next_time'] = cur_next_traveltime
                            apc_pairs.loc[pair_cnt, 'cur_next_loc_dis'] = cur_next_loc_dis
                            apc_pairs.loc[pair_cnt, 'cur_next_rev_dis'] = apc_cmp.loc[cur_stop_trip_idx, 'REV_DISTANCE']
    
                        pair_cnt = pair_cnt + 1
                  
    apc_pairs['cur_next_loc_dis'] = np.where(apc_pairs['cur_next_loc_dis'] >= apc_pairs['cur_next_rev_dis'], 
                                            apc_pairs['cur_next_loc_dis'],
                                            apc_pairs['cur_next_rev_dis'])
    
    apc_pairs_clean = apc_pairs[apc_pairs['cur_stop_dwell_time']<180]
    apc_trip_speeds = apc_pairs_clean.groupby(['cmp_segid', 'trip_id', 'trip_date']).agg({'cur_next_loc_dis': 'sum',
                                                                        'cur_next_time': 'sum'}).reset_index()
    
    apc_trip_speeds.columns = ['cmp_segid', 'trip_id', 'trip_date', 'trip_stop_distance', 'trip_traveltime']
    apc_trip_speeds['trip_loc_speed'] = 3600* apc_trip_speeds['trip_stop_distance']/apc_trip_speeds['trip_traveltime']
    
    apc_trip_speeds = pd.merge(apc_trip_speeds, cmp_segs_prj[['cmp_segid', 'length']], on='cmp_segid', how='left')
    apc_trip_speeds['len_ratio'] = 100*apc_trip_speeds['trip_stop_distance']/apc_trip_speeds['length']
    
    #apc_trip_speeds.to_csv(os.path.join(APC_DIR, 'CMP2021_APC_Matched_Trips_%s.csv' %timep), index=False)
    
    # Only include trips covering at least 50% of CMP length
    apc_trip_speeds_over50 = apc_trip_speeds[apc_trip_speeds['len_ratio']>=50]
    apc_cmp_speeds = apc_trip_speeds_over50.groupby(['cmp_segid']).agg({'trip_loc_speed': ['mean', 'std'],
                                                                        'trip_id': 'count'}).reset_index()
    
    apc_cmp_speeds.columns = ['cmp_segid', 'avg_speed', 'std_dev', 'sample_size']
    apc_cmp_speeds['cov'] = 100* apc_cmp_speeds['std_dev']/apc_cmp_speeds['avg_speed']
    
    apc_cmp_speeds['year'] = 2021
    apc_cmp_speeds['source'] = 'APC'
    apc_cmp_speeds['period'] = timep
    
    return apc_cmp_speeds
    
# ## AM 
apc_cmp_speeds_am = match_intermediate_apc_stops(apc_pairs_am, apc_cmp_am, overlap_pairs, cmp_segs_prj, 'AM')

# ## PM 
apc_cmp_speeds_pm = match_intermediate_apc_stops(apc_pairs_pm, apc_cmp_pm, overlap_pairs, cmp_segs_prj, 'PM')

# ## Combine AM and PM 
apc_cmp_speeds = apc_cmp_speeds_am.append(apc_cmp_speeds_pm, ignore_index=True)
apc_cmp_speeds = apc_cmp_speeds[apc_cmp_speeds['sample_size']>=9]
print('Number of segment-periods ', len(apc_cmp_speeds))

out_cols = ['cmp_segid', 'year', 'source', 'period', 'avg_speed', 'std_dev', 'cov', 'sample_size']
apc_cmp_speeds[out_cols].to_csv(os.path.join(APC_DIR, 'CMP2021_APC_Transit_Speeds.csv'), index=False)
