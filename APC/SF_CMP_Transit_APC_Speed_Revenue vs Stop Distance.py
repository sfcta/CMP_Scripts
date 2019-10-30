# Import necessary packages
import pandas as pd
pd.options.display.max_columns = None
import numpy as np
import math
import geopandas as gp
from shapely.geometry import Point, mapping, LineString
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
from datetime import datetime
import warnings
import os
warnings.filterwarnings("ignore")

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}

#Define NAD 1983 StatePlane California III
cal3 = {'proj': 'lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002', 'ellps': 'GRS80', 'datum': 'NAD83', 'no_defs': True}

# UK Paths
# MAIN_DIR = 'S:/CMP/Transit/Speed'
# NETCONF_DIR = 'S:/CMP/Network Conflation'
# APC_FILE = 'S:/CMP/Transit/Speed/APC_2019_SPRING/APC_2019_SPRING.txt'

# SFCTA Paths
MAIN_DIR = r'Q:\CMP\LOS Monitoring 2019\Transit\Speed'
NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation'
APC_FILE = r'Q:\Data\Observed\Transit\Muni\APC\CMP_2019\APC_2019_SPRING.txt'


# Convert original coordinate system to state plane
stops = gp.read_file(os.path.join(MAIN_DIR, 'stops.shp'))
stops = stops.to_crs(cal3)
stops['stop_name'] = stops['stop_name'].str.lower()
stops[['street_1','street_2']] = stops.stop_name.str.split('&', expand=True)  #split stop name into operating street and intersecting street

# CMP network
cmp_segs_org=gp.read_file(os.path.join(NETCONF_DIR, 'cmp_roadway_segments.shp'))
cmp_segs_prj = cmp_segs_org.to_crs(cal3)
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.replace('/ ','/')
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.lower()
cmp_segs_prj['Length'] = cmp_segs_prj.geometry.length 
cmp_segs_prj['Length'] = cmp_segs_prj['Length'] * 3.2808  #meters to feet

# INRIX network
inrix_net=gp.read_file(os.path.join(NETCONF_DIR, 'inrix_xd_sf.shp'))
inrix_net['RoadName'] = inrix_net['RoadName'].str.lower()

cmp_inrix_corr = pd.read_csv(os.path.join(NETCONF_DIR, 'CMP_Segment_INRIX_Links_Correspondence.csv'))

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
            inrix_link_names = inrix_net[inrix_net['SegID'].isin(inrix_links['INRIX_SegID'])]['RoadName'].tolist()
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


# Preprocess APC data
apc_fields = ['EXT_TRIP_ID', 'DIRECTION', 'ACTUAL_DATE', 'VEHICLE_ID', 'CALC_SPEED', 
            'REV_DISTANCE', 'OPEN_DATE_TIME', 'DWELL_TIME', 'CLOSE_DATE_TIME', 'STOPID']
apc_cmp = pd.read_csv(APC_FILE, sep='\t', usecols=apc_fields)


apc_cmp['Date']=pd.to_datetime(apc_cmp['ACTUAL_DATE'])
apc_cmp['DOW']=apc_cmp.Date.dt.dayofweek #Tue-1, Wed-2, Thu-3
apc_cmp['Month']=apc_cmp.Date.dt.month
apc_cmp['Day']=apc_cmp.Date.dt.day

apc_cmp['Open_Hour'] = apc_cmp['OPEN_DATE_TIME'].str[10:12].astype(int)
apc_cmp['Open_Minute'] = apc_cmp['OPEN_DATE_TIME'].str[13:15].astype(int)
apc_cmp['Open_Second'] = apc_cmp['OPEN_DATE_TIME'].str[16:18].astype(int)
apc_cmp['Open_Period'] = apc_cmp['OPEN_DATE_TIME'].str[-2:]

apc_cmp['Close_Hour'] = apc_cmp['CLOSE_DATE_TIME'].str[10:12].astype(int)
apc_cmp['Close_Minute'] = apc_cmp['CLOSE_DATE_TIME'].str[13:15].astype(int)
apc_cmp['Close_Second'] = apc_cmp['CLOSE_DATE_TIME'].str[16:18].astype(int)
apc_cmp['Close_Period'] = apc_cmp['CLOSE_DATE_TIME'].str[-2:]

apc_cmp['Open_Time'] = apc_cmp['ACTUAL_DATE'] + ' ' + apc_cmp['Open_Hour'].astype('str') + ':' + apc_cmp['Open_Minute'].astype('str') + ':' + apc_cmp['Open_Second'].astype('str') + ' ' + apc_cmp['Open_Period']
apc_cmp['Open_Time'] = pd.to_datetime(apc_cmp['Open_Time'])

apc_cmp['Close_Time'] = apc_cmp['ACTUAL_DATE'] + ' ' + apc_cmp['Close_Hour'].astype('str') + ':' + apc_cmp['Close_Minute'].astype('str') + ':' + apc_cmp['Close_Second'].astype('str') + ' ' + apc_cmp['Close_Period']
apc_cmp['Close_Time'] = pd.to_datetime(apc_cmp['Close_Time'])

apc_cmp = apc_cmp[(apc_cmp['DOW'] ==1) | (apc_cmp['DOW'] ==2) | (apc_cmp['DOW'] ==3)]  # only Tue, Wed, and Thu

apc_cmp = apc_cmp[(apc_cmp['Month']==4) | ((apc_cmp['Month']==5) & (apc_cmp['Day'] < 15))] # CMP monitoring dates

apc_cmp['Open_Hour'] = apc_cmp.Open_Time.dt.hour
apc_cmp['Close_Hour'] = apc_cmp.Close_Time.dt.hour
apc_cmp['Open_Time_float'] = apc_cmp['Open_Hour'] + apc_cmp['Open_Minute']/60 + apc_cmp['Open_Second']/3600
apc_cmp['Close_Time_float'] = apc_cmp['Close_Hour'] + apc_cmp['Close_Minute']/60 + apc_cmp['Close_Second']/3600


# # Match AM&PM transit stops to CMP segments
angle_thrd = 10
def match_stop_pairs_to_cmp(apc_cmp_df, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd):
    apc_pairs_df = pd.DataFrame()
    pair_cnt = 0
    for cur_stop_idx in range(len(apc_cmp_df)-1):
        if apc_cmp_df.loc[cur_stop_idx, 'stop_id'] in stops_near_cmp_list:
            next_stop_match = 0
            if apc_cmp_df.loc[cur_stop_idx + 1, 'stop_id'] in stops_near_cmp_list:
                next_stop_idx = cur_stop_idx + 1 
                next_stop_match = 1
            elif apc_cmp_df.loc[cur_stop_idx + 2, 'stop_id'] in stops_near_cmp_list:
                next_stop_idx = cur_stop_idx +2
                next_stop_match = 2

            if next_stop_match >0:
                # Transit stop of interest
                cur_stop_trip_id = apc_cmp_df.loc[cur_stop_idx, 'EXT_TRIP_ID']
                cur_stop_date = apc_cmp_df.loc[cur_stop_idx, 'ACTUAL_DATE']
                cur_stop_veh_id = apc_cmp_df.loc[cur_stop_idx, 'VEHICLE_ID']

                # Succeeding candidate stop in the dataframe
                next_stop_trip_id = apc_cmp_df.loc[next_stop_idx, 'EXT_TRIP_ID']
                next_stop_date = apc_cmp_df.loc[next_stop_idx, 'ACTUAL_DATE']
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

        if cur_stop_idx % 10000 == 0:
            print('Processed %s percent' % round(100*cur_stop_idx/len(apc_cmp_df), 2))
    return apc_pairs_df


# ## AM 
apc_cmp_am = apc_cmp[(apc_cmp['Open_Hour']<9) & (apc_cmp['Close_Hour']>6)]

apc_cmp_am = apc_cmp_am.merge(stops, left_on='STOPID', right_on='stop_id', how='left')
apc_cmp_am = apc_cmp_am.sort_values(by=['EXT_TRIP_ID', 'ACTUAL_DATE', 'VEHICLE_ID', 'Open_Time']).reset_index()

print('------------Start processing AM trips------------')
apc_pairs_am = match_stop_pairs_to_cmp(apc_cmp_am, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd)
apc_pairs_am.to_csv(os.path.join(MAIN_DIR, 'APC_2019_Stop_Pairs_AM_update.csv'), index=False)

# ## PM 
apc_cmp_pm = apc_cmp[(apc_cmp['Open_Time_float']<=18.5) & (apc_cmp['Close_Time_float']>=16.5)]

apc_cmp_pm = apc_cmp_pm.merge(stops, left_on='STOPID', right_on='stop_id', how='left')
apc_cmp_pm = apc_cmp_pm.sort_values(by=['EXT_TRIP_ID', 'ACTUAL_DATE', 'VEHICLE_ID', 'Open_Time']).reset_index()

print('------------Start processing PM trips------------')
apc_pairs_pm = match_stop_pairs_to_cmp(apc_cmp_pm, stops_near_cmp_list, cmp_segs_near, cmp_segs_prj, angle_thrd)
apc_pairs_pm.to_csv(os.path.join(MAIN_DIR, 'APC_2019_Stop_Pairs_PM_update.csv'), index=False)


# ## Stop and Segment Matching 
stop_seg_am_1 = apc_pairs_am[['cur_stop_id', 'cur_stop_loc', 'cmp_segid']]
stop_seg_am_1.columns = ['stop_id', 'stop_loc', 'cmp_segid']
stop_seg_am_2 = apc_pairs_am[['next_stop_id', 'next_stop_loc', 'cmp_segid']]
stop_seg_am_2.columns = ['stop_id', 'stop_loc', 'cmp_segid']

stop_seg_am = stop_seg_am_1.append(stop_seg_am_2, ignore_index=False)

stop_seg_pm_1 = apc_pairs_pm[['cur_stop_id', 'cur_stop_loc', 'cmp_segid']]
stop_seg_pm_1.columns = ['stop_id', 'stop_loc', 'cmp_segid']
stop_seg_pm_2 = apc_pairs_pm[['next_stop_id', 'next_stop_loc', 'cmp_segid']]
stop_seg_pm_2.columns = ['stop_id', 'stop_loc', 'cmp_segid']

stop_seg_pm = stop_seg_pm_1.append(stop_seg_pm_2, ignore_index=False)

stop_seg_match = stop_seg_am.append(stop_seg_pm, ignore_index=False)
stop_seg_match = stop_seg_match.drop_duplicates(subset=['stop_id','cmp_segid']).reset_index()

stop_seg_match.to_csv(os.path.join(MAIN_DIR, 'transit_stop_cmp_segment_match.csv'), index=False)

stop_cmp_match = stops.merge(stop_seg_match, on='stop_id')
stop_cmp_match.to_crs(wgs84)
stop_cmp_match.to_file(os.path.join(MAIN_DIR, 'transit_stop_cmp_segment_match_update.shp'))

stop_cmp_match['cmp_segid'] = stop_cmp_match['cmp_segid'].astype(int)

matched_cmp_segs = stop_cmp_match['cmp_segid'].unique().tolist()


# # Transit Speeds on CMP Segment 

# ### Manually Matched Pairs 
overlap_pairs = pd.read_csv(os.path.join(MAIN_DIR, 'Postprocessing_overlapping_transit_segments.csv'))

def match_intermediate_apc_stops(apc_pairs, apc_cmp, timep):
    # ### Auto Matched Pairs 
    apc_pairs['cmp_segid'] = apc_pairs['cmp_segid'].astype(int)
    apc_pairs['cur_stop_id'] = apc_pairs['cur_stop_id'].astype(int)
    apc_pairs['next_stop_id'] = apc_pairs['next_stop_id'].astype(int)
    
    covered_stop_pairs = pd.DataFrame()
    cnt = 0
    apc_pairs_unique = apc_pairs.drop_duplicates(subset=['cmp_segid', 'cur_stop_id', 'next_stop_id']).reset_index()
    for cmp_segid in matched_cmp_segs:
        cmp_stops = stop_cmp_match[stop_cmp_match['cmp_segid']==cmp_segid].sort_values(by=['stop_loc']).reset_index()
        cmp_stop_pairs = apc_pairs_unique.index[apc_pairs_unique['cmp_segid']==cmp_segid].tolist()
        if len(cmp_stop_pairs)>0:
            for pair_idx in cmp_stop_pairs:
                cur_stopid = apc_pairs_unique.loc[pair_idx, 'cur_stop_id']
                cur_stop_idx_list = cmp_stops.index[cmp_stops['stop_id'] == cur_stopid].tolist()
                if len(cur_stop_idx_list)>1:
                    print('Duplicate cur stop id %s on CMP seg %s' % (cur_stopid, cmp_segid))
                cur_stop_seq_idx = cur_stop_idx_list[0]
                
                next_stopid = apc_pairs_unique.loc[pair_idx, 'next_stop_id']
                next_stop_idx_list = cmp_stops.index[cmp_stops['stop_id'] == next_stopid].tolist()
                if len(next_stop_idx_list)>1:
                    print('Duplicate next stop id %s on CMP seg %s' % (next_stopid, cmp_segid)) 
                next_stop_seq_idx = next_stop_idx_list[0]
                
                for mid_stop_seq_idx in range(cur_stop_seq_idx, next_stop_seq_idx):
                    covered_stop_pairs.loc[cnt, 'cmp_segid'] = cmp_segid
                    covered_stop_pairs.loc[cnt, 'cur_stop_id'] = cmp_stops.loc[mid_stop_seq_idx, 'stop_id']
                    covered_stop_pairs.loc[cnt, 'cur_stop_loc'] = cmp_stops.loc[mid_stop_seq_idx, 'stop_loc']
                    covered_stop_pairs.loc[cnt, 'next_stop_id'] = cmp_stops.loc[mid_stop_seq_idx + 1, 'stop_id']
                    covered_stop_pairs.loc[cnt, 'next_stop_loc'] = cmp_stops.loc[mid_stop_seq_idx + 1, 'stop_loc']
                    cnt = cnt + 1

    covered_stop_pairs_unique = covered_stop_pairs.drop_duplicates()
    covered_stop_pairs_unique['dist'] = abs(covered_stop_pairs_unique['next_stop_loc'] - covered_stop_pairs_unique['cur_stop_loc'])

    # ### Manually Matched Pairs 
    pair_cnt = len(apc_pairs)
    for cur_stop_idx in range(len(overlap_pairs)):
        cur_stopid = overlap_pairs.loc[cur_stop_idx, 'pre_stopid']
        next_stopid = overlap_pairs.loc[cur_stop_idx, 'next_stopid']
        cur_stop_trips = apc_cmp.index[apc_cmp['STOPID']==cur_stopid].tolist()
        for cur_stop_trip_idx in cur_stop_trips:
            if apc_cmp.loc[cur_stop_trip_idx + 1, 'STOPID'] == next_stopid:
    
                cur_stop_trip_id = apc_cmp.loc[cur_stop_trip_idx, 'EXT_TRIP_ID']
                cur_stop_date = apc_cmp.loc[cur_stop_trip_idx, 'ACTUAL_DATE']
                cur_stop_veh_id = apc_cmp.loc[cur_stop_trip_idx, 'VEHICLE_ID']
                cur_stop_open_time = apc_cmp.loc[cur_stop_trip_idx, 'Open_Time']
                cur_stop_close_time = apc_cmp.loc[cur_stop_trip_idx, 'Close_Time']
                cur_stop_dwell_time = apc_cmp.loc[cur_stop_trip_idx, 'DWELL_TIME']
    
                next_stop_trip_id = apc_cmp.loc[cur_stop_trip_idx + 1, 'EXT_TRIP_ID']
                next_stop_date = apc_cmp.loc[cur_stop_trip_idx + 1, 'ACTUAL_DATE']
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


    cnt = len(covered_stop_pairs_unique)
    for cur_stop_idx in range(len(overlap_pairs)):
        covered_stop_pairs_unique.loc[cnt, 'cmp_segid'] = overlap_pairs.loc[cur_stop_idx, 'cmp_segid']
        covered_stop_pairs_unique.loc[cnt, 'cur_stop_id'] = overlap_pairs.loc[cur_stop_idx, 'pre_stopid']
        covered_stop_pairs_unique.loc[cnt, 'next_stop_id'] = overlap_pairs.loc[cur_stop_idx, 'next_stopid']
        covered_stop_pairs_unique.loc[cnt, 'dist'] = overlap_pairs.loc[cur_stop_idx, 'cmp_overlap_len']
        cnt = cnt + 1
    
    covered_cmp_len = covered_stop_pairs_unique.groupby(['cmp_segid']).agg({'cur_stop_id': 'count', 'dist': 'sum'}).reset_index()
    covered_cmp_len.columns = ['cmp_segid', 'pair_cnt', 'pair_len']
    covered_cmp_len['cmp_segid'] = covered_cmp_len['cmp_segid'].astype(int)
    
    covered_cmp_len_ratio = pd.merge(covered_cmp_len, cmp_segs_prj, on='cmp_segid')
    covered_cmp_len_ratio['len_ratio'] = round(100 * covered_cmp_len_ratio['pair_len']/covered_cmp_len_ratio['Length'], 2)
    
    apc_pairs['speed_rev_dis'] = 3600* apc_pairs['cur_next_rev_dis']/apc_pairs['cur_next_time']
    apc_pairs['speed_loc_dis'] = 3600* apc_pairs['cur_next_loc_dis']/apc_pairs['cur_next_time']
    
    apc_pairs.to_csv(os.path.join(MAIN_DIR, 'APC_2019_Stop_Pairs_%s_update_manual.csv' %timep), index=False)
    
    apc_pairs_clean = apc_pairs[apc_pairs['cur_stop_dwell_time']<180]
    apc_cmp_speeds = apc_pairs_clean.groupby(['cmp_segid']).agg({'cur_next_loc_dis': 'sum',
                                                                       'cur_next_rev_dis': 'sum',
                                                                        'cur_next_time': 'sum',
                                                                       'speed_loc_dis': 'std',
                                                                       'speed_rev_dis': 'std',
                                                                        'trip_id': 'count'}).reset_index()
    apc_cmp_speeds.columns = ['cmp_segid', 'total_stop_distance', 'total_rev_distance',
                                 'total_traveltime', 'std_loc_speed','std_rev_speed', 'sample_size']
    apc_cmp_speeds['avg_loc_speed'] = 3600* apc_cmp_speeds['total_stop_distance']/apc_cmp_speeds['total_traveltime']
    apc_cmp_speeds['avg_rev_speed'] = 3600* apc_cmp_speeds['total_rev_distance']/apc_cmp_speeds['total_traveltime']
    apc_cmp_speeds['cov_loc_speed'] = 100* apc_cmp_speeds['std_loc_speed']/apc_cmp_speeds['avg_loc_speed']
    apc_cmp_speeds['cov_rev_speed'] = 100* apc_cmp_speeds['std_rev_speed']/apc_cmp_speeds['avg_rev_speed']
    
    apc_cmp_speeds = pd.merge(apc_cmp_speeds, covered_cmp_len_ratio[['cmp_segid', 'pair_len', 'Length', 'len_ratio']], on='cmp_segid', how='left')
    return apc_cmp_speeds

# ## AM 
apc_cmp_speeds_am = match_intermediate_apc_stops(apc_pairs_am, apc_cmp_am, 'AM')
# ## PM 
apc_cmp_speeds_pm = match_intermediate_apc_stops(apc_pairs_pm, apc_cmp_pm, 'PM')

# ## Combine AM and PM 
apc_cmp_speeds_am['period'] = 'AM'
apc_cmp_speeds_pm['period'] = 'PM'
apc_cmp_speeds = apc_cmp_speeds_am.append(apc_cmp_speeds_pm, ignore_index=True)

apc_cmp_speeds['len_ratio'] = np.where(apc_cmp_speeds['len_ratio']>100, 100, apc_cmp_speeds['len_ratio'])

apc_cmp_speeds.to_csv(os.path.join(MAIN_DIR, 'SF_CMP_Transit_Speeds_update.csv'), index=False)

cmp_segs_apc_am = cmp_segs_org.merge(apc_cmp_speeds_am, on='cmp_segid', how='left')
cmp_segs_apc_am['period'] = 'AM'
cmp_segs_apc_pm = cmp_segs_org.merge(apc_cmp_speeds_pm, on='cmp_segid', how='left')
cmp_segs_apc_pm['period'] = 'PM'
cmp_segs_apc = cmp_segs_apc_am.append(cmp_segs_apc_pm, ignore_index=True)
cmp_segs_apc.crs = wgs84
cmp_segs_apc.to_file(os.path.join(MAIN_DIR, 'SF_CMP_Transit_Speeds_Aug31.shp'))

apc_cmp_speeds_over50 = apc_cmp_speeds[apc_cmp_speeds['len_ratio']>=50]
print('Number of final segment-periods ', len(apc_cmp_speeds_over50))

out_cols = ['cmp_segid', 'period', 'avg_loc_speed', 'std_loc_speed', 'cov_loc_speed', 'avg_rev_speed', 'std_rev_speed', 'cov_rev_speed', 'len_ratio']
apc_cmp_speeds_over50[out_cols].to_csv(os.path.join(MAIN_DIR, 'SF_CMP_Transit_Speeds_Over50%_update.csv'), index=False)

# Generate sample size distribution graph
# Create sample size groups
apc_cmp_speeds_over50['group_id'] = np.where(apc_cmp_speeds_over50['sample_size']<50, 0,
                                          np.where(apc_cmp_speeds_over50['sample_size']<100, 1,
                                                   np.where(apc_cmp_speeds_over50['sample_size']<250, 2,
                                                            np.where(apc_cmp_speeds_over50['sample_size']<500, 3,
                                                                     np.where(apc_cmp_speeds_over50['sample_size']<1000, 4,
                                                                              np.where(apc_cmp_speeds_over50['sample_size']<2000, 5,
                                                                                       6
                                         ))))))
apc_cmp_speeds_over50['group_range'] = np.where(apc_cmp_speeds_over50['sample_size']<50, '0-50',
                                          np.where(apc_cmp_speeds_over50['sample_size']<100, '50-100',
                                                   np.where(apc_cmp_speeds_over50['sample_size']<250, '100-250',
                                                            np.where(apc_cmp_speeds_over50['sample_size']<500, '250-500',
                                                                     np.where(apc_cmp_speeds_over50['sample_size']<1000, '500-1000',
                                                                              np.where(apc_cmp_speeds_over50['sample_size']<2000, '1000-2000',
                                                                                       '>2000'
                                         ))))))
apc_cmp_count = apc_cmp_speeds_over50.groupby(['group_id', 'group_range', 'period']).cmp_segid.count().reset_index()
apc_cmp_count.columns = ['group_id', 'group_range', 'period', 'count']

# Generate graph
import matplotlib.pyplot as plt
n_groups = 7
fig, ax = plt.subplots(figsize=(10,5))
bar_width = 0.35
opacity = 0.8

#Create bars
am_bvalue = apc_cmp_count[apc_cmp_count['period']=='AM']['count'].tolist()
pm_bvalue = apc_cmp_count[apc_cmp_count['period']=='PM']['count'].tolist()

# Get x position of bars
am_bx = apc_cmp_count[apc_cmp_count['period']=='AM']['group_id'].tolist()
pm_bx = [pm_x + bar_width for pm_x in apc_cmp_count[apc_cmp_count['period']=='PM']['group_id'].tolist()]

am = plt.bar(am_bx, am_bvalue, bar_width, alpha=opacity, color='blue', label='AM')
pm = plt.bar(pm_bx, pm_bvalue, bar_width, alpha=opacity, color='orange', label='PM')

plt.xticks([grp + bar_width-0.18 for grp in range(n_groups)], ['0-50', '50-100', '100-250', '250-500', '500-1000', '1000-2000', '>2000'])
ax.tick_params(axis='both', which='major', labelsize=12)

plt.xlabel('Number of APC Records', fontsize = 14)
plt.ylabel('Number of CMP Segments', fontsize = 14)
plt.legend(fontsize=12)

# Create bar value labels
label = am_bvalue + pm_bvalue
bx = am_bx + pm_bx
# Text on the top of each bar
for i in range(len(bx)):
    plt.text(x = bx[i] - 0.1 , y = label[i] + 1, s = label[i], size = 10)

ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)

plt.tight_layout()
plt.savefig(os.path.join(MAIN_DIR, 'Sample_Size_Distribution.png'), bbox_inches = 'tight')
plt.show()
