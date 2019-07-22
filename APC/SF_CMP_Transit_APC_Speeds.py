import pandas as pd
pd.options.display.max_columns = None
import numpy as np
import math
import geopandas as gp
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
from datetime import datetime
import time
import warnings
warnings.filterwarnings("ignore")

directory = 'S:/CMP/Transit/Speed/'

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}

#Define NAD 1983 StatePlane California III
cal3 = {'proj': 'lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002', 'ellps': 'GRS80', 'datum': 'NAD83', 'no_defs': True}

# Convert original coordinate system to state plane
stops = gp.read_file(directory + 'stops.shp')
stops = stops.to_crs(cal3)
stops['stop_name'] = stops['stop_name'].str.lower()
stops[['street_1','street_2']] = stops.stop_name.str.split('&', expand=True)  #split stop name into operating street and intersecting street

# CMP network
cmp_segs_org=gp.read_file('S:/CMP/Network Conflation/cmp_roadway_segments.shp')
cmp_segs_prj = cmp_segs_org.to_crs(cal3)
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.replace('/ ','/')
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.lower()
cmp_segs_prj['Length'] = cmp_segs_prj.geometry.length 
cmp_segs_prj['Length'] = cmp_segs_prj['Length'] * 3.2808  #meters to feet

# INRIX network
inrix_net=gp.read_file('S:/CMP/Network Conflation/inrix_xd_sf.shp')
inrix_net['RoadName'] = inrix_net['RoadName'].str.lower()

# CMP-INRIX correspondence table
cmp_inrix_corr = pd.read_csv('S:/CMP/Network Conflation/CMP_Segment_INRIX_Links_Correspondence.csv')


# # Find transit stops near cmp segments
# Create a buffer zone for each cmp segment
ft=100
mt=round(ft/3.2808,4)
stops_buffer=stops.copy()
stops_buffer['geometry'] = stops_buffer.geometry.buffer(mt)
#stops_buffer.to_file(directory + 'stops_buffer.shp')

# cmp segments intersecting transit stop buffer zone
cmp_segs_intersect=gp.sjoin(cmp_segs_prj, stops_buffer, op='intersects').reset_index()

# Look for stops near cmp segments
stops['near_cmp'] = 0
cmp_segs_intersect['name_match']=0
for stop_idx in range(len(stops)):
    stop_id = stops.loc[stop_idx, 'stop_id']
    stop_geo = stops.loc[stop_idx]['geometry']
    stop_names = stops.loc[stop_idx]['street_1'].split('/')
    
    cmp_segs_int = cmp_segs_intersect[cmp_segs_intersect['stop_id']==stop_id]
    cmp_segs_idx = cmp_segs_intersect.index[cmp_segs_intersect['stop_id']==stop_id].tolist()
    
    near_dis = 5000
    if ~cmp_segs_int.empty:
        for seg_idx in cmp_segs_idx:
            cmp_seg_id = cmp_segs_int.loc[seg_idx, 'cmp_segid']
            cmp_seg_geo = cmp_segs_int.loc[seg_idx]['geometry']
            cmp_seg_names = cmp_segs_int.loc[seg_idx]['cmp_name'].split('/')
            
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
#stops_near_cmp.to_file(directory + 'stops_near_cmp_updated.shp')

cmp_segs_near = cmp_segs_intersect[cmp_segs_intersect['name_match']==1]



# # Preprocess APC data
apc_fields = ['EXT_TRIP_ID', 'DIRECTION', 'ACTUAL_DATE', 'VEHICLE_ID', 'CALC_SPEED', 
            'REV_DISTANCE', 'OPEN_DATE_TIME', 'DWELL_TIME', 'CLOSE_DATE_TIME', 'STOPID']
apc = dd.read_csv(directory + 'APC_2019_SPRING/APC_2019_SPRING.txt', sep='\t', usecols=apc_fields)

apc_cmp = apc[apc['STOPID'].isin(stops_near_cmp['stop_id'])].compute()

# Add date and time information
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



# # Match AM&PM transit stops to CMP segments
apc_cmp = apc_cmp[(apc_cmp['DOW'] ==1) | (apc_cmp['DOW'] ==2) | (apc_cmp['DOW'] ==3)]  # only Tue, Wed, and Thu
apc_cmp = apc_cmp[(apc_cmp['Month']==4) | ((apc_cmp['Month']==5) & (apc_cmp['Day'] < 15))] # CMP monitoring dates

apc_cmp['Open_Hour'] = apc_cmp.Open_Time.dt.hour
apc_cmp['Close_Hour'] = apc_cmp.Close_Time.dt.hour

apc_cmp_am = apc_cmp[(apc_cmp['Open_Hour']<9) & (apc_cmp['Close_Hour']>6)]
apc_cmp['Open_Time_float'] = apc_cmp['Open_Hour'] + apc_cmp['Open_Minute']/60 + apc_cmp['Open_Second']/3600
apc_cmp['Close_Time_float'] = apc_cmp['Close_Hour'] + apc_cmp['Close_Minute']/60 + apc_cmp['Close_Second']/3600
apc_cmp_pm = apc_cmp[(apc_cmp['Open_Hour']<=18.5) & (apc_cmp['Close_Hour']>=16.5)]


# ## AM 
apc_cmp_am = apc_cmp_am.merge(stops, left_on='STOPID', right_on='stop_id', how='left')
apc_cmp_am = apc_cmp_am.sort_values(by=['EXT_TRIP_ID', 'ACTUAL_DATE', 'VEHICLE_ID', 'Close_Time']).reset_index()
angle_thrd = 10
dwell_time_thrd = 20
for trip_stop_idx in range(len(apc_cmp_am)-1):
    cur_stop_trip_id = apc_cmp_am.loc[trip_stop_idx, 'EXT_TRIP_ID']
    cur_stop_date = apc_cmp_am.loc[trip_stop_idx, 'ACTUAL_DATE']
    cur_stop_veh_id = apc_cmp_am.loc[trip_stop_idx, 'VEHICLE_ID']
    cur_stop_close_time = apc_cmp_am.loc[trip_stop_idx, 'Close_Time']
    
    next_stop_trip_id = apc_cmp_am.loc[trip_stop_idx + 1, 'EXT_TRIP_ID']
    next_stop_date = apc_cmp_am.loc[trip_stop_idx + 1, 'ACTUAL_DATE']
    next_stop_veh_id = apc_cmp_am.loc[trip_stop_idx + 1, 'VEHICLE_ID']
    next_stop_open_time = apc_cmp_am.loc[trip_stop_idx + 1, 'Open_Time']
    next_stop_close_time = apc_cmp_am.loc[trip_stop_idx + 1, 'Close_Time']
    next_stop_dwell_time = apc_cmp_am.loc[trip_stop_idx + 1, 'DWELL_TIME']
    
    stop_run_time = (next_stop_open_time-cur_stop_close_time).total_seconds()
    if (cur_stop_trip_id == next_stop_trip_id) & (cur_stop_date == next_stop_date) & (cur_stop_veh_id == next_stop_veh_id):
        cur_stop_id = apc_cmp_am.loc[trip_stop_idx, 'stop_id']
        next_stop_id = apc_cmp_am.loc[trip_stop_idx + 1, 'stop_id']
        
        cur_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==cur_stop_id]['cmp_segid'])
        next_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==next_stop_id]['cmp_segid'])
        
        common_segs = list(set(cur_stop_near_segs) & set(next_stop_near_segs))
        if len(common_segs)>0:
            cur_stop_geo = apc_cmp_am.loc[trip_stop_idx]['geometry']
            next_stop_geo = apc_cmp_am.loc[trip_stop_idx+1]['geometry']
            stop_degree=(180 / math.pi) * math.atan2(next_stop_geo.x - cur_stop_geo.x, next_stop_geo.y - cur_stop_geo.y)

            for common_seg_id in common_segs:
                common_seg_geo = cmp_segs_prj[cmp_segs_prj['cmp_segid']==common_seg_id]['geometry']
                
                cur_stop_dis = common_seg_geo.project(cur_stop_geo)
                cur_stop_projected = common_seg_geo.interpolate(cur_stop_dis) #projected current stop on cmp segment

                next_stop_dis = common_seg_geo.project(next_stop_geo)
                next_stop_projected = common_seg_geo.interpolate(next_stop_dis) #projected next stop on cmp segme
                
                # Ensure the degree is calculated following the traveling direction
                if float(next_stop_dis) > float(cur_stop_dis):
                    cmp_degree=(180 / math.pi) * math.atan2(next_stop_projected.x - cur_stop_projected.x, next_stop_projected.y - cur_stop_projected.y)
                else:
                    cmp_degree=(180 / math.pi) * math.atan2(cur_stop_projected.x - next_stop_projected.x, cur_stop_projected.y - next_stop_projected.y)
                
                stop_cmp_angle = abs(cmp_degree-stop_degree)
                if stop_cmp_angle>270:
                    stop_cmp_angle=360-stop_cmp_angle
                    
                if stop_cmp_angle < angle_thrd:
                    matched_segid = common_seg_id
                    apc_cmp_am.loc[trip_stop_idx, 'matched_segid'] = common_seg_id
                    apc_cmp_am.loc[trip_stop_idx + 1, 'matched_segid'] = common_seg_id
                    if next_stop_dwell_time < dwell_time_thrd:
                        apc_cmp_am.loc[trip_stop_idx, 'travel_time'] = (next_stop_close_time-cur_stop_close_time).total_seconds()
                    else:
                        apc_cmp_am.loc[trip_stop_idx, 'travel_time'] = stop_run_time

#apc_cmp_am.to_csv('S:/CMP/Transit/Speed/APC_2019_Near_CMP_AM.csv', index=False)


# ## PM 
apc_cmp_pm = apc_cmp_pm.merge(stops, left_on='STOPID', right_on='stop_id', how='left')
apc_cmp_pm = apc_cmp_pm.sort_values(by=['EXT_TRIP_ID', 'ACTUAL_DATE', 'VEHICLE_ID', 'Close_Time']).reset_index()

for trip_stop_idx in range(len(apc_cmp_pm)-1):
    cur_stop_trip_id = apc_cmp_pm.loc[trip_stop_idx, 'EXT_TRIP_ID']
    cur_stop_date = apc_cmp_pm.loc[trip_stop_idx, 'ACTUAL_DATE']
    cur_stop_veh_id = apc_cmp_pm.loc[trip_stop_idx, 'VEHICLE_ID']
    cur_stop_close_time = apc_cmp_pm.loc[trip_stop_idx, 'Close_Time']
    
    next_stop_trip_id = apc_cmp_pm.loc[trip_stop_idx + 1, 'EXT_TRIP_ID']
    next_stop_date = apc_cmp_pm.loc[trip_stop_idx + 1, 'ACTUAL_DATE']
    next_stop_veh_id = apc_cmp_pm.loc[trip_stop_idx + 1, 'VEHICLE_ID']
    next_stop_open_time = apc_cmp_pm.loc[trip_stop_idx + 1, 'Open_Time']
    next_stop_close_time = apc_cmp_pm.loc[trip_stop_idx + 1, 'Close_Time']
    next_stop_dwell_time = apc_cmp_pm.loc[trip_stop_idx + 1, 'DWELL_TIME']
    
    stop_run_time = (next_stop_open_time-cur_stop_close_time).total_seconds()
    
    if (cur_stop_trip_id == next_stop_trip_id) & (cur_stop_date == next_stop_date) & (cur_stop_veh_id == next_stop_veh_id):
        cur_stop_id = apc_cmp_pm.loc[trip_stop_idx, 'stop_id']
        next_stop_id = apc_cmp_pm.loc[trip_stop_idx + 1, 'stop_id']
        
        cur_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==cur_stop_id]['cmp_segid'])
        next_stop_near_segs = list(cmp_segs_near[cmp_segs_near['stop_id']==next_stop_id]['cmp_segid'])
        
        common_segs = list(set(cur_stop_near_segs) & set(next_stop_near_segs))
        if len(common_segs)>0:
            cur_stop_geo = apc_cmp_pm.loc[trip_stop_idx]['geometry']
            next_stop_geo = apc_cmp_pm.loc[trip_stop_idx+1]['geometry']
            stop_degree=(180 / math.pi) * math.atan2(next_stop_geo.x - cur_stop_geo.x, next_stop_geo.y - cur_stop_geo.y)

            for common_seg_id in common_segs:
                common_seg_geo = cmp_segs_prj[cmp_segs_prj['cmp_segid']==common_seg_id]['geometry']
                
                cur_stop_dis = common_seg_geo.project(cur_stop_geo)
                cur_stop_projected = common_seg_geo.interpolate(cur_stop_dis) #projected current stop on cmp segment

                next_stop_dis = common_seg_geo.project(next_stop_geo)
                next_stop_projected = common_seg_geo.interpolate(next_stop_dis) #projected next stop on cmp segme
                
                # Ensure the degree is calculated following the traveling direction
                if float(next_stop_dis) > float(cur_stop_dis):
                    cmp_degree=(180 / math.pi) * math.atan2(next_stop_projected.x - cur_stop_projected.x, next_stop_projected.y - cur_stop_projected.y)
                else:
                    cmp_degree=(180 / math.pi) * math.atan2(cur_stop_projected.x - next_stop_projected.x, cur_stop_projected.y - next_stop_projected.y)
                
                stop_cmp_angle = abs(cmp_degree-stop_degree)
                if stop_cmp_angle>270:
                    stop_cmp_angle=360-stop_cmp_angle
                    
                if stop_cmp_angle < angle_thrd:
                    matched_segid = common_seg_id
                    apc_cmp_pm.loc[trip_stop_idx, 'matched_segid'] = common_seg_id
                    apc_cmp_pm.loc[trip_stop_idx + 1, 'matched_segid'] = common_seg_id
                    if next_stop_dwell_time < 20:
                        apc_cmp_pm.loc[trip_stop_idx, 'travel_time'] = (next_stop_close_time-cur_stop_close_time).total_seconds()
                    else:
                        apc_cmp_pm.loc[trip_stop_idx, 'travel_time'] = stop_run_time

apc_cmp_pm.to_csv('S:/CMP/Transit/Speed/APC_2019_Near_CMP_PM.csv', index=False)


# ## Stop and Segment Matching 
stop_seg_am = apc_cmp_am[apc_cmp_am['matched_segid']>0][['stop_id','matched_segid']]
stop_seg_am = stop_seg_am.drop_duplicates(subset=['stop_id','matched_segid']).reset_index()
stop_seg_am['period'] = 'AM'

stop_seg_pm = apc_cmp_pm[apc_cmp_pm['matched_segid']>0][['stop_id','matched_segid']]
stop_seg_pm = stop_seg_pm.drop_duplicates(subset=['stop_id','matched_segid']).reset_index()
stop_seg_pm['period'] = 'PM'

stop_seg_match = stop_seg_am.append(stop_seg_pm, ignore_index=False)
stop_seg_match = stop_seg_match.drop_duplicates(subset=['stop_id','matched_segid']).reset_index()

# Output transit stops matching file
stop_cmp_match = stops.merge(stop_seg_match, on='stop_id')
stop_cmp_match.to_crs(wgs84)
stop_cmp_match.to_file(directory + 'transit_stop_cmp_segment_match.shp')