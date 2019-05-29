'''
This script is developed to automatically establish the corresponding relationship between CMP segments and INRIX XD network links.
It finds INRIX links that are near each CMP segment and decides the matching links based on street name, angle and distance attributes.

INPUTS:
1. cmp_roadway_segments.shp, the CMP segment shapefile provided by SFCTA.
2. inrix_xd_sf.shp, the INRIX XD network shapefile provided by INRIX. Because the originial shapefile provided is for the whole
state of California, only roadways in San Francisco County are selected and exported as inrix_xd_sf.shp.

OUTPUTS:
1. cmp_roadway_segments_matchedlength_check.shp, a shapefile generated for quality check purpose. The field "Len_Ratio" indicates the 
ratio of the total length of matched INRIX links over the length of the CMP segment. Special attention should should be given to 
segments whose Len_Ratio is below 95% or over 100%. 
2. CMP_Segment_INRIX_Links_Correspondence.csv, a csv file containing the correspondence between CMP segments and INRIX XD links. The
field "Length_Matched" indicates the length of an INRIX link matched to a CMP segment. Note that the table is not final as quality 
assurance needs to be performed on the conflation results and necessary changes could be made. Please see 
Network Conflation Process Memo.docx for more detailed discussions.

USAGE:
The script should be placed under the same folder with input files. Use python SF_CMP_INRIX_Network_Conflation.py to run the script.
'''


# Specify input file names
# INRIX XD network
inrix_sf = 'inrix_xd_sf'
# CMP network
cmp = 'cmp_roadway_segments'


# Import needed python modules
import fiona
import fiona.crs
from shapely.geometry import Point, LineString, mapping
import geopandas as gp
import pandas as pd
import numpy as np
import math
import warnings
warnings.filterwarnings("ignore")

#Define WGS 1984 coordinate system
wgs84 = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}
#Define NAD 1983 StatePlane California III
cal3 = {'proj': 'lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002', 'ellps': 'GRS80', 'datum': 'NAD83', 'no_defs': True}

# Convert original coordinate system to California III state plane
# CMP network
cmp_segs_org=gp.read_file(cmp + '.shp')
cmp_segs_prj = cmp_segs_org.to_crs(cal3)
cmp_segs_prj['cmp_name'] = cmp_segs_prj['cmp_name'].str.replace('/ ','/')
cmp_segs_prj['Length'] = cmp_segs_prj.geometry.length 
cmp_segs_prj['Length'] = cmp_segs_prj['Length'] * 3.2808  #meters to feet
cmp_segs_prj.to_file(cmp + '_prj.shp')

# INRIX XD network
inrix_net_org=gp.read_file(inrix_sf + '.shp')
inrix_net_prj = inrix_net_org.to_crs(cal3)
inrix_net_prj['Length'] = inrix_net_prj.geometry.length 
inrix_net_prj['Length'] = inrix_net_prj['Length'] * 3.2808  #meters to feet
inrix_net_prj.to_file(inrix_sf + '_prj.shp')


# # Get endpoints of INRIX links
# Attributes to be included
vschema =  {'geometry': 'Point',
             'properties': {'SegID': 'int',
                            'RoadName': 'str',
                            'type': 'str',
                            'Latitude': 'float',
                            'Longitude': 'float'}}
#Input file
inrixin = inrix_sf+'_prj.shp'
#Output file
inrixout = inrix_sf + '_prj_endpoints.shp'
with fiona.open(inrixout, mode='w', crs=cal3, driver='ESRI Shapefile', schema=vschema) as output:
    layer= fiona.open(inrixin)
    for line in layer:
        vertices=line['geometry']['coordinates']
        v_begin=vertices[0]
        if isinstance(v_begin, list):
            v_begin=vertices[0][0]
            point = Point(float(v_begin[0]), float(v_begin[1]))
            prop = {'SegID': int(line['properties']['SegID']),
                    'RoadName': line['properties']['RoadName'],
                    'type': 'begin', 
                    'Latitude': float(v_begin[1]), 'Longitude': float(v_begin[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
        else:
            point = Point(float(v_begin[0]), float(v_begin[1]))
            prop = {'SegID': int(line['properties']['SegID']),
                    'RoadName': line['properties']['RoadName'],
                    'type': 'begin', 
                    'Latitude': float(v_begin[1]), 'Longitude': float(v_begin[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
        
        v_end=vertices[-1]
        if isinstance(v_end, list):
            v_end=vertices[-1][-1]
            point = Point(float(v_end[0]), float(v_end[1]))
            prop = {'SegID': int(line['properties']['SegID']),
                    'RoadName': line['properties']['RoadName'],
                    'type': 'end', 
                    'Latitude': float(v_end[1]), 'Longitude': float(v_end[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
            
        else:
            point = Point(float(v_end[0]), float(v_end[1]))
            prop = {'SegID': int(line['properties']['SegID']),
                    'RoadName': line['properties']['RoadName'],
                    'type': 'end', 
                    'Latitude': float(v_end[1]), 'Longitude': float(v_end[0])}
            output.write({'geometry': mapping(point), 'properties':prop})


# Read in the created inrix endpoints file
endpoints=gp.read_file(inrix_sf + '_prj_endpoints.shp')

# Assign unique node id
endpoints_cnt=endpoints.groupby(['Latitude', 'Longitude']).SegID.count().reset_index()
endpoints_cnt.rename(columns={'SegID':'NodeCnt'}, inplace=True)
endpoints_cnt['NodeID'] = endpoints_cnt.index+1

# Generate the the unique node shapefile  
endpoints_cnt['Coordinates'] = list(zip(endpoints_cnt.Longitude, endpoints_cnt.Latitude))
endpoints_cnt['Coordinates'] = endpoints_cnt['Coordinates'].apply(Point)
endpoints_cnt_gpd = gp.GeoDataFrame(endpoints_cnt, geometry='Coordinates')
endpoints_cnt_gpd.to_file(directory + "INRIX_HERE_SF_Endpoints_unique.shp")

endpoints=endpoints.merge(endpoints_cnt, on=['Latitude', 'Longitude'], how='left')

# Attached the endpoint information to the link network
endpoints_begin=endpoints[endpoints['type']=='begin'][['SegID', 'Latitude', 'Longitude', 'NodeID']]
endpoints_begin.columns=['SegID', 'B_Lat', 'B_Long', 'B_NodeID']

endpoints_end=endpoints[endpoints['type']=='end'][['SegID', 'Latitude', 'Longitude', 'NodeID']]
endpoints_end.columns=['SegID', 'E_Lat', 'E_Long', 'E_NodeID']

inrix_net_prj=inrix_net_prj.merge(endpoints_begin, on='SegID')
inrix_net_prj=inrix_net_prj.merge(endpoints_end, on='SegID')


# # Get endnodes of CMP segments
# Attributes to be included
cmp_schema =  {'geometry': 'Point',
             'properties': {'cmp_segid': 'int',
                            'cmp_name': 'str',
                            'type': 'str',
                            'NodeID': 'int',
                            'Latitude': 'float',
                            'Longitude': 'float'}}
node_cnt=0
#Input file
cmpin = cmp + '_prj.shp'

#Output file
cmpout = cmp + '_prj_endpoints.shp'
with fiona.open(cmpout, mode='w', crs=cal3, driver='ESRI Shapefile', schema=cmp_schema) as output:
    layer= fiona.open(cmpin)
    for line in layer:
        vertices=line['geometry']['coordinates']
        
        node_cnt = node_cnt + 1
        v_begin=vertices[0]
        if isinstance(v_begin, list):
            v_begin=vertices[0][0]
            point = Point(float(v_begin[0]), float(v_begin[1]))
            prop = {'cmp_segid': int(line['properties']['cmp_segid']),
                    'cmp_name': line['properties']['cmp_name'],
                    'NodeID': node_cnt, 'type': 'begin', 
                    'Latitude': float(v_begin[1]), 'Longitude': float(v_begin[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
        else:
            point = Point(float(v_begin[0]), float(v_begin[1]))
            prop = {'cmp_segid': int(line['properties']['cmp_segid']),
                    'cmp_name': line['properties']['cmp_name'],
                    'NodeID': node_cnt, 'type': 'begin', 
                    'Latitude': float(v_begin[1]), 'Longitude': float(v_begin[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
        
        node_cnt = node_cnt + 1
        v_end=vertices[-1]
        if isinstance(v_end, list):
            v_end=vertices[-1][-1]
            point = Point(float(v_end[0]), float(v_end[1]))
            prop = {'cmp_segid': int(line['properties']['cmp_segid']),
                    'cmp_name': line['properties']['cmp_name'],
                    'NodeID': node_cnt, 'type': 'end', 
                    'Latitude': float(v_end[1]), 'Longitude': float(v_end[0])}
            output.write({'geometry': mapping(point), 'properties':prop})
            
        else:
            point = Point(float(v_end[0]), float(v_end[1]))
            prop = {'cmp_segid': int(line['properties']['cmp_segid']),
                    'cmp_name': line['properties']['cmp_name'],
                    'NodeID': node_cnt, 'type': 'end', 
                    'Latitude': float(v_end[1]), 'Longitude': float(v_end[0])}
            output.write({'geometry': mapping(point), 'properties':prop})


# Read in the created cmp endpoints
cmp_endpoints=gp.read_file(cmp + '_prj_endpoints.shp')

cmp_endpoint_b=cmp_endpoints[cmp_endpoints['type']=='begin'][['cmp_segid','Latitude','Longitude', 'NodeID']]
cmp_endpoint_b.columns=['cmp_segid','B_Lat','B_Long', 'CMP_Node_B']
cmp_endpoint_e=cmp_endpoints[cmp_endpoints['type']=='end'][['cmp_segid','Latitude','Longitude', 'NodeID']]
cmp_endpoint_e.columns=['cmp_segid','E_Lat','E_Long', 'CMP_Node_E']

cmp_segs_prj=cmp_segs_prj.merge(cmp_endpoint_b, on='cmp_segid', how='left')
cmp_segs_prj=cmp_segs_prj.merge(cmp_endpoint_e, on='cmp_segid', how='left')

# Calculate the bearing of cmp segments
cmp_segs_prj['Degree']=cmp_segs_prj.apply(lambda x: (180 / math.pi) * math.atan2(x['E_Long'] - x['B_Long'], x['E_Lat'] - x['B_Lat']), axis=1)


# # Network Conflation
# ## Get nearby INRIX links 
# Create a buffer zone for each cmp segment
ft=150  # 150 ft distance
mt=round(ft/3.2808,4)
cmp_segs_buffer=cmp_segs_prj.copy()
cmp_segs_buffer['geometry'] = np.where(cmp_segs_buffer['cmp_segid'] == 81,
                                       cmp_segs_buffer.geometry.buffer(mt * 2),
                                       cmp_segs_buffer.geometry.buffer(mt))
cmp_segs_buffer.to_file(cmp + '_prj_buffer.shp')

# INRIX lines intersecting cmp segment buffer zone
inrix_lines_intersect=gp.sjoin(inrix_net_prj, cmp_segs_buffer, op='intersects').reset_index()

# INRIX lines within cmp segment buffer zone
inrix_lines_within=gp.sjoin(inrix_net_prj, cmp_segs_buffer, op='within').reset_index()


# ## Determine matching INRIX links within each CMP buffer
len_ratio_thrhd=0.8
angle_thrhd=15
for cmp_seg_idx in range(len(cmp_segs_prj)):
    cmp_seg_id = cmp_segs_prj.loc[cmp_seg_idx, 'cmp_segid']
    cmp_seg_geo = cmp_segs_prj.loc[cmp_seg_idx]['geometry']
    cmp_seg_len = cmp_segs_prj.loc[cmp_seg_idx]['Length']
    cmp_b_lat = cmp_segs_prj.loc[cmp_seg_idx]['B_Lat']
    cmp_b_long = cmp_segs_prj.loc[cmp_seg_idx]['B_Long']
    cmp_e_lat = cmp_segs_prj.loc[cmp_seg_idx]['E_Lat']
    cmp_e_long = cmp_segs_prj.loc[cmp_seg_idx]['E_Long']
    cmp_names = cmp_segs_prj.loc[cmp_seg_idx]['cmp_name'].lower().split('/')  #Street name
    if cmp_segs_prj.loc[cmp_seg_idx]['cmp_name']=='Doyle/Lombard/Richardson':
        cmp_names = cmp_names + ['us-101']
    if cmp_segs_prj.loc[cmp_seg_idx]['cmp_name']=='Duboce/Division':
        cmp_names = cmp_names + ['13th st']
    if (cmp_seg_id==147) or (cmp_seg_id==148):
        cmp_names = cmp_names + ['kennedy']
    if (cmp_seg_id==26) or (cmp_seg_id==27):
        cmp_names = cmp_names + ['veterans']  
    inrix_lns_within = inrix_lines_within[inrix_lines_within['cmp_segid']==cmp_seg_id]
    inrix_lns_idx = inrix_lines_within.index[inrix_lines_within['cmp_segid']==cmp_seg_id].tolist()
    if ~inrix_lns_within.empty:
        for ln_idx in inrix_lns_idx:
            inrix_b_lat = inrix_lines_within.loc[ln_idx]['B_Lat_left']
            inrix_b_long = inrix_lines_within.loc[ln_idx]['B_Long_left']
            inrix_e_lat = inrix_lines_within.loc[ln_idx]['E_Lat_left']
            inrix_e_long = inrix_lines_within.loc[ln_idx]['E_Long_left']
            inrix_geo = inrix_lines_within.loc[ln_idx]['geometry']
            inrix_len = inrix_lines_within.loc[ln_idx]['Miles'] * 5280 # miles to feet
            
            if pd.notnull(inrix_lines_within.loc[ln_idx]['RoadName']):
                inrix_roadname = inrix_lines_within.loc[ln_idx]['RoadName'].lower().split('/')  #Street name
            else:
                inrix_roadname = []
            if pd.notnull(inrix_lines_within.loc[ln_idx]['RoadNumber']):
                inrix_roadnumber = inrix_lines_within.loc[ln_idx]['RoadNumber'].lower().split('/')  
                inrix_roadname = inrix_roadname + inrix_roadnumber
            if pd.notnull(inrix_lines_within.loc[ln_idx]['RoadList']):
                inrix_roadlist = inrix_lines_within.loc[ln_idx]['RoadList'].lower().split('/')
                inrix_roadname = inrix_roadname + inrix_roadlist
            inrix_roadname = list(set(inrix_roadname))
            if inrix_roadname:
                matched_names= [iname for iname in inrix_roadname if any(cname in iname for cname in cmp_names)]
                if len(matched_names)>0:
                    inrix_lines_within.loc[ln_idx, 'namematch']=1
                else:
                    inrix_lines_within.loc[ln_idx, 'namematch']=0
            else:
                inrix_lines_within.loc[ln_idx, 'namematch']=0
                        
            inrix_b_point = Point(float(inrix_b_long), float(inrix_b_lat))
            inrix_b_dis = cmp_seg_geo.project(inrix_b_point)
            inrix_lines_within.loc[ln_idx, 'INRIX_B_Loc'] = inrix_b_dis * 3.2808  #meters to feet
            inrix_b_projected = cmp_seg_geo.interpolate(inrix_b_dis) #projected inrix begin point on cmp segment

            inrix_e_point = Point(float(inrix_e_long), float(inrix_e_lat))
            inrix_e_dis = cmp_seg_geo.project(inrix_e_point)
            inrix_lines_within.loc[ln_idx, 'INRIX_E_Loc'] = inrix_e_dis * 3.2808
            inrix_e_projected = cmp_seg_geo.interpolate(inrix_e_dis) #projected inrix end point on cmp segment
            
            inrix_b_e_dis=(inrix_e_dis-inrix_b_dis) * 3.2808  
            inrix_lines_within.loc[ln_idx, 'INX_B_CMP_B'] = inrix_b_dis * 3.2808
            inrix_lines_within.loc[ln_idx, 'INX_E_CMP_E'] = cmp_seg_len - inrix_e_dis * 3.2808
            
            if (inrix_b_dis==0) & (inrix_b_long !=cmp_b_long) & (inrix_b_lat !=cmp_b_lat):
                # INRIX link begin point is outside cmp segment
                cmp_b_point = Point(float(cmp_b_long), float(cmp_b_lat))
                cmp_b_dis = inrix_geo.project(cmp_b_point) 
                cmp_b_inrix_e_dis = inrix_len - cmp_b_dis * 3.2808
                cmp_b_projected = inrix_geo.interpolate(cmp_b_dis)
                projected_len_ratio= abs(inrix_b_e_dis)/cmp_b_inrix_e_dis
            elif (inrix_e_dis* 3.2808/cmp_seg_len>0.99) & (inrix_e_long !=cmp_e_long) & (inrix_e_lat !=cmp_e_lat):
                # INRIX link end point is outside cmp segment
                cmp_e_point = Point(float(cmp_e_long), float(cmp_e_lat))
                cmp_e_inrix_b_dis = inrix_geo.project(cmp_e_point)
                if cmp_e_inrix_b_dis==0:
                    inrix_lines_within.loc[ln_idx, 'Matching'] = 'No'
                    continue
                cmp_e_projected = inrix_geo.interpolate(cmp_e_inrix_b_dis)
                projected_len_ratio= abs(inrix_b_e_dis)/(cmp_e_inrix_b_dis * 3.2808)
            else:
                # INRIX link in between cmp endpoints
                projected_len_ratio= abs(inrix_b_e_dis)/inrix_len
                
            inrix_lines_within.loc[ln_idx, 'Proj_Ratio'] = projected_len_ratio
            
            inrix_degree=(180 / math.pi) * math.atan2(inrix_e_long - inrix_b_long, inrix_e_lat - inrix_b_lat)
            # Ensure the degree is calculated following the original direction
            if inrix_e_dis>inrix_b_dis:
                cmp_degree=(180 / math.pi) * math.atan2(inrix_e_projected.x - inrix_b_projected.x, inrix_e_projected.y - inrix_b_projected.y)
            else:
                cmp_degree=(180 / math.pi) * math.atan2(inrix_b_projected.x - inrix_e_projected.x, inrix_b_projected.y - inrix_e_projected.y)

            inrix_cmp_angle = abs(cmp_degree-inrix_degree)
            if inrix_cmp_angle>270:
                inrix_cmp_angle=360-inrix_cmp_angle
            inrix_lines_within.loc[ln_idx, 'Proj_Angle'] = inrix_cmp_angle

            if (projected_len_ratio > len_ratio_thrhd) & (inrix_cmp_angle < angle_thrhd):
                inrix_lines_within.loc[ln_idx, 'Matching'] = 'Yes'
            else:
                inrix_lines_within.loc[ln_idx, 'Matching'] = 'No'


inrix_lines_matched=inrix_lines_within[(inrix_lines_within['namematch']==1) & (inrix_lines_within['Matching']=='Yes')].sort_values(['cmp_segid', 'INX_B_CMP_B']).reset_index()
inrix_lines_matched['Length_Matched']=inrix_lines_matched['INRIX_E_Loc']-inrix_lines_matched['INRIX_B_Loc']

cols=['SegID', 'PreviousSe', 'NextSegID', 'B_NodeID', 'E_NodeID', 'Length_Matched', 'cmp_segid', 'INRIX_B_Loc', 'INRIX_E_Loc', 'INX_B_CMP_B', 'INX_E_CMP_E']
inrix_lines_matched_final= inrix_lines_matched[cols]


# Calculate the total length of INRIX links that have been matched
inrix_matched_length=inrix_lines_matched_final.groupby('cmp_segid').Length_Matched.sum().reset_index()
inrix_matched_length.columns = ['cmp_segid', 'Len_Matched']
inrix_matched_length['cmp_segid']= inrix_matched_length['cmp_segid'].astype(int)
cmp_segs_prj=cmp_segs_prj.merge(inrix_matched_length, on='cmp_segid', how='left')

# Calculate the ratio of the total length of matched INRIX links to the CMP segment length
cmp_segs_prj['Len_Ratio']=np.where(pd.isnull(cmp_segs_prj['Len_Matched']), 
                                   0, 
                                   round(100* cmp_segs_prj['Len_Matched']/cmp_segs_prj['Length'], 1))



# ## Search forward and backward based on already matched INRIX links 
# Establish sequence from matched links
gap_thrhd = 0.01
for cmp_seg_idx in range(len(cmp_segs_prj)):
    if (cmp_segs_prj.loc[cmp_seg_idx, 'Len_Ratio']>0) & (cmp_segs_prj.loc[cmp_seg_idx, 'Len_Ratio']<98):
        cmp_seg_id = cmp_segs_prj.loc[cmp_seg_idx, 'cmp_segid']
        cmp_seg_geo = cmp_segs_prj.loc[cmp_seg_idx]['geometry']
        cmp_seg_buffer = cmp_segs_prj.loc[cmp_seg_idx]['geometry'].buffer(mt)  #Create 150ft buffer for the segment
        cmp_b_lat = cmp_segs_prj.loc[cmp_seg_idx]['B_Lat']
        cmp_b_long = cmp_segs_prj.loc[cmp_seg_idx]['B_Long']
        cmp_e_lat = cmp_segs_prj.loc[cmp_seg_idx]['E_Lat']
        cmp_e_long = cmp_segs_prj.loc[cmp_seg_idx]['E_Long']
        cmp_seg_len = cmp_segs_prj.loc[cmp_seg_idx]['Length']
        
        inrix_lns_matched = inrix_lines_matched_final[inrix_lines_matched_final['cmp_segid']==cmp_seg_id]
        inrix_lns_matched_idx = inrix_lns_matched.index.tolist()
        
        # Search backward to check previous links
        inrix_matched_first_idx = inrix_lns_matched_idx[0]
        inrix_matched_first_b_dis = inrix_lines_matched_final.loc[inrix_matched_first_idx]['INX_B_CMP_B']
        if inrix_matched_first_b_dis/cmp_seg_len < gap_thrhd:
            cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'Yes'
        else:
            # Need find previous links
            preNode = inrix_lines_matched_final.loc[inrix_matched_first_idx]['B_NodeID']
            found = 0
            matched = 0
            while inrix_matched_first_b_dis/cmp_seg_len > gap_thrhd:
                preLinks = inrix_lines_intersect[(inrix_lines_intersect['cmp_segid'] == cmp_seg_id) & (inrix_lines_intersect['E_NodeID'] == preNode)]
                if len(preLinks)==0:
                    cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'Previous links not found'
                    break
                else:
                    preLinks_idx = preLinks.index.tolist()
                    for prelink_idx in preLinks_idx:
                        prelink_geo = inrix_lines_intersect.loc[prelink_idx]['geometry']
                        prelink_b_lat = preLinks.loc[prelink_idx]['B_Lat_left']
                        prelink_b_long = preLinks.loc[prelink_idx]['B_Long_left']
                        prelink_e_lat = preLinks.loc[prelink_idx]['E_Lat_left']
                        prelink_e_long = preLinks.loc[prelink_idx]['E_Long_left']
                        prelink_len = preLinks.loc[prelink_idx]['Miles'] * 5280 # miles to feet

                        if prelink_geo.within(cmp_seg_buffer):   # prelink is within the buffer zone               
                            prelink_b_point = Point(float(prelink_b_long), float(prelink_b_lat))
                            prelink_b_dis = cmp_seg_geo.project(prelink_b_point)
                            preLinks.loc[prelink_idx, 'INRIX_B_Loc'] = prelink_b_dis * 3.2808  
                            prelink_b_projected = cmp_seg_geo.interpolate(prelink_b_dis) #projected prelink begin point on cmp segment

                            prelink_e_point = Point(float(prelink_e_long), float(prelink_e_lat))
                            prelink_e_dis = cmp_seg_geo.project(prelink_e_point)
                            preLinks.loc[prelink_idx, 'INRIX_E_Loc'] = prelink_e_dis * 3.2808  
                            prelink_e_projected = cmp_seg_geo.interpolate(prelink_e_dis) #projected prelink end point on cmp segment

                            preLinks.loc[prelink_idx, 'INX_B_CMP_B'] = prelink_b_dis * 3.2808  #meters to feet
                            preLinks.loc[prelink_idx, 'INX_E_CMP_E'] = cmp_seg_len - prelink_e_dis * 3.2808  #meters to feet

                            prelink_b_e_dis=(prelink_e_dis-prelink_b_dis) * 3.2808
                            projected_len_ratio= abs(prelink_b_e_dis)/prelink_len
                            preLinks.loc[prelink_idx, 'Proj_Ratio'] = projected_len_ratio

                            prelink_degree=(180 / math.pi) * math.atan2(prelink_e_long - prelink_b_long, prelink_e_lat - prelink_b_lat)
                            # Ensure the degree is calculated following the original direction
                            if prelink_e_dis>prelink_b_dis:
                                cmp_degree=(180 / math.pi) * math.atan2(prelink_e_projected.x - prelink_b_projected.x, prelink_e_projected.y - prelink_b_projected.y)
                            else:
                                cmp_degree=(180 / math.pi) * math.atan2(prelink_b_projected.x - prelink_e_projected.x, prelink_b_projected.y - prelink_e_projected.y)

                            prelink_cmp_angle = abs(cmp_degree-prelink_degree)
                            if prelink_cmp_angle>270:
                                prelink_cmp_angle=360-prelink_cmp_angle
                            preLinks.loc[prelink_idx, 'Proj_Angle'] = prelink_cmp_angle

                            if (projected_len_ratio > len_ratio_thrhd) & (prelink_cmp_angle < angle_thrhd):
                                found = 1
                                cur_cnt = len(inrix_lines_matched_final)
                                inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = preLinks.loc[prelink_idx]['SegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = preLinks.loc[prelink_idx]['PreviousSe']
                                inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = preLinks.loc[prelink_idx]['NextSegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = prelink_b_e_dis
                                inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = prelink_b_dis * 3.2808
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = prelink_e_dis * 3.2808  
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = prelink_b_dis * 3.2808  
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - prelink_e_dis * 3.2808
                                inrix_matched_first_b_dis = prelink_b_dis * 3.2808
                                if inrix_matched_first_b_dis/cmp_seg_len < gap_thrhd:
                                    matched = 1
                                preNode = preLinks.loc[prelink_idx]['B_NodeID']
                                break
                            else:
                                preLinks.loc[prelink_idx, 'Matching'] = 'No'

                        else:  # prelink is intersecting with buffer zone
                            # prelink End Node is within buffer zone while Begin Node is outside buffer zone
                            prelink_e_point = Point(float(prelink_e_long), float(prelink_e_lat))
                            prelink_e_cmp_b_dis = cmp_seg_geo.project(prelink_e_point) # meter, distance from cmp begin point to prelink end point
                            preLinks.loc[prelink_idx, 'INRIX_E_Loc'] = prelink_e_cmp_b_dis * 3.2808  #meters to feet
                            prelink_e_projected = cmp_seg_geo.interpolate(prelink_e_cmp_b_dis) #projected prelink end point on cmp segment

                            cmp_b_point = Point(float(cmp_b_long), float(cmp_b_lat))
                            cmp_b_dis = prelink_geo.project(cmp_b_point) # distance from inrix prelink to cmp begin point
                            cmp_b_prelink_e_dis = prelink_len - cmp_b_dis * 3.2808  # feet
                            cmp_b_projected = prelink_geo.interpolate(cmp_b_dis) #projected cmp begin point on intersecting inrix link

                            projected_len_ratio= min(cmp_b_prelink_e_dis/(prelink_e_cmp_b_dis * 3.2808), (prelink_e_cmp_b_dis * 3.2808)/cmp_b_prelink_e_dis)
                            preLinks.loc[prelink_idx, 'Proj_Ratio'] = projected_len_ratio

                            prelink_degree=(180 / math.pi) * math.atan2(prelink_e_long - cmp_b_projected.x, prelink_e_lat - cmp_b_projected.y)
                            cmp_degree=(180 / math.pi) * math.atan2(prelink_e_projected.x - cmp_b_long, prelink_e_projected.y - cmp_b_lat)
                            prelink_cmp_angle = abs(cmp_degree-prelink_degree)
                            if prelink_cmp_angle>270:
                                prelink_cmp_angle=360-prelink_cmp_angle
                            preLinks.loc[prelink_idx, 'Proj_Angle'] = prelink_cmp_angle

                            if (projected_len_ratio > len_ratio_thrhd) & (prelink_cmp_angle < angle_thrhd):
                                found = 1
                                matched = 1
                                cur_cnt = len(inrix_lines_matched_final)
                                inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = preLinks.loc[prelink_idx]['SegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = preLinks.loc[prelink_idx]['PreviousSe']
                                inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = preLinks.loc[prelink_idx]['NextSegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = prelink_e_cmp_b_dis * 3.2808
                                inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = 0
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = prelink_e_cmp_b_dis * 3.2808  #meters to feet
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = 0
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - prelink_e_cmp_b_dis * 3.2808  #meters to feet
                                inrix_matched_first_b_dis = 0
                                break
                            else:
                                preLinks.loc[prelink_idx, 'Matching'] = 'No'
                    if matched ==1: 
                        cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'Yes-LinkAdded'
                    if found == 0:  #break while loop if no mathcing link found in prelinks
                        cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'No Matching Previous Link'
                        break
                        
        # Search forward to maintain the sequence along the traveling direction
        inrix_matched_e_dis = inrix_lines_matched_final.loc[inrix_matched_first_idx]['INX_E_CMP_E']
        if inrix_matched_e_dis/cmp_seg_len < gap_thrhd:
            cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'Yes'
        else:
            nextNode = inrix_lines_matched_final.loc[inrix_matched_first_idx]['E_NodeID']
            found = 0
            matched = 0
            while inrix_matched_e_dis/cmp_seg_len > gap_thrhd:
                if len(inrix_lns_matched[inrix_lns_matched['B_NodeID']==nextNode])==1:
                    matched_link_next = inrix_lns_matched[inrix_lns_matched['B_NodeID']==nextNode]
                    inrix_matched_e_dis = matched_link_next.INX_E_CMP_E.item()
                    nextNode = matched_link_next.E_NodeID.item()
                elif len(inrix_lns_matched[inrix_lns_matched['B_NodeID']==nextNode])>1:
                    break
                else:
                    nextLinks = inrix_lines_intersect[(inrix_lines_intersect['cmp_segid'] == cmp_seg_id) & (inrix_lines_intersect['B_NodeID'] == nextNode)]
                    if len(nextLinks)==0:
                        cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'Next links not found'
                        break
                    else:
                        nextLinks_idx = nextLinks.index.tolist()
                        for nextlink_idx in nextLinks_idx:
                            nextlink_geo = inrix_lines_intersect.loc[nextlink_idx]['geometry']
                            nextlink_b_lat = nextLinks.loc[nextlink_idx]['B_Lat_left']
                            nextlink_b_long = nextLinks.loc[nextlink_idx]['B_Long_left']
                            nextlink_e_lat = nextLinks.loc[nextlink_idx]['E_Lat_left']
                            nextlink_e_long = nextLinks.loc[nextlink_idx]['E_Long_left']
                            nextlink_len = nextLinks.loc[nextlink_idx]['Miles'] * 5280 # miles to feet
                            
                            if nextlink_geo.within(cmp_seg_buffer):   # nextlink is within the buffer zone               
                                nextlink_b_point = Point(float(nextlink_b_long), float(nextlink_b_lat))
                                nextlink_b_dis = cmp_seg_geo.project(nextlink_b_point)
                                nextLinks.loc[nextlink_idx, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                                nextlink_b_projected = cmp_seg_geo.interpolate(nextlink_b_dis) #projected nextlink begin point on cmp segment

                                nextlink_e_point = Point(float(nextlink_e_long), float(nextlink_e_lat))
                                nextlink_e_dis = cmp_seg_geo.project(nextlink_e_point)
                                nextLinks.loc[nextlink_idx, 'INRIX_E_Loc'] = nextlink_e_dis * 3.2808  #meters to feet
                                nextlink_e_projected = cmp_seg_geo.interpolate(nextlink_e_dis) #projected nextlink end point on cmp segment

                                nextLinks.loc[nextlink_idx, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808  #meters to feet
                                nextLinks.loc[nextlink_idx, 'INX_E_CMP_E'] = cmp_seg_len - nextlink_e_dis * 3.2808  #meters to feet

                                nextlink_b_e_dis=(nextlink_e_dis-nextlink_b_dis) * 3.2808  #meters to feet
                                projected_len_ratio= abs(nextlink_b_e_dis)/nextlink_len
                                nextLinks.loc[nextlink_idx, 'Proj_Ratio'] = projected_len_ratio

                                nextlink_degree=(180 / math.pi) * math.atan2(nextlink_e_long - nextlink_b_long, nextlink_e_lat - nextlink_b_lat)
                                # Ensure the degree is calculated following the original direction
                                if nextlink_e_dis>nextlink_b_dis:
                                    cmp_degree=(180 / math.pi) * math.atan2(nextlink_e_projected.x - nextlink_b_projected.x, nextlink_e_projected.y - nextlink_b_projected.y)
                                else:
                                    cmp_degree=(180 / math.pi) * math.atan2(nextlink_b_projected.x - nextlink_e_projected.x, nextlink_b_projected.y - nextlink_e_projected.y)

                                nextlink_cmp_angle = abs(cmp_degree-nextlink_degree)
                                if nextlink_cmp_angle>270:
                                    nextlink_cmp_angle=360-nextlink_cmp_angle
                                nextLinks.loc[nextlink_idx, 'Proj_Angle'] = nextlink_cmp_angle

                                if (projected_len_ratio > len_ratio_thrhd) & (nextlink_cmp_angle < angle_thrhd):
                                    found = 1
                                    cur_cnt = len(inrix_lines_matched_final)
                                    inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = nextLinks.loc[nextlink_idx]['SegID']
                                    inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = nextLinks.loc[nextlink_idx]['PreviousSe']
                                    inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = nextLinks.loc[nextlink_idx]['NextSegID']
                                    inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = nextlink_b_e_dis
                                    inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                    inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                                    inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = nextlink_e_dis * 3.2808  #meters to feet
                                    inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808  #meters to feet
                                    inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - nextlink_e_dis * 3.2808  #meters to feet
                                    inrix_matched_e_dis = cmp_seg_len - nextlink_e_dis * 3.2808
                                    if inrix_matched_e_dis/cmp_seg_len < gap_thrhd:
                                        matched = 1
                                    nextNode = nextLinks.loc[nextlink_idx]['E_NodeID']
                                    break
                                else:
                                    nextLinks.loc[nextlink_idx, 'Matching'] = 'No'

                            else:  # nextlink is intersecting with buffer zone
                                # nextlink Begin Node is within buffer zone while End Node is outside buffer zone
                                nextlink_b_point = Point(float(nextlink_b_long), float(nextlink_b_lat))
                                nextlink_b_dis = cmp_seg_geo.project(nextlink_b_point)
                                nextlink_b_cmp_e_dis = cmp_seg_len - nextlink_b_dis * 3.2808 # distance from nextlink begin point to cmp end point
                                nextLinks.loc[nextlink_idx, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                                nextlink_b_projected = cmp_seg_geo.interpolate(nextlink_b_dis) #projected nextlink begin point on cmp segment

                                cmp_e_point = Point(float(cmp_e_long), float(cmp_e_lat))
                                cmp_e_nextlink_b_dis = nextlink_geo.project(cmp_e_point) # distance from inrix nextlink begin point to cmp end point
                                cmp_e_projected = nextlink_geo.interpolate(cmp_e_nextlink_b_dis) #projected cmp end point on intersecting inrix link

                                projected_len_ratio= min(cmp_e_nextlink_b_dis* 3.2808/nextlink_b_cmp_e_dis, nextlink_b_cmp_e_dis/(cmp_e_nextlink_b_dis* 3.2808))
                                nextLinks.loc[nextlink_idx, 'Proj_Ratio'] = projected_len_ratio

                                nextlink_degree=(180 / math.pi) * math.atan2(cmp_e_projected.x - nextlink_b_long, cmp_e_projected.y - nextlink_b_lat)
                                cmp_degree=(180 / math.pi) * math.atan2(cmp_e_long - nextlink_b_projected.x, cmp_e_lat - nextlink_b_projected.y)
                                nextlink_cmp_angle = abs(cmp_degree-nextlink_degree)
                                if nextlink_cmp_angle>270:
                                    nextlink_cmp_angle=360-nextlink_cmp_angle
                                nextLinks.loc[nextlink_idx, 'Proj_Angle'] = nextlink_cmp_angle

                                if (projected_len_ratio > len_ratio_thrhd) & (nextlink_cmp_angle < angle_thrhd):
                                    found = 1
                                    matched = 1
                                    cur_cnt = len(inrix_lines_matched_final)
                                    inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = nextLinks.loc[nextlink_idx]['SegID']
                                    inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = nextLinks.loc[nextlink_idx]['PreviousSe']
                                    inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = nextLinks.loc[nextlink_idx]['NextSegID']
                                    inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = nextlink_b_cmp_e_dis
                                    inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                    inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808
                                    inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = cmp_seg_len
                                    inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808
                                    inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = 0
                                    inrix_matched_e_dis = 0
                                    break
                                else:
                                    nextLinks.loc[nextlink_idx, 'Matching'] = 'No'
                        if matched ==1: 
                            cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'Yes-LinkAdded'
                        if found == 0:  #break while loop if no mathcing link found in nextlinks
                            cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'No Matching Next Link'
                            break


# ## Find matches for remaining CMP segments that don't have any matches from previous steps
# Create buffers at both endpoints to get intersecting INRIX links
ft=150
mt=round(ft/3.2808,4)
cmp_endpoints_buffer=cmp_endpoints.copy()
cmp_endpoints_buffer['geometry'] = cmp_endpoints_buffer.geometry.buffer(mt)
cmp_endpoints_buffer.to_file(cmp + '_prj_endpoints_buffer.shp')

# INRIX lines intersecting with cmp endpoint buffer zone
inrix_lines_intersect_ep=gp.sjoin(inrix_net_prj, cmp_endpoints_buffer, op='intersects').reset_index()


# Find matching lines for zero Len_Ratio
for cmp_seg_idx in range(len(cmp_segs_prj)):
    if cmp_segs_prj.loc[cmp_seg_idx, 'Len_Ratio']==0:
        cmp_seg_id = cmp_segs_prj.loc[cmp_seg_idx, 'cmp_segid']
        cmp_seg_geo = cmp_segs_prj.loc[cmp_seg_idx]['geometry']
        cmp_seg_buffer = cmp_segs_prj.loc[cmp_seg_idx]['geometry'].buffer(mt)  #Create 150ft buffer for the segment
        cmp_seg_len = cmp_segs_prj.loc[cmp_seg_idx]['Length']
        cmp_b_id = cmp_segs_prj.loc[cmp_seg_idx]['CMP_Node_B']
        cmp_b_lat = cmp_segs_prj.loc[cmp_seg_idx]['B_Lat']
        cmp_b_long = cmp_segs_prj.loc[cmp_seg_idx]['B_Long']
        cmp_e_id = cmp_segs_prj.loc[cmp_seg_idx]['CMP_Node_E']
        cmp_e_lat = cmp_segs_prj.loc[cmp_seg_idx]['E_Lat']
        cmp_e_long = cmp_segs_prj.loc[cmp_seg_idx]['E_Long']

        intersect_b = inrix_lines_intersect_ep[inrix_lines_intersect_ep['NodeID']==cmp_b_id]
        
        if intersect_b.empty:
            cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'No Intersecting Lines Found'
        else:
            found_b = 0
            found_match = 0
            intersect_b_idx = intersect_b.index.tolist()
            for link_idx in intersect_b_idx:
                link_geo = intersect_b.loc[link_idx]['geometry']
                link_b_lat = intersect_b.loc[link_idx]['B_Lat']
                link_b_long = intersect_b.loc[link_idx]['B_Long']
                link_e_lat = intersect_b.loc[link_idx]['E_Lat']
                link_e_long = intersect_b.loc[link_idx]['E_Long']
                link_len = intersect_b.loc[link_idx]['Miles'] * 5280 # miles to feet

                # link End Node is within buffer zone while Begin Node is outside buffer zone
                link_e_point = Point(float(link_e_long), float(link_e_lat))
                link_e_cmp_b_dis = cmp_seg_geo.project(link_e_point) # distance from cmp begin point to link end point
                link_e_projected = cmp_seg_geo.interpolate(link_e_cmp_b_dis) #projected link end point on cmp segment
                
                if link_e_cmp_b_dis * 3.2808/link_len < 0.02:
                    continue
                if link_e_cmp_b_dis * 3.2808/cmp_seg_len > 0.98:
                    cmp_b_point = Point(float(cmp_b_long), float(cmp_b_lat))
                    cmp_b_dis = link_geo.project(cmp_b_point) # distance from inrix link to cmp begin point
                    cmp_b_projected = link_geo.interpolate(cmp_b_dis) #projected cmp begin point on intersecting inrix link                    
                    cmp_e_point = Point(float(cmp_e_long), float(cmp_e_lat))
                    cmp_e_dis = link_geo.project(cmp_e_point) # distance from inrix link to cmp begin point
                    cmp_e_projected = link_geo.interpolate(cmp_e_dis) #projected cmp begin point on intersecting inrix link
                    
                    link_degree=(180 / math.pi) * math.atan2(cmp_e_projected.x - cmp_b_projected.x, cmp_e_projected.y - cmp_b_projected.y)
                    cmp_degree=(180 / math.pi) * math.atan2(cmp_e_long - cmp_b_long, cmp_e_lat - cmp_b_lat)
                    link_cmp_angle = abs(cmp_degree-link_degree)
                    if link_cmp_angle>270:
                        link_cmp_angle=360-link_cmp_angle

                    if link_cmp_angle < angle_thrhd:
                        found_match = 1
                        cur_cnt = len(inrix_lines_matched_final)
                        inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = intersect_b.loc[link_idx]['SegID']
                        inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = intersect_b.loc[link_idx]['PreviousSe']
                        inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = intersect_b.loc[link_idx]['NextSegID']
                        inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = link_e_cmp_b_dis * 3.2808
                        inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                        inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = 0
                        inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = link_e_cmp_b_dis * 3.2808  #meters to feet
                        inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = 0
                        inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - link_e_cmp_b_dis * 3.2808  #meters to feet
                        break   # link_idx for loop
                    
                else:
                    cmp_b_point = Point(float(cmp_b_long), float(cmp_b_lat))
                    cmp_b_dis = link_geo.project(cmp_b_point) # distance from inrix link to cmp begin point
                    cmp_b_link_e_dis = link_len - cmp_b_dis * 3.2808  #meters to feet
                    cmp_b_projected = link_geo.interpolate(cmp_b_dis) #projected cmp begin point on intersecting inrix link
                    projected_len_ratio= cmp_b_link_e_dis/(link_e_cmp_b_dis * 3.2808)  #meters to feet

                    link_degree=(180 / math.pi) * math.atan2(link_e_long - cmp_b_projected.x, link_e_lat - cmp_b_projected.y)
                    cmp_degree=(180 / math.pi) * math.atan2(link_e_projected.x - cmp_b_long, link_e_projected.y - cmp_b_lat)
                    link_cmp_angle = abs(cmp_degree-link_degree)
                    if link_cmp_angle>270:
                        link_cmp_angle=360-link_cmp_angle

                    if (projected_len_ratio > len_ratio_thrhd) & (link_cmp_angle < angle_thrhd):
                        found_b = 1
                        cur_cnt = len(inrix_lines_matched_final)
                        inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = intersect_b.loc[link_idx]['SegID']
                        inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = intersect_b.loc[link_idx]['PreviousSe']
                        inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = intersect_b.loc[link_idx]['NextSegID']
                        inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = link_e_cmp_b_dis * 3.2808
                        inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                        inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = 0
                        inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = link_e_cmp_b_dis * 3.2808  #meters to feet
                        inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = 0
                        inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - link_e_cmp_b_dis * 3.2808  #meters to feet
                        inrix_matched_e_dis = cmp_seg_len - link_e_cmp_b_dis * 3.2808
                        nextNode = intersect_b.loc[link_idx]['E_NodeID']
                        inrix_frc = intersect_b.loc[link_idx]['FRC']
                        break # link_idx for loop
                        
            if found_match == 1:
                cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'Found Matching Link for Whole CMP Segment'
                continue
                
            if found_b == 0:
                cmp_segs_prj.loc[cmp_seg_idx, 'CMP_B_Indt'] = 'No Matching Intersecting Link'
                continue
                
            found_next = 0
            found_next_match = 0
            while inrix_matched_e_dis/cmp_seg_len > gap_thrhd:
                nextLinks = inrix_lines_intersect[(inrix_lines_intersect['cmp_segid'] == cmp_seg_id) & (inrix_lines_intersect['B_NodeID'] == nextNode)]
                if len(nextLinks)==0:
                    cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'Next links not found'
                    break
                else:
                    nextLinks_idx = nextLinks.index.tolist()
                    for nextlink_idx in nextLinks_idx:
                        nextlink_geo = nextLinks.loc[nextlink_idx]['geometry']
                        nextlink_b_lat = nextLinks.loc[nextlink_idx]['B_Lat_left']
                        nextlink_b_long = nextLinks.loc[nextlink_idx]['B_Long_left']
                        nextlink_e_lat = nextLinks.loc[nextlink_idx]['E_Lat_left']
                        nextlink_e_long = nextLinks.loc[nextlink_idx]['E_Long_left']
                        nextlink_len = nextLinks.loc[nextlink_idx]['Miles'] * 5280 # miles to feet

                        if nextlink_geo.within(cmp_seg_buffer):   # nextlink is within the buffer zone               
                            nextlink_b_point = Point(float(nextlink_b_long), float(nextlink_b_lat))
                            nextlink_b_dis = cmp_seg_geo.project(nextlink_b_point)
                            nextLinks.loc[nextlink_idx, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                            nextlink_b_projected = cmp_seg_geo.interpolate(nextlink_b_dis) #projected nextlink begin point on cmp segment

                            nextlink_e_point = Point(float(nextlink_e_long), float(nextlink_e_lat))
                            nextlink_e_dis = cmp_seg_geo.project(nextlink_e_point)
                            nextLinks.loc[nextlink_idx, 'INRIX_E_Loc'] = nextlink_e_dis * 3.2808  #meters to feet
                            nextlink_e_projected = cmp_seg_geo.interpolate(nextlink_e_dis) #projected nextlink end point on cmp segment

                            nextLinks.loc[nextlink_idx, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808  #meters to feet
                            nextLinks.loc[nextlink_idx, 'INX_E_CMP_E'] = cmp_seg_len - nextlink_e_dis * 3.2808  #meters to feet

                            nextlink_b_e_dis=(nextlink_e_dis-nextlink_b_dis) * 3.2808  #meters to feet
                            projected_len_ratio= abs(nextlink_b_e_dis)/nextlink_len
                            nextLinks.loc[nextlink_idx, 'Proj_Ratio'] = projected_len_ratio

                            nextlink_degree=(180 / math.pi) * math.atan2(nextlink_e_long - nextlink_b_long, nextlink_e_lat - nextlink_b_lat)
                            # Ensure the degree is calculated following the original direction
                            if nextlink_e_dis>nextlink_b_dis:
                                cmp_degree=(180 / math.pi) * math.atan2(nextlink_e_projected.x - nextlink_b_projected.x, nextlink_e_projected.y - nextlink_b_projected.y)
                            else:
                                cmp_degree=(180 / math.pi) * math.atan2(nextlink_b_projected.x - nextlink_e_projected.x, nextlink_b_projected.y - nextlink_e_projected.y)

                            nextlink_cmp_angle = abs(cmp_degree-nextlink_degree)
                            if nextlink_cmp_angle>270:
                                nextlink_cmp_angle=360-nextlink_cmp_angle
                            nextLinks.loc[nextlink_idx, 'Proj_Angle'] = nextlink_cmp_angle
                            
                            nextlink_frc = nextLinks.loc[nextlink_idx]['FRC']

                            if (projected_len_ratio > len_ratio_thrhd) & (nextlink_cmp_angle < angle_thrhd) & (inrix_frc==nextlink_frc):
                                found_next = 1
                                cur_cnt = len(inrix_lines_matched_final)
                                inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = nextLinks.loc[nextlink_idx]['SegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = nextLinks.loc[nextlink_idx]['PreviousSe']
                                inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = nextLinks.loc[nextlink_idx]['NextSegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = nextlink_b_e_dis
                                inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = nextlink_e_dis * 3.2808  #meters to feet
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808  #meters to feet
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = cmp_seg_len - nextlink_e_dis * 3.2808  #meters to feet
                                inrix_matched_e_dis = cmp_seg_len - nextlink_e_dis * 3.2808
                                if inrix_matched_e_dis/cmp_seg_len < gap_thrhd:
                                    found_next_match = 1
                                nextNode = nextLinks.loc[nextlink_idx]['E_NodeID']
                                break  # for loop
                            else:
                                nextLinks.loc[nextlink_idx, 'Matching'] = 'No'

                        else:  # nextlink is intersecting with buffer zone
                            # nextlink Begin Node is within buffer zone while End Node is outside buffer zone
                            nextlink_b_point = Point(float(nextlink_b_long), float(nextlink_b_lat))
                            nextlink_b_dis = cmp_seg_geo.project(nextlink_b_point)
                            nextlink_b_cmp_e_dis = cmp_seg_len - nextlink_b_dis * 3.2808 # distance from nextlink begin point to cmp end point
                            nextLinks.loc[nextlink_idx, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808  #meters to feet
                            nextlink_b_projected = cmp_seg_geo.interpolate(nextlink_b_dis) #projected nextlink begin point on cmp segment

                            cmp_e_point = Point(float(cmp_e_long), float(cmp_e_lat))
                            cmp_e_nextlink_b_dis = nextlink_geo.project(cmp_e_point) # distance from inrix nextlink begin point to cmp end point
                            cmp_e_projected = nextlink_geo.interpolate(cmp_e_nextlink_b_dis) #projected cmp end point on intersecting inrix link

                            projected_len_ratio= min(cmp_e_nextlink_b_dis * 3.2808/nextlink_b_cmp_e_dis, nextlink_b_cmp_e_dis/(cmp_e_nextlink_b_dis* 3.2808))
                            nextLinks.loc[nextlink_idx, 'Proj_Ratio'] = projected_len_ratio

                            nextlink_degree=(180 / math.pi) * math.atan2(cmp_e_projected.x - nextlink_b_long, cmp_e_projected.y - nextlink_b_lat)
                            cmp_degree=(180 / math.pi) * math.atan2(cmp_e_long - nextlink_b_projected.x, cmp_e_lat - nextlink_b_projected.y)
                            nextlink_cmp_angle = abs(cmp_degree-nextlink_degree)
                            if nextlink_cmp_angle>270:
                                nextlink_cmp_angle=360-nextlink_cmp_angle
                            nextLinks.loc[nextlink_idx, 'Proj_Angle'] = nextlink_cmp_angle
                            
                            nextlink_frc = nextLinks.loc[nextlink_idx]['FRC']

                            if (projected_len_ratio > len_ratio_thrhd) & (nextlink_cmp_angle < angle_thrhd) & (inrix_frc==nextlink_frc):
                                found_next = 1
                                found_next_match = 1
                                cur_cnt = len(inrix_lines_matched_final)
                                inrix_lines_matched_final.loc[cur_cnt, 'SegID'] = nextLinks.loc[nextlink_idx]['SegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'PreviousSe'] = nextLinks.loc[nextlink_idx]['PreviousSe']
                                inrix_lines_matched_final.loc[cur_cnt, 'NextSegID'] = nextLinks.loc[nextlink_idx]['NextSegID']
                                inrix_lines_matched_final.loc[cur_cnt, 'Length_Matched'] = nextlink_b_cmp_e_dis
                                inrix_lines_matched_final.loc[cur_cnt, 'cmp_segid'] = cmp_seg_id
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_B_Loc'] = nextlink_b_dis * 3.2808
                                inrix_lines_matched_final.loc[cur_cnt, 'INRIX_E_Loc'] = cmp_seg_geo.project(cmp_e_projected)* 3.2808
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_B_CMP_B'] = nextlink_b_dis * 3.2808
                                inrix_lines_matched_final.loc[cur_cnt, 'INX_E_CMP_E'] = max(0, cmp_seg_len-cmp_seg_geo.project(cmp_e_projected)* 3.2808)
                                inrix_matched_e_dis = max(0, cmp_seg_len-cmp_seg_geo.project(cmp_e_projected)* 3.2808)
                                nextNode = nextLinks.loc[nextlink_idx]['E_NodeID']
                                break # for loop
                            else:
                                nextLinks.loc[nextlink_idx, 'Matching'] = 'No'
                    if found_next_match ==1: 
                        cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'Yes-LinkAdded'
                    if found_next == 0:  #break while loop if no mathcing link found in nextlinks
                        cmp_segs_prj.loc[cmp_seg_idx, 'CMP_E_Indt'] = 'No Matching Next Link'
                        break


del cmp_segs_prj['Len_Matched']
del cmp_segs_prj['Len_Ratio']

# Calculate the total length of matched INRIX links
inrix_matched_length=inrix_lines_matched_final.groupby('cmp_segid').Length_Matched.sum().reset_index()
inrix_matched_length.columns = ['cmp_segid', 'Len_Matched']
inrix_matched_length['cmp_segid']= inrix_matched_length['cmp_segid'].astype(int)

cmp_segs_prj=cmp_segs_prj.merge(inrix_matched_length, on='cmp_segid', how='left')
cmp_segs_prj['Len_Ratio']=np.where(pd.isnull(cmp_segs_prj['Len_Matched']), 
                                   0, 
                                   round(100* cmp_segs_prj['Len_Matched']/cmp_segs_prj['Length'], 1))

inrix_lines_matched_output = inrix_lines_matched_final[['cmp_segid', 'SegID', 'Length_Matched']]
inrix_lines_matched_output.columns = ['CMP_SegID', 'INRIX_SegID', 'Length_Matched']

# Output files
cmp_segs_prj.to_file(cmp + '_matchedlength_check.shp')
inrix_lines_matched_output.to_csv('CMP_Segment_INRIX_Links_Correspondence.csv', index=False)
