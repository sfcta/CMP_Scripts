import geopandas as gp
import pandas as pd
import numpy as np
import networkx as nx
import os
from shapely.geometry import Point, Polygon, LineString, mapping
from shapely import geometry
from simpledbf import Dbf5
import warnings
warnings.filterwarnings('ignore')

# GTFS directories, service ids, and years
GTFS = [[r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\Coverage\gtfs_2021may13', '1_merged_10007724', 2021],
       [r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\Coverage\gtfs_2020april9', 1, 2020],
       [r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\Coverage\gtfs_2019may22', '1_merged_8846826', 2019]]

# Output directory
Coverage_Dir = r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\Coverage'

# OSM Streets
Streets_Dir = r'C:\Users\xzh263\Dropbox (KTC)\SFCTA CMP\2021 CMP\Coverage\champ_hwy_shapefile'
street_file = 'champ_freeflow.shp'

#TAZ shapefile
TAZ_Dir = Coverage_Dir
taz_file = 'TAZ2454_clean\TAZ2454_clean.shp'

# define parameters needed by the calculation
min_trips = 22
buffer_radius = 0.25 * 5280 # a quarter mile walking distance

#Define NAD 1983 StatePlane California III
cal3 = {'proj': 'lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002', 'ellps': 'GRS80', 'datum': 'NAD83', 'no_defs': True}

# Define Functions
def generate_transit_stops_geo(stop_dir):
    stops=pd.read_csv(os.path.join(stop_dir, 'stops.txt'))
    stops['geometry'] = list(zip(stops.stop_lon, stops.stop_lat))
    stops['geometry'] = stops['geometry'].apply(Point)
    stops = gp.GeoDataFrame(stops, geometry='geometry', crs={'init': 'epsg:4326'})
    return stops
    
def generate_transit_shapes_geo(stop_dir, service_id):
    shapes=pd.read_csv(os.path.join(stop_dir, 'shapes.txt'))
    shapes_gdf = pd.DataFrame()
    shape_ids = shapes.shape_id.unique().tolist()
    rid = 0
    for shpid in shape_ids:
        shp = shapes[shapes['shape_id']==shpid].sort_values(by='shape_pt_sequence')
        linestr = LineString(zip(shp.shape_pt_lon, shp.shape_pt_lat))
        linestr = gp.GeoDataFrame(index=[shpid], crs='epsg:4326', geometry=[linestr]) 
        shapes_gdf = shapes_gdf.append(linestr)
        rid = rid + 1
    shapes_gdf = shapes_gdf.reset_index()
    shapes_gdf.columns = ['shape_id', 'geometry']
    
    trips = pd.read_csv(os.path.join(stop_dir, 'trips.txt'))
    trips = trips[trips['service_id']==service_id]
    trips_shapes = shapes_gdf[shapes_gdf['shape_id'].isin(trips['shape_id'])]
    return trips, shapes, trips_shapes
    
def frequent_bus_routes(gtfs_dir, service_id, peak_period, outname):
    # input gtfs files
    routes_info =pd.read_csv(os.path.join(gtfs_dir, 'routes.txt'))
    stops = generate_transit_stops_geo(gtfs_dir)
    trips, shapes, trips_shapes = generate_transit_shapes_geo(gtfs_dir, service_id)
    stop_times = pd.read_csv(os.path.join(gtfs_dir, 'stop_times.txt'))
    stop_times['hour'] = stop_times['arrival_time'].apply(lambda x: int(x[0:2]))
    stop_times['minute'] = stop_times['arrival_time'].apply(lambda x: int(x[3:5]))
    
    period_cols = ['route_id', 'direction_id']
    #There may be multiples shapes for the same route, so here the most frequent shape is used for each route_id
    trips_shapes_mcv = trips.groupby(period_cols)['shape_id'].agg(lambda x:x.value_counts().index[0]).reset_index()
    
    start_stops_idx = stop_times.groupby(['trip_id'])['stop_sequence'].transform(min) == stop_times['stop_sequence']
    trips_hour = pd.merge(trips, 
                          stop_times[start_stops_idx][['trip_id', 'arrival_time', 'departure_time', 'hour', 'minute']],
                         on='trip_id', how='left')
    
    # trips occuring during the time period of interest
    # whole period
    if peak_period == 'AM':
        trips_period = trips_hour[(trips_hour['hour']>=7) & (trips_hour['hour']<9)]
    elif peak_period == 'PM':
        trips_period= trips_hour[((trips_hour['hour']==16) & (trips_hour['minute']>=30)) | (trips_hour['hour']==17) | ((trips_hour['hour']==18) & (trips_hour['minute']<30))]
    else:
        print('Input needs to be either AM or PM')
        
    # check if routes meet the minimum period and hourly requirements
    route_period_counts = trips_period.groupby(period_cols).trip_id.count().reset_index()
    route_period_counts.columns = period_cols +['total_trips']
    route_frequent = route_period_counts[route_period_counts['total_trips']>= min_trips]
    if len(route_frequent)>0:
        route_frequent_shapes = route_frequent.merge(trips_shapes_mcv, on= period_cols, how='left')
        route_frequent_shapes = trips_shapes.merge(route_frequent_shapes, on='shape_id')
        route_frequent_shapes = route_frequent_shapes.merge(routes_info, on='route_id', how='left')
        route_frequent_shapes.to_file(os.path.join(Coverage_Dir, 'frequent_routes_5min_' + outname + '_' + peak_period + '.shp'))
    else:
        print('No frequent routes found for %s %s' % (outname, peak_period))
    
    # frequent stops
    stop_cols = ['stop_id', 'route_id', 'direction_id']
    stop_times_by_route = stop_times.merge(trips[['route_id', 'direction_id', 'trip_id']], on='trip_id', how='left')
    if peak_period == 'AM':
        stop_route_period = stop_times_by_route[(stop_times_by_route['hour']>=7) & (stop_times_by_route['hour']<9)]
    elif peak_period == 'PM':
        stop_route_period = stop_times_by_route[((stop_times_by_route['hour']==16) & (stop_times_by_route['minute']>=30)) | (stop_times_by_route['hour']==17) | ((stop_times_by_route['hour']==18) & (stop_times_by_route['minute']<30))]
    else:
        print('Input needs to be either AM or PM')
        
    stop_period_counts = stop_route_period.groupby(stop_cols).trip_id.count().reset_index()
    stop_period_counts.columns = stop_cols + ['total_trips']
    stop_frequent = stop_period_counts[stop_period_counts['total_trips']>= min_trips]

    if len(stop_frequent)>0:
        stop_frequent_list = stop_frequent.stop_id.unique().tolist()
        stop_frequent_gdf = stops[stops['stop_id'].isin(stop_frequent_list)]
        stop_frequent_gdf.to_file(os.path.join(Coverage_Dir, 'frequent_stops_5min_' + outname + '_' + peak_period + '.shp'))
    else:
        print('No frequent stops found for %s %s' % (outname, peak_period))
        stop_frequent_list=[]
    return stop_frequent_list, route_period_counts, stop_period_counts
    
# TAZ Zones
taz_shp = gp.read_file(os.path.join(TAZ_Dir, taz_file))
taz_sf_shp = taz_shp[taz_shp['COUNTY']==1] 
taz_sf_shp = taz_sf_shp.to_crs(cal3)

# Streets network
streets = gp.read_file(os.path.join(Streets_Dir, street_file))
streets.insert(0, 'LinkID', range(1, len(streets)+1))
streets = streets.to_crs(cal3)

def latlong(x):
    return round(x.coords.xy[1][0],6), round(x.coords.xy[0][0], 6), round(x.coords.xy[1][-1], 6), round(x.coords.xy[0][-1], 6)
streets['B_Lat'], streets['B_Long'], streets['E_Lat'], streets['E_Long'] = zip(*streets['geometry'].map(latlong))

b_nodes = streets[['B_Lat', 'B_Long']]
b_nodes.columns = ['Lat', 'Long']
e_nodes = streets[['E_Lat', 'E_Long']]
e_nodes.columns = ['Lat', 'Long']

streets_endnodes = b_nodes.append(e_nodes, ignore_index=True).reset_index()

# Assign unique node id
endnodes_cnt=streets_endnodes.groupby(['Lat', 'Long']).index.count().reset_index()
endnodes_cnt.rename(columns={'index':'NodeCnt'}, inplace=True)
endnodes_cnt['NodeID'] = endnodes_cnt.index+1

# Generate the the unique node shapefile  
#endnodes_cnt['geometry'] = list(zip(endnodes_cnt.Long, endnodes_cnt.Lat))
#endnodes_cnt['geometry'] = endnodes_cnt['geometry'].apply(Point)
#endnodes_unique_gpd = gp.GeoDataFrame(endnodes_cnt, geometry='geometry')
#endnodes_unique_gpd.crs = cal3
#endnodes_unique_gpd.to_file(os.path.join(Streets_Dir, 'streets_endnodes.shp'))

endnodes_cnt = endnodes_cnt[['Lat', 'Long', 'NodeCnt', 'NodeID']]
endnodes_cnt.columns = ['B_Lat', 'B_Long', 'B_NodeCnt', 'B_NodeID']
streets = streets.merge(endnodes_cnt, on=['B_Lat', 'B_Long'], how='left')

endnodes_cnt.columns = ['E_Lat', 'E_Long', 'E_NodeCnt', 'E_NodeID']
streets = streets.merge(endnodes_cnt, on=['E_Lat', 'E_Long'], how='left')
endnodes_cnt.columns = ['Lat', 'Long', 'NodeCnt', 'NodeID']

streets['length'] = 3.2808 * streets.geometry.length
streets['b_e'] = list(zip(streets['B_NodeID'], streets['E_NodeID']))
streets['e_b'] = list(zip(streets['E_NodeID'], streets['B_NodeID']))
# Save the updated street shapefile with endnodes
#outcols = [c for c in streets.columns.tolist() if c not in ['b_e', 'e_b']]
#streets[outcols].to_file(os.path.join(Streets_Dir, 'streets_with_endnodes.shp'))

# Build Walking Network
def build_walking_network(gtfs_dir):
    stops = generate_transit_stops_geo(gtfs_dir)
    stops = stops.to_crs(cal3)
    
    stops_within_sf = gp.sjoin(stops, taz_sf_shp, op='within').reset_index()
    stops_within_sf = stops_within_sf[stops.columns]
    stops_within_sf.insert(0, 'NodeID', range(endnodes_cnt['NodeID'].max() + 1, endnodes_cnt['NodeID'].max() + 1 + len(stops_within_sf)))
    
    search_radius = 300 # ft

    stops_geo = stops_within_sf.copy()
    stops_geo['point_geo'] = stops_geo['geometry']
    stops_geo['geometry'] = stops_geo['geometry'].buffer(search_radius/3.2808)
    stop_near_links = gp.sjoin(streets[['LinkID', 'B_NodeID', 'E_NodeID', 'length', 'geometry']], stops_geo, op='intersects')

    def calc_dist(x):
        stop_point = x['point_geo']
        link_geo = x['geometry']
        x['near_dist'] = stop_point.distance(link_geo)
        x['stop_to_begin'] = link_geo.project(stop_point) * 3.2808  #meters to feet
        x['stop_to_end'] = x['length'] - x['stop_to_begin']
        return x

    stop_near_links = stop_near_links.apply(calc_dist, axis=1)
    stop_near_links = stop_near_links.sort_values(['stop_id','near_dist'])
    stop_near_links = stop_near_links.drop_duplicates('stop_id')
    stop_near_links['near_link'] = stop_near_links['LinkID']
    stop_near_links['near_link_bid'] = stop_near_links['B_NodeID']
    stop_near_links['near_link_eid'] = stop_near_links['E_NodeID']
    stop_near_links = stop_near_links.reset_index()

    # construct a network graph
    tgraph = nx.Graph() 

    # road network nodes
    tgraph.add_nodes_from(endnodes_cnt.NodeID.tolist())

    # road network links
    for i in range (0, len(streets)):
        tgraph.add_edge(streets.loc[i,'B_NodeID'], 
                              streets.loc[i,'E_NodeID'], 
                              weight = streets.loc[i, 'length'])
    return stops_within_sf, stop_near_links, tgraph
    
def stop_walking_area(walk_graph, walk_dis, start_node, link_near_stop):
    cur_path = dict(nx.single_source_dijkstra_path(walk_graph, start_node, cutoff=walk_dis, weight='weight'))
    del cur_path[start_node]
    reach_links = {}
    for key in cur_path:
        sub_path = list(zip(cur_path[key][:-1],cur_path[key][1:]))
        for each_link in sub_path:
            if each_link in reach_links:
                next
            else:
                reach_links[each_link] = 1
    reach_links_df = pd.DataFrame.from_dict(reach_links, orient='index',columns=['accessed']).reset_index()
    reach_links_df.rename(columns={'index':'b_e'},inplace=True)
    streets_access = streets[(streets['b_e'].isin(reach_links_df['b_e'])) | (streets['e_b'].isin(reach_links_df['b_e'])) | (streets['LinkID']==link_near_stop)]
    geom = [x for x in streets_access.geometry]
    multi_line = geometry.MultiLineString(geom)
    multi_line_polygon = multi_line.convex_hull
    
    return multi_line_polygon
    
# Accessible area from high frequent stops
def frequent_access_area(walk_graph, stop_list, stop_with_nearest_link, buffer_radius):
    stop_access_gdf = gp.GeoDataFrame()
    for cur_stop_id in stop_list:
        lidx = stop_with_nearest_link.index[stop_with_nearest_link['stop_id']==cur_stop_id][0]
        cur_node_id = stop_with_nearest_link.loc[lidx, 'NodeID']
        cur_link = stop_with_nearest_link.loc[lidx, 'near_link']

        cur_graph = walk_graph.copy()
        cur_graph.add_node(cur_node_id)
        cur_graph.add_edge(stop_with_nearest_link.loc[lidx,'near_link_bid'], 
                          stop_with_nearest_link.loc[lidx,'NodeID'], 
                          weight = stop_with_nearest_link.loc[lidx, 'stop_to_begin'])
        cur_graph.add_edge(stop_with_nearest_link.loc[lidx,'NodeID'], 
                              stop_with_nearest_link.loc[lidx,'near_link_eid'], 
                              weight = stop_with_nearest_link.loc[lidx, 'stop_to_end'])

        get_geo = stop_walking_area(cur_graph, buffer_radius, cur_node_id, cur_link)
        cur_access_polygon = gp.GeoDataFrame(index=[0], crs=cal3, geometry=[get_geo])
        cur_access_polygon['stop_id'] = cur_stop_id
        stop_access_gdf = stop_access_gdf.append(cur_access_polygon, ignore_index=True)
    return stop_access_gdf
    
# Attach TAZ attributes
taz_dbf = Dbf5(os.path.join(Coverage_Dir, 'tazdata.dbf'))
taz = taz_dbf.to_dataframe()
taz['SFTAZ'] = taz['SFTAZ'].astype(int)
taz_sf = taz_sf_shp.merge(taz, left_on = 'TAZ', right_on = 'SFTAZ', how = 'left')
taz_sf["area_acre"] = taz_sf['geometry'].area * 0.00024711 #Square meters to acres

def frequent_stops_access_taz(frequent_stops_access_union):
    frequent_stops_access_taz= taz_sf_shp['geometry'].intersection(frequent_stops_access_union)

    taz_sf_access_gdf = gp.GeoDataFrame()
    taz_sf_access_gdf['accessarea'] = frequent_stops_access_taz.area* 0.00024711 #Square meters to acres
    taz_sf_access_gdf['index'] = frequent_stops_access_taz.index
    taz_sf_access_gdf['geometry'] = frequent_stops_access_taz.geometry

    taz_sf_access_tazid = taz_sf_access_gdf.merge(taz_sf_shp[['TAZ', 'AREALAND']], left_on='index', right_index=True, how='left')
    taz_sf_access_attrs = taz_sf[['TAZ', 'AREALAND', 'HHLDS', 'TOTALEMP', 'POP', 'area_acre']].merge(taz_sf_access_tazid, on=['TAZ', 'AREALAND'], how='left')
    
    taz_sf_access_attrs['areapcnt'] = 100 * taz_sf_access_attrs['accessarea'] / taz_sf_access_attrs['area_acre']
    taz_sf_access_attrs['access_pop'] = taz_sf_access_attrs['POP'] * taz_sf_access_attrs['areapcnt'] / 100 
    taz_sf_access_attrs['access_jobs'] = taz_sf_access_attrs['TOTALEMP'] * taz_sf_access_attrs['areapcnt'] / 100 
    taz_sf_access_attrs['access_hhlds'] = taz_sf_access_attrs['HHLDS'] * taz_sf_access_attrs['areapcnt'] / 100 
    
    outcols = ['accessarea', 'index', 'TAZ', 'AREALAND', 'HHLDS', 'TOTALEMP',
           'POP', 'area_acre', 'areapcnt', 'access_pop', 'access_jobs', 'access_hhlds']    
    return taz_sf_access_attrs[outcols]

df_coverage = pd.DataFrame()
idx = 0
for gtfs_id_year in GTFS:
    stops_within_sf, stop_near_links, tgraph = build_walking_network(gtfs_id_year[0])
    for period in ['AM', 'PM']:
        df_coverage.loc[idx, 'year'] = int(gtfs_id_year[2])
        df_coverage.loc[idx, 'period'] = period
        
        stop_frequent_list, route_period_counts, stop_period_counts = frequent_bus_routes(gtfs_id_year[0], gtfs_id_year[1], period, str(gtfs_id_year[2]))
        stop_frequent_list_with_sf = [stopid for stopid in stop_frequent_list if stopid in stops_within_sf['stop_id'].tolist()]
        if len(stop_frequent_list_with_sf)>0:
            frequent_stops_access_area = frequent_access_area(tgraph, stop_frequent_list_with_sf, stop_near_links, buffer_radius)
            frequent_stops_access_area.to_file(os.path.join(Coverage_Dir, 'frequent_stops_5min_' + str(gtfs_id_year[2]) + '_' + period + 'buffer.shp'))
            frequent_stops_access_union = frequent_stops_access_area.geometry.unary_union
            frequent_access_taz_attrs = frequent_stops_access_taz(frequent_stops_access_union)
            df_coverage.loc[idx, 'cover_area'] = round(100*frequent_access_taz_attrs['accessarea'].sum()/frequent_access_taz_attrs['area_acre'].sum(),2)
            df_coverage.loc[idx, 'cover_pop'] = round(100*frequent_access_taz_attrs['access_pop'].sum()/frequent_access_taz_attrs['POP'].sum(),2)
            df_coverage.loc[idx, 'cover_jobs'] = round(100*frequent_access_taz_attrs['access_jobs'].sum()/frequent_access_taz_attrs['TOTALEMP'].sum(),2)
            df_coverage.loc[idx, 'cover_hhlds'] = round(100*frequent_access_taz_attrs['access_hhlds'].sum()/frequent_access_taz_attrs['HHLDS'].sum(),2)
        else:
            df_coverage.loc[idx, 'cover_area']= 0
            df_coverage.loc[idx, 'cover_pop']= 0
            df_coverage.loc[idx, 'cover_jobs']= 0
            df_coverage.loc[idx, 'cover_hhlds']= 0
            df_coverage.loc[idx, 'note'] = 'No frequent stops found'
        idx += 1
        
df_coverage.to_csv(os.path.join(Coverage_Dir, 'cmp_transit_coverage_metrics.csv'), index=False)