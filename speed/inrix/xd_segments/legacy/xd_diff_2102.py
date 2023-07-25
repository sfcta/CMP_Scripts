'''
Created on Dec 6, 2021

@author: bsana
'''

import pandas as pd
import geopandas as gpd
from os.path import join

NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2021\Network_Conflation\pre-CMP'
CORR_FILE = 'CMP_Segment_INRIX_Links_Correspondence_2101_Manual - PLUS.csv'

GEO_DIR = r'Q:\GIS\Transportation\Roads\INRIX\XD\21_02'
add_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdadded', 'USA_CALIFORNIA.csv'))
# remove_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdremoved', 'USA_California.csv'))
# replace_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdreplaced', 'USA_California.csv'))
shp_df = gpd.read_file(join(GEO_DIR, 'maprelease-shapefiles', 'SF', 'Inrix_XD_2102_SF.shp'))

old_shp_df = gpd.read_file(r'Q:\GIS\Transportation\Roads\INRIX\XD\21_01\maprelease-shapefiles\SF\Inrix_XD_2101_SF.shp')

OUT_DIR = r'Q:\CMP\LOS Monitoring 2021\Network_Conflation'
# print(shp_df.columns)
# Index(['OID_1', 'XDSegID', 'PreviousXD', 'NextXDSegI', 'FRC', 'RoadNumber',
#        'RoadName', 'LinearID', 'Country', 'State', 'County', 'District',
#        'Miles', 'Lanes', 'SlipRoad', 'SpecialRoa', 'RoadList', 'StartLat',
#        'StartLong', 'EndLat', 'EndLong', 'Bearing', 'XDGroup', 'ShapeSRID',
#        'geometry'],
#       dtype='object')

corr_df = pd.read_csv(join(NETCONF_DIR, CORR_FILE))
count_series = corr_df['INRIX_SegID'].value_counts()
count_series = count_series[count_series>1]
count_series = count_series.reset_index()
corr_df2 = corr_df.drop_duplicates('INRIX_SegID')

xd_add = shp_df.loc[shp_df['XDSegID'].isin(add_df['SegId'].tolist()), ]
print('XD Segments Added - {}'.format(len(xd_add)))
print(xd_add)
# looks like no new segments were added in SF
