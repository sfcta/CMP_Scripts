'''
Created on Sep 30, 2020

@author: bsana
'''

import pandas as pd
import geopandas as gpd
from os.path import join

NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2019\Network_Conflation\XD_20_01'
CORR_FILE = 'CMP_Segment_INRIX_Links_Correspondence_2001_Manual.csv'

GEO_DIR = r'Q:\GIS\Transportation\Roads\INRIX\XD\20_02'
add_df = pd.read_csv(join(GEO_DIR, 'xdadded', 'USA_CALIFORNIA.csv'))
remove_df = pd.read_csv(join(GEO_DIR, 'xdremoved', 'USA_CALIFORNIA.csv'))
replace_df = pd.read_csv(join(GEO_DIR, 'xdreplaced', 'USA_California.csv'))
shp_df = gpd.read_file(join(GEO_DIR, 'shapefiles', 'SF', 'Inrix_XD_2002_SF.shp'))

old_shp_df = gpd.read_file(r'Q:\GIS\Transportation\Roads\INRIX\XD\20_01\SF\Inrix_XD_2001_SF.shp')

OUT_DIR = r'Q:\CMP\LOS Monitoring 2020\Network_Conflation'

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

replace_list = replace_df['XDId_20_1'].tolist()
xd_rep_2001 = corr_df2.loc[corr_df2['INRIX_SegID'].isin(replace_list), ]
print('2001 XD Segments Replaced - {}'.format(len(xd_rep_2001)))
replace_list = xd_rep_2001['INRIX_SegID'].tolist()
xd_rep_2001 = corr_df.loc[corr_df['INRIX_SegID'].isin(replace_list), ]
cmp_list = xd_rep_2001['CMP_SegID'].unique().tolist()
xd_rep_2001 = corr_df.loc[corr_df['CMP_SegID'].isin(cmp_list), ]
order_df = xd_rep_2001[['CMP_SegID','INRIX_SegID']]
xd_rep_2001 = xd_rep_2001.merge(replace_df[['XDId_20_1', 'XDId_20_2']], how='left', 
                                left_on='INRIX_SegID', right_on='XDId_20_1')

shp_df['XDId_20_2'] = shp_df['XDSegID'].astype(float)
xd_rep_2001 = xd_rep_2001.merge(shp_df[['XDId_20_2','RoadName','Miles']], how='left', on='XDId_20_2')
xd_rep_2001 = order_df.merge(xd_rep_2001)
xd_rep_2001.to_csv(join(OUT_DIR, 'XD_2002_replaced.csv'), index=False)

xd_rem = corr_df2.loc[corr_df2['INRIX_SegID'].isin(remove_df['SegId']), ]
print('XD Segments Removed - {}'.format(len(xd_rem)))
print(xd_rem)

xd_rem_norep = xd_rem.loc[~xd_rem['INRIX_SegID'].isin(replace_list), ]
print('XD Segments Removed but not Replaced - {}'.format(len(xd_rem_norep)))
old_shp_df['INRIX_SegID'] = old_shp_df['XDSegID'].astype(int)
xd_rem_norep = xd_rem_norep.merge(old_shp_df[['INRIX_SegID','RoadName','Miles']], how='left', on='INRIX_SegID')
xd_rem_norep.to_csv(join(OUT_DIR, 'XD_2001_removed.csv'), index=False)

