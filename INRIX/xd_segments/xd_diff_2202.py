'''
Created on Apr 12, 2022

@author: bsana
'''

import pandas as pd
import geopandas as gpd
from os.path import join

NETCONF_DIR = r'Q:\CMP\LOS Monitoring 2022\Network_Conflation'
CORR_FILE = 'CMP_Segment_INRIX_Links_Correspondence_2201_Manual_PLUS_ExpandedNetwork.csv'

GEO_DIR = r'Q:\GIS\Transportation\Roads\INRIX\XD\22_02'
add_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdadded', 'USA_CALIFORNIA.csv'))
remove_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdremoved', 'USA_California.csv'))
#replace_df = pd.read_csv(join(GEO_DIR, 'maprelease-xdreplaced', 'USA_California.csv'))
#no replace df for 2202
shp_df = gpd.read_file(join(GEO_DIR, 'maprelease-shapefiles', 'SF', 'Inrix_XD_2202_SF.shp'))
#should i update / change this to be just sf ?? 

old_shp_df = gpd.read_file(r'Q:\GIS\Transportation\Roads\INRIX\XD\22_01\maprelease-shapefiles\SF\Inrix_XD_2201_SF.shp')

OUT_DIR = r'Q:\CMP\LOS Monitoring 2022\Network_Conflation'
# print(shp_df.columns)
# Index(['OID_1', 'XDSegID', 'PreviousXD', 'NextXDSegI', 'FRC', 'RoadNumber',
#        'RoadName', 'LinearID', 'Country', 'State', 'County', 'District',
#        'Miles', 'Lanes', 'SlipRoad', 'SpecialRoa', 'RoadList', 'StartLat',
#        'StartLong', 'EndLat', 'EndLong', 'Bearing', 'XDGroup', 'ShapeSRID',
#        'geometry'],
#       dtype='object')

corr_df = pd.read_csv(join(NETCONF_DIR, CORR_FILE))
count_series = corr_df['INRIX_SegID'].value_counts()

onetoone_series = count_series[count_series==1]
onetoone_series = onetoone_series.reset_index()

count_series = count_series[count_series>1]
count_series = count_series.reset_index()
corr_df2 = corr_df.drop_duplicates('INRIX_SegID')

shp_df.XDSegID = pd.to_numeric(shp_df.XDSegID)

xd_add = shp_df.loc[shp_df['XDSegID'].isin(add_df['SegId'].tolist()), ]
print('XD Segments Added - {}'.format(len(xd_add)))
print(xd_add)

xd_add.to_file(join(OUT_DIR, 'xd_added_2202.shp'))
# looks like 947 new segments were added

#replace_list = replace_df['XDId_21_1'].tolist()
#no replace data from INRIX this time around
#xd_rep_1 = onetoone_series.loc[onetoone_series['INRIX_SegID'].isin(replace_list), ]
#print('2101 XD Segments Replaced with one-to-one Corr - {}'.format(len(xd_rep_1)))

#xd_rep_2101 = corr_df2.loc[corr_df2['INRIX_SegID'].isin(replace_list), ]
#print('2101 XD Segments Replaced - {}'.format(len(xd_rep_2101)))
#replace_list = xd_rep_2101['INRIX_SegID'].tolist()
#xd_rep_2101 = corr_df.loc[corr_df['INRIX_SegID'].isin(replace_list), ]
#cmp_list = xd_rep_2101['CMP_SegID'].unique().tolist()
#xd_rep_2101 = corr_df.loc[corr_df['CMP_SegID'].isin(cmp_list), ]
#order_df = xd_rep_2101[['CMP_SegID','INRIX_SegID']]
#xd_rep_2101 = xd_rep_2101.merge(replace_df[['XDId_21_1', 'XDId_22_1']], how='left', 
#                                left_on='INRIX_SegID', right_on='XDId_21_1')

#shp_df['XDId_22_1'] = shp_df['XDSegID'].astype(float)
#xd_rep_2101 = xd_rep_2101.merge(shp_df[['XDId_22_1','RoadName','Miles']], how='left', on='XDId_22_1')
#xd_rep_2101 = order_df.merge(xd_rep_2101)
#xd_rep_2101.to_csv(join(OUT_DIR, 'XD_2201_replaced.csv'), index=False)

xd_rem = corr_df2.loc[corr_df2['INRIX_SegID'].isin(remove_df['SegId']), ]
print('XD Segments Removed - {}'.format(len(xd_rem)))
print(xd_rem)
#nothing removed

#nothing going forward bc nothing removed and nothing in replace list this time around
#xd_rem_norep = xd_rem.loc[~xd_rem['INRIX_SegID'].isin(replace_list), ]
#print('XD Segments Removed but not Replaced - {}'.format(len(xd_rem_norep)))
#old_shp_df['INRIX_SegID'] = old_shp_df['XDSegID'].astype(int)
#xd_rem_norep = xd_rem_norep.merge(old_shp_df[['INRIX_SegID','RoadName','Miles']], how='left', on='INRIX_SegID')
#xd_rem_norep.to_csv(join(OUT_DIR, 'XD_2102_removed.csv'), index=False)
