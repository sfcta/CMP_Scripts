'''
Created on Sep 30, 2019

@author: bsana
'''
import pandas as pd

infile = r'Q:\CMP\LOS Monitoring 2019\Transit\Speed\SF_CMP_Transit_Speeds_2019_Final.csv'

tr_df = pd.read_csv(infile)
tr_df = tr_df.rename(columns={'cmp_segid':'cmp_id', 'avg_loc_speed':'avg_speed', 'std_loc_speed':'std_dev'})
tr_df['year'] = 2019
tr_df['source'] = 'APC'
tr_df['comment'] = ''
tr_df['cmp_id'] = tr_df['cmp_id'].astype(int)
tr_df = tr_df[['cmp_id','year','source','period','avg_speed','std_dev','sample_size','comment']]
tr_df.to_csv(r'Q:\CMP\LOS Monitoring 2019\Transit\Speed\CMP 2019 Transit LOS v1.csv', index=False)