'''
This script is developed to automatically summarize the transit boardings, alightings, and loads at specified cordon stops at a 30-minute 
resolution for typical weekdays, Saturdays, and Sundays.

INPUTS: Transit APC data

OUTPUTS: The average boardings, alightings, and loads by stops and day types at the 30-min resolution.

USAGE:
To use the script, replace dir1 with the directory where the APC data of interest is located and replace the APC file name. 
Run the script with the command: python SF_CMP_Transit_APC_Volume.py.
'''

import pandas as pd
import numpy as np

# Read in the file
dir1 = 'S:/CMP/Transit/Volume/'
vol = pd.read_csv(dir1 + 'APC_2019_SPRING_SO_STOPS02.txt', sep='\t')

# Assign 30-minute period
vol['Close_Hour'] = vol['CLOSE_DATE_TIME'].str[10:12].astype(int)
vol['Close_Minute'] = vol['CLOSE_DATE_TIME'].str[13:15].astype(int)
vol['Close_Second'] = vol['CLOSE_DATE_TIME'].str[16:18].astype(int)
vol['Close_Period'] = vol['CLOSE_DATE_TIME'].str[-2:]
vol['Close_Time'] = vol['ACTUAL_DATE'] + ' ' + vol['Close_Hour'].astype('str') + ':' + vol['Close_Minute'].astype('str') + ':' + vol['Close_Second'].astype('str') + ' ' + vol['Close_Period']
vol['Close_Time'] = pd.to_datetime(vol['Close_Time'])
vol = vol.sort_values(by=['EXT_TRIP_ID', 'ACTUAL_DATE', 'VEHICLE_ID', 'Close_Time']).reset_index()
vol['Epoch'] = 2*vol['Close_Time'].dt.hour + vol['Close_Time'].dt.minute//30 

# Aggregate the boardings, alightings, and loads from individual trips to the corresponding 30-minute period
vol_sum = vol.groupby(['STOPID', 'ACTUAL_DATE', 'Epoch']).agg({'ONS': 'sum', 'OFFS': 'sum', 'MAX_LOAD': 'sum'}).reset_index()
vol_sum.columns = ['STOPID', 'ACTUAL_DATE', 'Epoch', 'Boardings', 'Alightings', 'Loads']

# Assign day type, i.e. weekdays, Saturday, or Sunday
vol_sum['Date']=pd.to_datetime(vol_sum['ACTUAL_DATE'])
vol_sum['DOW']=vol_sum.Date.dt.dayofweek #Mon-0
vol_sum['DayType'] = np.where(vol_sum['DOW']<=4, 'Weekdays', np.where(vol_sum['DOW']==5, 'Saturday', 'Sunday'))

# Remove Mondays and Fridays to get typical weekdays
vol_sum = vol_sum[(vol_sum['DOW']!=0) & (vol_sum['DOW']!=4)]

# Average boardings, alightings, and loads by stop, daytype, and period
vol_daytype_avg_stops = vol_sum.groupby(['STOPID', 'DayType', 'Epoch']).agg({'Boardings': 'mean', 
                                                                             'Alightings': 'mean',
                                                                            'Loads': 'mean'}).reset_index()
vol_daytype_avg_stops.columns= ['STOPID', 'DayType', 'Epoch', 'Avg_Boardings', 'Avg_Alightings', 'Avg_Loads']

# Create empty dataframe with continuous time periods
daytypes = ['Weekdays', 'Saturday', 'Sunday']
stop_vol_complete = pd.DataFrame()
cnt = 0
for stop_id in vol_stops:
    for day_type in daytypes:
        for epoch_id in range(48):
            stop_vol_complete.loc[cnt, 'STOPID'] = stop_id
            stop_vol_complete.loc[cnt, 'DayType'] = day_type
            stop_vol_complete.loc[cnt, 'Epoch'] = epoch_id
            cnt = cnt + 1
stop_vol_complete['Epoch'] = stop_vol_complete['Epoch'].astype(int)

# Join the vol_daytype_avg_stops dataframe to the created complate dataframe
stop_vol_complete = pd.merge(stop_vol_complete, vol_daytype_avg_stops, on=['STOPID', 'DayType', 'Epoch'], how='left')
stop_vol_complete['Hour'] = stop_vol_complete['Epoch']//2
stop_vol_complete['Minute'] = np.where(stop_vol_complete['Epoch']%2 ==0, '00', '30')
stop_vol_complete['Period'] = stop_vol_complete['Hour'].astype(str) + ':' + stop_vol_complete['Minute']

# Save the output file
stop_vol_complete[['STOPID', 'DayType', 'Period', 'Avg_Boardings', 'Avg_Alightings', 'Avg_Loads']].to_csv(dir1 + 'CMP_APC_Average_Volume.csv', index=False)