'''
This script is developed to summarize the PeMS counts by stations and day types, i.e typical weekdays, Saturdays, and Sundays.

INPUTS:
1. Selected PeMS stations in the San Francisco.
2. Downloaded PeMS Station 5-min count datasets.

OUTPUTS:
1. The average counts by stations and day types at 30-min resolution.

USAGE:
To use the script, place the script under the S:\CMP\PeMS folder where input files are located and run with the command: 
python SF_CMP_PeMS_Counts.py.
'''


import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
client = dask.distributed.Client()
import warnings
warnings.filterwarnings("ignore")

pems = pd.read_csv('S:/CMP/PeMS/SF_PeMS_Stations.csv')

census = pd.read_csv('S:/CMP/PeMS/SF_Census_Stations.csv')

# Only April and May count data are downloaded. Additional count data can be downloaded and processed if necessary.
months = [4, 5]
sf_counts = pd.DataFrame()
# Attach column names following PeMS instruction
colnames = ['Timestamp', 'Station', 'District', 'Freeway', 'Direction', 'Lane Type', 'Station Length',
           'Samples', 'Observed%', 'Total Flow', 'Avg Occupancy', 'Avg Speed', 
           'Lane 1 Samples', 'Lane 1 Flow', 'Lane 1 Avg Occ', 'Lane 1 Avg Speed', 'Lane 1 Observed',
           'Lane 2 Samples', 'Lane 2 Flow', 'Lane 2 Avg Occ', 'Lane 2 Avg Speed', 'Lane 2 Observed',
           'Lane 3 Samples', 'Lane 3 Flow', 'Lane 3 Avg Occ', 'Lane 3 Avg Speed', 'Lane 3 Observed',
           'Lane 4 Samples', 'Lane 4 Flow', 'Lane 4 Avg Occ', 'Lane 4 Avg Speed', 'Lane 4 Observed',
           'Lane 5 Samples', 'Lane 5 Flow', 'Lane 5 Avg Occ', 'Lane 5 Avg Speed', 'Lane 5 Observed',
           'Lane 6 Samples', 'Lane 6 Flow', 'Lane 6 Avg Occ', 'Lane 6 Avg Speed', 'Lane 6 Observed',
           'Lane 7 Samples', 'Lane 7 Flow', 'Lane 7 Avg Occ', 'Lane 7 Avg Speed', 'Lane 7 Observed',
           'Lane 8 Samples', 'Lane 8 Flow', 'Lane 8 Avg Occ', 'Lane 8 Avg Speed', 'Lane 8 Observed']
for month in months:
    if month ==4:
        for day in range(1, 31):
            if day<10:
                filename = 'S:/CMP/PeMS/d04_text_station_5min_2019_04_0' + str(day) + '.txt'
            else:
                filename = 'S:/CMP/PeMS/d04_text_station_5min_2019_04_' + str(day) + '.txt'
            df = dd.read_csv(filename, names = colnames, assume_missing=True)
			
	    # Query out counts for selected stations
            df_sf_day = df[df['Station'].isin(sf_stations)].compute()
            sf_counts = sf_counts.append(df_sf_day, ignore_index=True)
            
    if month ==5:
        for day in range(1, 32):
            if day<10:
                filename = 'S:/CMP/PeMS/d04_text_station_5min_2019_05_0' + str(day) + '.txt'
            else:
                filename = 'S:/CMP/PeMS/d04_text_station_5min_2019_05_' + str(day) + '.txt'
            df = dd.read_csv(filename, names = colnames, assume_missing=True)
            df_sf_day = df[df['Station'].isin(sf_stations)].compute()
            sf_counts = sf_counts.append(df_sf_day, ignore_index=True)


# Convert the timestamp into date_time attribute
sf_counts['Date_Time'] = pd.to_datetime(sf_counts['Timestamp'])
sf_counts['DOW']=sf_counts.Date_Time.dt.dayofweek # Monday-0
sf_counts['DayType'] = np.where(sf_counts['DOW']<=4, 'Weekdays', np.where(sf_counts['DOW']==5, 'Saturday', 'Sunday'))
sf_counts['Epoch'] = 2*sf_counts['Date_Time'].dt.hour + sf_counts['Date_Time'].dt.minute//30 
sf_counts['Date']=sf_counts.Date_Time.dt.date

# Remove Mondays and Fridays to get typical weekdays
sf_counts = sf_counts[(sf_counts['DOW']!=0) & (sf_counts['DOW']!=4)]

# Aggregate counts from 5-min to 30-min
counts_epoch_agg = sf_counts.groupby(['Station', 'DayType', 'Date', 'Epoch']).agg({'Total Flow': 'sum'}).reset_index()
counts_epoch_agg.columns = ['Station', 'DayType', 'Date', 'Epoch', 'Counts_30Min']

# Average counts by station, daytpe and epoch
counts_daytype_avg = counts_epoch_agg.groupby(['Station', 'DayType', 'Epoch']).agg({'Counts_30Min': 'mean'}).reset_index()
counts_daytype_avg.columns= ['Station', 'DayType', 'Epoch', 'Avg_Counts']
counts_daytype_avg['Station'] = counts_daytype_avg['Station'].astype(int)
counts_daytype_avg['Avg_Counts'] = round(counts_daytype_avg['Avg_Counts'], 0)

# Create empty dataframe with continuous time periods
daytypes = ['Weekdays', 'Saturday', 'Sunday']
counts_daytype_complete = pd.DataFrame()
cnt = 0
count_stations = counts_daytype_avg['Station'].unique().tolist()
for stop_id in count_stations:
    for day_type in daytypes:
        for epoch_id in range(48):
            counts_daytype_complete.loc[cnt, 'Station'] = stop_id
            counts_daytype_complete.loc[cnt, 'DayType'] = day_type
            counts_daytype_complete.loc[cnt, 'Epoch'] = epoch_id
            cnt = cnt + 1
counts_daytype_complete['Epoch'] = counts_daytype_complete['Epoch'].astype(int)
counts_daytype_complete['Station'] = counts_daytype_complete['Station'].astype(int)

# Join the average counts to the complete table
counts_daytype_complete = pd.merge(counts_daytype_complete, counts_daytype_avg, on=['Station', 'DayType', 'Epoch'], how='left')

counts_daytype_complete['Hour'] = counts_daytype_complete['Epoch']//2
counts_daytype_complete['Minute'] = np.where(counts_daytype_complete['Epoch']%2 ==0, '00', '30')
counts_daytype_complete['Period'] = counts_daytype_complete['Hour'].astype(str) + ':' + counts_daytype_complete['Minute']

# Save the output table
counts_daytype_complete[['Station', 'DayType', 'Period', 'Avg_Counts']].to_csv('S:/CMP/PeMS/SF_PeMS_Stations_Average_Counts.csv', index=False)
