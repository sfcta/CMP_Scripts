
# Use this script to extract all ADT counts from Excel file to a csv file

import pandas as pd
import sys
from time import time,localtime,strftime

starttime    = time()
print(strftime("%x %X", localtime(starttime)) + " Started")

month_dict = {'APRIL':'4', 'MAY':'5'}

cells_info = {
    'LOC-1': [5, 53, 101, 149, 197, 245, 293],
    'LOC-2': [5, 53, 101],
    'LOC-3': [5, 53, 101],
    'LOC-4': [5, 53, 101],
    'LOC-5': [5, 53, 101],
    'LOC-6': [5, 53, 101],
    'LOC-7': [5, 53, 101],
    'LOC-8': [5, 53, 101],
    'LOC-9': [5, 53, 101],
    'LOC-10': [5, 53, 101],
    'LOC-11': [5, 53, 101, 149, 197, 245, 293],
    'LOC-12': [5, 53, 101],
    'LOC-13': [5, 53, 101],
    'LOC-14': [5, 53, 101, 149, 197, 245, 293],
    'LOC-15': [5, 53, 101],
    'LOC-16': [5, 53, 101],
    'LOC-17': [5, 53, 101],
    'LOC-18': [5, 53, 101],
    'LOC-19': [5, 53, 101],
    'LOC-20': [5, 53, 101],
    'LOC-21': [5, 53, 101],
    'LOC-22': [5, 53, 101],
    'LOC-23': [5, 53, 101],
    'LOC-24': [5, 53, 101],
    'LOC-25': [5, 53, 101],
    'LOC-26': [5, 53, 101],
    'LOC-27': [5, 53, 101],
    'LOC-28': [5, 53, 101],
    'LOC-29': [5, 53, 101]
    }

id_map = {'4':'5','5':'6','6':'7','7':'8','8':'9','9':'10','10':'11',
          '11':'13','12':'14','13':'16','14':'18','15':'20','16':'21',
          '17':'22','18':'23','19':'24','20':'25','21':'27','22':'28','23':'30',
          '24':'31','25':'32','26':'34','27':'35','28':'36','29':'37'}

infile = r'Q:\CMP\LOS Monitoring 2019\Counts\ADT\SFCTA_final_v2.xlsx'

time_dict = {'t1':['00','15'],'t2':['15','30'],'t3':['30','45'],'t4':['45','00']}
def fmt_time(x):
    t_nxt = str(int(x['Time'])+1).zfill(2)
    if x['variable']=='t4':
        x['Time'] = x['Time'] + time_dict[x['variable']][0] + '-' + t_nxt + time_dict[x['variable']][1]
    else:
        x['Time'] = x['Time'] + time_dict[x['variable']][0] + '-' + x['Time'] + time_dict[x['variable']][1]
    return x
def proc_data(temp_df, direction):
    temp_df.columns = ['Time','t1','t2','t3','t4']
    temp_df['Time'] = pd.to_datetime(temp_df['Time'],format= '%H:%M:%S' ).dt.hour.astype(str)
    temp_df['Time'] = temp_df['Time'].apply(lambda x: x.zfill(2))
    temp_df = pd.melt(temp_df, id_vars=['Time'], value_name='Vol')
    temp_df = temp_df.apply(fmt_time, axis=1)
    temp_df['Direction'] = direction
    return temp_df

out_df = pd.DataFrame()
for key in cells_info.keys():
    sheet_name = key
    for loc in cells_info[key]:
        street = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="D:D", nrows=1).values[0][0]
        loc += 1
        intersection = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="D:D", nrows=1).values[0][0]
        loc += 1
        date = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="D:D", nrows=1).values[0][0].split()
        date = '%s.%s.%s' %(date[3], month_dict[date[1]],date[2][:-1])
        
        loc += 2
        dir1 = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="D:D", nrows=1).values[0][0]
        dir2 = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="K:K", nrows=1).values[0][0]
        
        loc += 3
        temp_df = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="A:E", nrows=24)
        temp_df1 = proc_data(temp_df, dir1)
        temp_df = pd.read_excel(infile, sheet_name, header=None, skiprows=loc, usecols="H:L", nrows=24)
        temp_df2 = proc_data(temp_df, dir2)
        temp_df = pd.concat([temp_df1,temp_df2])
        temp_df['Date'] = date
        dummy, locid = sheet_name.split('-',1)
        if locid in id_map.keys(): locid = id_map[locid]
        temp_df['ID'] = locid
        temp_df['ID'] += '_'
        temp_df = temp_df[['ID','Date','Direction','Time','Vol']]
        out_df = out_df.append(temp_df)

out_df = out_df.loc[out_df['Vol']>0]
out_df.to_csv(r'Q:\CMP\LOS Monitoring 2019\Counts\ADT\data2019.csv', index=False)

print("Finished in %5.2f mins" % ((time() - starttime)/60.0))

# df = pd.read_excel(infile, 'LOC-1', header=None, skiprows=9, usecols="D:D", nrows=1)
# print(df.values[0][0])