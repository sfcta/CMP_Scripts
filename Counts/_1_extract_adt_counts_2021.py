import pandas as pd
from time import time,localtime,strftime
from os.path import join
import sys

starttime    = time()
print(strftime("%x %X", localtime(starttime)) + " Started")

id_map = {'4':'5','5':'6','6':'7','7':'8','8':'9','9':'10','10':'11',
          '11':'13','12':'14','13':'16','14':'18','15':'20','16':'21',
          '17':'22','18':'23','19':'24','20':'25','21':'27','22':'28','23':'30',
          '24':'31','25':'32','26':'34','27':'35','28':'36','29':'37'}

month_dict = {'APRIL':'4', 'MAY':'5'}
BASE_DIR = 'Q:\CMP\LOS Monitoring 2021\Counts\ADT\SFCTA 2021 ADT Results'

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
for i in range(1,30):
    infile = join(BASE_DIR, 'LOC #{}.xls'.format(i))
    for dayno in range(1,7):
        sheet_name = 'Day' + str(dayno)
        try:
            street = pd.read_excel(infile, sheet_name, header=None, skiprows=5, usecols="D:D", nrows=1).values[0][0]
            date = pd.read_excel(infile, sheet_name, header=None, skiprows=7, usecols="D:D", nrows=1).values[0][0].split()
            date = '%s.%s.%s' %(date[3], month_dict[date[1]],date[2][:-3])
            
            if i in [1,11,14]:
                dir1 = pd.read_excel(infile, sheet_name, header=None, skiprows=9, usecols="C:C", nrows=1).values[0][0]
                dir2 = pd.read_excel(infile, sheet_name, header=None, skiprows=9, usecols="J:J", nrows=1).values[0][0]
            else:
                dir1 = pd.read_excel(infile, sheet_name, header=None, skiprows=9, usecols="D:D", nrows=1).values[0][0]
                dir2 = pd.read_excel(infile, sheet_name, header=None, skiprows=9, usecols="K:K", nrows=1).values[0][0]
            
            temp_df = pd.read_excel(infile, sheet_name, header=None, skiprows=12, usecols="A:E", nrows=24)
            temp_df1 = proc_data(temp_df, dir1)
            temp_df = pd.read_excel(infile, sheet_name, header=None, skiprows=12, usecols="H:L", nrows=24)
            temp_df2 = proc_data(temp_df, dir2)
            temp_df = pd.concat([temp_df1,temp_df2])
            temp_df['Date'] = date
            
            locid = str(i)
            if locid in id_map.keys(): locid = id_map[locid]
            temp_df['ID'] = locid + '_'
            temp_df = temp_df[['ID','Date','Direction','Time','Vol']]
            out_df = out_df.append(temp_df)
        
        except ValueError:
            pass  
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

out_df = out_df[out_df['Vol']>0]
out_df.to_csv(r'Q:\CMP\LOS Monitoring 2021\Counts\ADT\data2021.csv', index=False)

print("Finished in %5.2f mins" % ((time() - starttime)/60.0))