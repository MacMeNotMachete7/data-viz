import sqlite3
import glob
import pandas as pd
import sys
import time
import datetime
import os
import numpy as np

def main():
#    save_irradiance_to_pickle_agg_by_station()
#    save_irradiance_to_pickle_agg_by_day()
    
#    plot_station_data()
    
    load_irradiance_agg_by_station()
    load_irradiance_agg_by_day()
    
def plot_station_data():
    df = pd.read_csv('TMY3_StationsMeta.csv')
    df.plot(x='Longitude', y='Latitude')

def load_irradiance_agg_by_station():
    df = pd.read_pickle('tmy3_agg_by_station.pkl')
    df.plot(x='long', y='lat')

def load_irradiance_agg_by_day():
    df = pd.read_pickle('tmy3_agg_by_day.pkl')
    df.plot(x='long', y='lat')

def save_irradiance_to_pickle_agg_by_day():
    start, start_readable = time.time(), datetime.datetime.now()
    print(f'Started at {start_readable}')
    
    i, num_files = 0, len(list(glob.iglob('alltmy3a/*.csv')))
    df_global = pd.DataFrame(columns=['date', 'GHI (W/m^2)', 'usaf', 'name', 'state', 'lat', 'long', 'elev'])
    
    for filepath in glob.iglob('alltmy3a/*.csv'):
        df_data = pd.read_csv(filepath, header=1, parse_dates=[[0,1]])
        df_data = df_data[['Date (MM/DD/YYYY)_Time (HH:MM)', 'GHI (W/m^2)']]
        
        # combine date and time columns
        df_data = df_data.rename({'Date (MM/DD/YYYY)_Time (HH:MM)':'date'}, axis='columns')
        df_data.date = df_data.date.str.replace('24:00', '00:00') # technically, this is the wrong, but I'm not using the time anyway
        df_data.date = pd.to_datetime(df_data.date, infer_datetime_format=True)
        df_data['GHI (W/m^2)'] = df_data['GHI (W/m^2)'].astype(float)
        df_data = df_data.groupby([df_data.date.dt.to_period('D')]).mean()
        df_data.reset_index(inplace=True)
        
        with open(filepath) as f:
            first_line = f.readline().strip()
        s = first_line.split(',')

        df_data['usaf'] = s[0]
        df_data['name'] = s[1]
        df_data['state'] = s[2]
        df_data['lat'] = s[4]
        df_data['long'] = s[5]
        df_data['elev'] = s[6]
        df_data['lat'] = df_data['lat'].astype(float)
        df_data['long'] = df_data['long'].astype(float)
        df_data['elev'] = df_data['elev'].astype(float)
        
        df_global = df_global.append(df_data, ignore_index=True)

        print_progress(i, num_files)
        i += 1
    
    df_global['usaf'] = df_global['usaf'].astype('category')
    df_global['name'] = df_global['name'].astype('category')
    df_global['state'] = df_global['state'].astype('category')
    df_global.date = df_global.date.values.astype('datetime64[D]')
    df_global.date = df_global.date.apply(lambda x: x.replace(year=2000))

    df_global.to_pickle('tmy3_agg_by_day.pkl')
    
    end, end_readable = time.time(), datetime.datetime.now()
    print(f'Finished at {end_readable}')
    print(f'Total run-time {round(end - start,2)}s')

def save_irradiance_to_pickle_agg_by_station():
    start, start_readable = time.time(), datetime.datetime.now()
    print(f'Started at {start_readable}')
    
    i, num_files = 0, len(list(glob.iglob('alltmy3a/*.csv')))
    df_global = pd.DataFrame(columns=['usaf', 'name', 'state', 'lat', 'long', 'elev', 'GHI (W/m^2)'])
    
    for filepath in glob.iglob('alltmy3a/*.csv'):
        df_data = pd.read_csv(filepath, header=1)

        with open(filepath) as f:
            first_line = f.readline().strip()
        s = first_line.split(',')
        
        df_station = pd.DataFrame({'usaf':s[0], 'name':s[1], 'state':s[2], 'lat':s[4], 'long':s[5], 'elev':s[6]}, index=range(1))
        df_station['lat'] = df_station['lat'].astype(float)
        df_station['long'] = df_station['long'].astype(float)
        df_station['elev'] = df_station['elev'].astype(float)
        df_station['GHI (W/m^2)'] = df_data['GHI (W/m^2)'].mean()

        df_global = df_global.append(df_station, ignore_index=True)

        print_progress(i, num_files)
        i += 1
    
    df_global['usaf'] = df_global['usaf'].astype('category')
    df_global['name'] = df_global['name'].astype('category')
    df_global['state'] = df_global['state'].astype('category')
    
    df_global.to_pickle('tmy3_agg_by_station.pkl')

    end, end_readable = time.time(), datetime.datetime.now()
    print(f'Finished at {end_readable}')
    print(f'Total run-time {round(end - start,2)}s')

# saving the last two pickles runs OOM on my computer
def save_irradiance_to_pickle():
    start, start_readable = time.time(), datetime.datetime.now()
    print(f'Started at {start_readable}')
    
    i, num_files = 0, len(list(glob.iglob('alltmy3a/*.csv')))
    df_global = pd.DataFrame(columns=['usaf', 'name', 'state', 'lat', 'long', 'elev', 'date', 'GHI (W/m^2)'])
    
    for filepath in glob.iglob('alltmy3a/*.csv'):
        df_data = pd.read_csv(filepath, header=1, parse_dates=[[0,1]])
        df_data = df_data[['Date (MM/DD/YYYY)_Time (HH:MM)', 'GHI (W/m^2)']]
        
        # combine date and time columns
        df_data = df_data.rename({'Date (MM/DD/YYYY)_Time (HH:MM)':'date'}, axis='columns')
        df_data.date = df_data.date.str.replace('24:00', '00:00') # technically, this is the wrong, but I'm not using the time anyway
        df_data.date = pd.to_datetime(df_data.date, infer_datetime_format=True)
        df_data['GHI (W/m^2)'] = df_data['GHI (W/m^2)'].astype(float)
        
        with open(filepath) as f:
            first_line = f.readline().strip()
        s = first_line.split(',')
        
        df_station_info = pd.DataFrame({'usaf':s[0], 'name':s[1], 'state':s[2], 'lat':s[3], 'long':s[4], 'elev':s[5]}, index=range(5))
        df_station_info['lat'] = df_station_info['lat'].astype(float)
        df_station_info['long'] = df_station_info['long'].astype(float)
        df_station_info['elev'] = df_station_info['elev'].astype(float)
        df_station = df_station_info.iloc[np.full(df_data.shape[0], 0)]
        
        df_station.reset_index(drop=True, inplace=True)
        df_data.reset_index(drop=True, inplace=True)
        
        df_concat = pd.concat([df_station, df_data], axis=1)
        df_global = df_global.append(df_concat, ignore_index=True)

        print_progress(i, num_files)
        i += 1
    
    df_global['usaf'] = df_global['usaf'].astype('category')
    df_global['name'] = df_global['name'].astype('category')
    df_global['state'] = df_global['state'].astype('category')
    df_global.date = df_global.date.apply(lambda x: x.replace(year=2000))
    
    df_global.to_pickle('tmy3.pkl')
    
    # aggregate and save more pickles
    df_agg_by_station = df_global.groupby(['usaf', 'name', 'state', 'lat', 'long', 'elev']).mean()
    df_agg_by_station.to_pickle('tmy3_agg_by_station.pkl')
    
    df_agg_by_day = df_global.groupby(['usaf', 'name', 'state', 'lat', 'long', 'elev', df_global.date.dt.to_period('D')]).mean()
    df_agg_by_day.to_pickle('tmy3_agg_by_day.pkl')
    
    end, end_readable = time.time(), datetime.datetime.now()
    print(f'Finished at {end_readable}')
    print(f'Total run-time {round(end - start,2)}s')
    
def load_full_data_set_to_sqlite():
    # setup schema
    os.remove('tmy3.db')
    conn = sqlite3.connect('tmy3.db')
    
    with conn:
        conn.execute("DROP TABLE IF EXISTS station")
        conn.execute("DROP TABLE IF EXISTS tmy")
        
    # load station data
    '''
    USAF		Station ID
    Site Name	Station full name (from National Climatic Data Center)
    State		State of station 
    Latitude	Latitude from NSRDB (decimal degrees)
    Longitude	Longitude from NSRDB (decimal degrees)
    TZ			Time zone (hours from GMT, negative west)
    Class		NSRDB station class
    Pool		The minimum number of years from which TMY candidate months were pulled
    '''
    station = pd.read_csv("TMY3_StationsMeta.csv")
    station.to_sql('station', conn)
    
    # load sunlight data
    i, num_files = 0, len(list(glob.iglob('alltmy3a/*.csv')))
    
    for filepath in glob.iglob('alltmy3a/*.csv'):
        with open(filepath) as f:
            first_line = f.readline().strip()
        usaf = first_line.split(',')[0]
        df = pd.read_csv(filepath, header=1)
        df.insert(0, 'USAF', pd.Series(len(df) * [usaf]))
        
        df.to_sql('usaf', conn, if_exists='append')
        
        print_progress(i, num_files)
        i += 1

    # cleanup
    conn.commit()
    conn.close()
    
def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    '''
    modified by incrementing iteration so bar ends at 100%
    from https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
    '''
    
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    iteration += 1
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
        sys.stdout.flush()

if __name__== "__main__":
    pd.set_option('display.expand_frame_repr', False)
    main()