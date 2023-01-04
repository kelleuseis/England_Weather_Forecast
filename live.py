"""Interactions with rainfall and river data."""

import pandas as pd
import numpy as np

import os
import argparse
import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import signal
from tqdm import tqdm

def get_live_station_measures(station_reference=None, param='rainfall', filename=None):
    """Return readings from live API.

    Parameters
    ----------

    filename: str
        Write Filename
        
    param: 'rainfall' or 'level'
        Station Parameter
        
    station_reference: str, list or None
        station_reference to return.

    >>> data = get_live_station_data('1029TH', 'live_data.csv')
    """
    
    assert (param == "rainfall") or (param == "level"), \
            'Please specify a valid parameter: "rainfall" or "level"'
        
    with urllib.request.urlopen(
        f'https://environment.data.gov.uk/flood-monitoring/id/measures.json?parameter={param}&_limit=5000'
    ) as url:
        file = pd.DataFrame(json.load(url)['items'])
        
    data = file[['stationReference','latestReading','parameter',
                 'qualifier','unitName']].set_index('stationReference')

    data['latestReading'] = data['latestReading'].apply(lambda x: x['value'] if isinstance(x,dict) else np.nan)
    data = data[~data.index.duplicated(keep='last')]
    
    if station_reference is not None:
        if isinstance(station_reference, str) == True:
            station_reference = [station_reference]
        data = data.reindex(station_reference)
        
    if filename is not None:
        with open(filename,'w') as outfile:
            outfile.write(data.to_csv(index=True))
    
    return data



def retrieve_file():
    matches = []
    keywords = input('Please specify keyword(s) e.g. station, data: ')


    for (dirpath,_,files) in os.walk(os.getcwd()):
        matches += [os.sep.join((dirpath,filename)) for filename in files 
                    if any(pattern.lower() in filename.lower() 
                           for pattern in keywords.split(","))
                    and (".csv" in filename.lower())]

    print("Found these matches:")
    print(*matches, sep="\n")

    usrinp = input("Please specify chosen full filepath: ")

    try:
        df = pd.read_csv(usrinp).set_index("stationReference")
        print(f"DataFrame Loaded! \n {df}")
        return df
    except:
        print('DataFrame cannot be loaded: File does not contain the column "stationReference"')
        return None


    
def download_station_data(url):
    file = pd.read_json(url).pop('items')
    try:
        file['maxOnRecord']=file.stageScale['maxOnRecord']['value']
        file['minOnRecord']=file.stageScale['minOnRecord']['value']
        file['typicalRangeHigh']=file.stageScale['typicalRangeHigh']
        file['typicalRangeLow']=file.stageScale['typicalRangeLow']
    except:
        pass
    stninfo = file.reindex(['stationReference', 'lat', 'long',
                            'easting', 'northing', 'maxOnRecord', 'minOnRecord',
                            'typicalRangeHigh', 'typicalRangeLow']).to_dict()
    return stninfo



def download_all_station_data(param="level", df=None):
    
    if df is None:
        usrinp = input("Input DataFrame not specified: " + \
                       "Do you perhaps want to build upon an existing file? " + \
                       "(This will not overwrite the file.) [Y/N] ")
        
        if usrinp.upper() != "N":
            df = retrieve_file()
    
    
    usrinp = input("Downloading the full dataset would take approx. 60 mins " + \
                   "!!!Please ensure that output is saved to a variable!!! Proceed? [Y/N] ")

    if usrinp.upper() == "N":
        return None
    
    
    def handler(signalnum, frame):
        raise Exception
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(30)
    
    try:
        data = get_live_station_measures(param=param)
    except:
        pass
    
    signal.alarm(0) 
    assert 'data' in locals(), "504 Gateway Timeout Error: Cannot fetch station list"
    
    print("List of live stations fetched successfully!")
    
    if df is not None:
        data = data.drop(index=df.reset_index().stationReference.tolist(), 
                         errors='ignore')
    
        
    executor = ThreadPoolExecutor(max_workers=min(os.cpu_count()+4, len(data.index)))
    futures = []
    stninfolist = []
    
    for ref in tqdm(data.index.tolist()):
        url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{ref}.json"
        futures.append(executor.submit(download_station_data, url))
    
    print("Download requested! Starting download ...")
    done,_ = wait(futures)
    print("Dataset downloaded! Saving to variable ...")
    
    for future in tqdm(done):
        try:
            stninfolist.append(future.result())
        except:
            continue

    executor.shutdown(wait=False)
    
    
    stninfodf = pd.DataFrame(stninfolist).set_index('stationReference')
    if df is not None:
        stninfodf = pd.concat([df,stninfodf])
    
    print("Downloaded dataset now saved to variable")
            
    return stninfodf[stninfodf.lat.notna()].drop_duplicates()


    
if __name__=="__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--param', type = str, 
                        help = 'Station Parameter ("rainfall" or "level")')
    parser.add_argument('-f', '--file', type = str, help = 'Write Filename')
    args = parser.parse_args()
    
    data = get_live_station_measures(station_reference=None, param=args.param, filename=args.file)
    
    print(data)
