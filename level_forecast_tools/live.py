"""Interactions with live rainfall and level data."""

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
    ------------------------------------------

    filename: str
        Write Filename
        
    param: 'rainfall' or 'level' or ''
        Station Parameter
        
    station_reference: str, array-like or None
        station_reference to return.
        
    Returns
    ------------------------------------------
    Pandas DataFrame of all most recent measures.
    """
    
    assert (param == "rainfall") or (param == "level") or (param == ''), \
            'Please specify a valid parameter: "rainfall" or "level" or ""'
        
    with urllib.request.urlopen(
        f'https://environment.data.gov.uk/flood-monitoring/id/measures.json?parameter={param}&_limit=10000'
    ) as url:
        file = pd.json_normalize(json.load(url)['items'])
        
    data = file[['stationReference', 'latestReading.value', 'parameter',
                 'qualifier', 'unitName']].set_index('stationReference')
    
    if station_reference is not None:
        if isinstance(station_reference, str) == True:
            station_reference = [station_reference]
        data = data[data.index.isin(station_reference)]
        
    if filename is not None:
        with open(filename,'w') as outfile:
            outfile.write(data.to_csv(index=True))
    
    return data


def get_live_station_readings(station_reference):   
    '''
    Get all recent readings from the past month for a 
    given station.
    
    Parameters
    -----------------------------------------------
    station_reference: str
                       Station reference
                       
    Returns
    -----------------------------------------------
    Dictionary indexed by datetime.
    '''
    
    url = 'https://environment.data.gov.uk/flood-monitoring/data/readings?_limit=10000'
    with urllib.request.urlopen(
        'https://environment.data.gov.uk/flood-monitoring/data/readings.json' +\
        f'?stationReference={station_reference}&_limit=10000'
    ) as url:
        file = pd.DataFrame(json.load(url)['items'])
    
    stndict = file.reindex(['dateTime', 'value'], axis=1).set_index('dateTime').to_dict()['value']
    stndict['stationReference'] = station_reference
    
    return stndict


def retrieve_CSV():
    '''
    Interactive script for retrieving CSV files.
    '''
    
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

    
    
def get_all_recent_readings(station_reference=None):
    '''
    Get all recent readings from the past month for a 
    given list of stations.
    
    Parameters
    -----------------------------------------------
    station_reference: str or array-like or None
                       List of stations
                       
    Returns
    -----------------------------------------------
    Pandas DataFrame indexed by datetime.
    '''
    
    if station_reference is None:
        usrinp = input("Downloading the full dataset would take approx. 60 mins " + \
                       "!!!Please ensure that output is saved to a variable!!! Proceed? [Y/N] ")

        if usrinp.upper() == "N":
            return None
    
    
    def handler(signalnum, frame):
        raise Exception
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(30)

    try:
        data = get_live_station_measures(param='')
        data = data[~data.index.duplicated(keep='last')]
    except:
        pass
    
    signal.alarm(0) 
    assert 'data' in locals(), "504 Gateway Timeout Error: Cannot fetch station list"
    
    print("List of live stations fetched successfully!")
    
    if station_reference is not None:
        if isinstance(station_reference, str):
            station_reference = [station_reference]
        data = data[data.index.isin(station_reference)]
    
        
    executor = ThreadPoolExecutor(max_workers=min(os.cpu_count()+4, len(data.index)))
    futures = []
    stnvallist = []
    
    for ref in tqdm(data.index.tolist()):
        futures.append(executor.submit(get_live_station_readings, ref))
    
    print("Download requested! Starting download ...")
    done,_ = wait(futures)
    print("Dataset downloaded! Saving to variable ...")
    
    for future in tqdm(done):
        try:
            stnvallist.append(future.result())
        except:
            continue

    executor.shutdown(wait=False)
    
    
    stnvaldf = pd.DataFrame(stnvallist).set_index('stationReference').transpose()
    
    print("Downloaded dataset now saved to variable")
            
    return stnvaldf
   
    
    
def download_station_info(station_reference):
    '''
    Get station location and range data for a given
    station.
    
    Parameters
    -----------------------------------------------
    station_reference: str
                       Station reference
                       
    Returns
    -----------------------------------------------
    Dictionary of station info.
    '''
    
    with urllib.request.urlopen(
        f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_reference}.json"
    ) as url:
        file = pd.json_normalize(json.load(url)['items'])
    
    try:
        file['maxOnRecord']=file['stageScale.maxOnRecord.value']
        file['minOnRecord']=file['stageScale.minOnRecord.value']
        file['typicalRangeHigh']=file['stageScale.typicalRangeHigh']
        file['typicalRangeLow']=file['stageScale.typicalRangeLow']
    except:
        pass
    
    stninfo = file.reindex(['stationReference', 'lat', 'long',
                            'easting', 'northing', 'maxOnRecord', 'minOnRecord',
                            'typicalRangeHigh', 'typicalRangeLow'], axis=1).to_dict('records')[0]
    return stninfo



def download_all_station_info(param="level", df=None):
    '''
    Get station location and range data for a given 
    list of stations.
    
    Parameters
    -----------------------------------------------
    station_reference: str or array-like or None
                       List of stations
                       
    Returns
    -----------------------------------------------
    Pandas DataFrame indexed by stationReference.
    '''
    
    if df is None:
        usrinp = input("Input DataFrame not specified: " + \
                       "Do you perhaps want to build upon an existing file? " + \
                       "(This will not overwrite the file.) [Y/N] ")
        
        if usrinp.upper() != "N":
            df = retrieve_CSV()
    
    
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
        data = data[~data.index.duplicated(keep='last')]
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
        futures.append(executor.submit(download_station_info, ref))
    
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
                        help = 'Station Parameter ("rainfall" or "level" or "")')
    parser.add_argument('-f', '--file', type = str, help = 'Write Filename')
    args = parser.parse_args()
    
    data = get_live_station_measures(station_reference=None, param=args.param, filename=args.file)
    
    print(data)
