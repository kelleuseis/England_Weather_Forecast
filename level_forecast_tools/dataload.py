import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime, timedelta

from tqdm import tqdm
import urllib.request
import contextlib
from itertools import cycle, chain
from pandas.io import sql

start_def = datetime.today()-timedelta(days=730)
end_def = datetime.today()-timedelta(days=2)


def generate_dtlist(start=start_def, end=end_def, count=1):
    if not isinstance(start, datetime): 
        start = datetime.strptime(
            ''.join(filter(str.isdigit, str(start)))[14::-1].zfill(14)[::-1], 
            "%Y%m%d%H%M%S")
    
    if not isinstance(end, datetime): 
        end = datetime.strptime(
            ''.join(filter(str.isdigit, str(end)))[14::-1].zfill(14)[::-1], 
            "%Y%m%d%H%M%S")
    
    assert start >= start_def and start < end_def, \
            "Please choose a reasonable time range"
    assert end > start_def and end <= end_def, \
            "Please choose a reasonable time range"
    
    return pd.date_range(start, end, int(count+1)).tolist()



def get_archive_data(start=start_def, end=end_def, 
                     overwrite=True, tbname='temp'):
    
    dtlist = generate_dtlist(start, end)
    
    dbpath = os.sep.join((os.path.dirname(__file__), 
                          'data/archive/csv_database.db'))
    csvdb = sa.create_engine('sqlite://' + dbpath, 
                             connect_args={'timeout': 15})
    if overwrite:
        sql.execute(f'DROP TABLE IF EXISTS {tbname}', csvdb)
    
    load_ani = cycle(list("\|/-"))
    
    
    for ts in tqdm(pd.date_range(dtlist[0], dtlist[-1]).tolist()):
        csvurl = "https://environment.data.gov.uk/flood-monitoring/"+\
                    f"archive/readings-full-{ts.strftime('%Y-%m-%d')}.csv"
    
        with contextlib.closing(urllib.request.urlopen(url=csvurl)) as rd:
            for chunk in pd.read_csv(rd, chunksize=10000):
                print(next(load_ani) + " Loading ...", end="\r")
                chunk.value = pd.to_numeric(chunk.value, errors="coerce")
                chunk[['dateTime','stationReference',
                       'value']].to_sql(tbname, csvdb, if_exists='append', 
                                        index=False)
                
    return None



def load_archive_data(staref, start=start_def, end=end_def, 
                      count=1, train=False, lag=2, tbname='temp', 
                      outdir='data/archive'):
    
    dbpath = os.sep.join((os.path.dirname(__file__), 
                          'data/archive/csv_database.db'))
    csvdb = sa.create_engine('sqlite://' + dbpath, 
                             connect_args={'timeout': 15})
    if not sa.inspect(csvdb).has_table(tbname):
        get_archive_data(start, end, tbname)
    
    if isinstance(staref, str):
        staref = [staref]
    staref = str(staref).replace('[','(').replace(']',')')
    
    dtlist = generate_dtlist(start, end, count)
    
    for idx, ts in enumerate(tqdm(dtlist)):
        try:
            upbound = dtlist[idx+1].strftime("%Y-%m-%dT%H:%M:%SZ")
            lowbound = dtlist[idx].strftime("%Y-%m-%dT%H:%M:%SZ")

            valser = pd.read_sql_query(
                f'SELECT stationReference, value FROM {tbname} ' +\
                f'WHERE (stationReference in {staref}) ' +\
                f'AND (dateTime < "{upbound}") ' +\
                f'AND (dateTime >= "{lowbound}")', csvdb
            ).groupby('stationReference').value.apply(list).apply(pd.Series).transpose()


            if train:
                valser.iloc[:,-1] = valser.iloc[:,-1].shift(lag, axis=0)
            
            outpath = os.sep.join((os.getcwd(), outdir, 
                                   f'valser_{lowbound[:13]}.tmp'))
            with open(outpath,'w') as outfile:
                outfile.write(valser.to_csv(index=False))
        
        except:
            continue
        
            
    return None
