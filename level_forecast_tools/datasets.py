"""For offline interactions"""

import pandas as pd
import sqlalchemy as sa
import os
from pandas.io import sql

class station_info():
    
    def __init__(self):
        data_path = os.sep.join((os.path.dirname(__file__), 'data'))
        self.defralevel = pd.read_csv(datapath + 'level_stations.csv')
        self.defrarain = pd.read_csv(datapath + 'rain_stations.csv')

    
    
class database_info():
    
    def __init__(self):
        
        self.dbpath = os.sep.join((os.path.dirname(__file__), 
                                   'data/archive/csv_database.db'))
        self.engine = sa.create_engine('sqlite:///' + self.dbpath, 
                                       connect_args={'timeout': 15})
        
        
    def infolist(self):
        '''Print database information'''
        
        inspector = sa.inspect(self.engine)
    
        for tbname in inspector.get_table_names():
            print(f"Table: {tbname}    Size: {os.path.getsize(self.dbpath)}")
            
            try:
                print(f"Entries: {self.engine.execute(f'SELECT COUNT(value) FROM {tbname}').scalar()}")
                print(f"Start: {self.engine.execute(f'SELECT MIN(dateTime) FROM {tbname}').scalar()}")
                print(f"End: {self.engine.execute(f'SELECT MAX(dateTime) FROM {tbname}').scalar()}")
            except:
                print("Corrupted Table")
                
            print("================================")
                
                
    def delete(self, tbname):
        '''Delete table from database if exists'''
        sql.execute(f'DROP TABLE IF EXISTS {tbname}', self.engine)

    
            