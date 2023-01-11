"""Preprocessing Module for Machine Learning Purposes"""

import numpy as np
import pandas as pd

import torch
import torch.nn as nn
from torch.utils.data import Dataset
            
class TsCSVDataset(Dataset):
    '''Generate Pytorch timeseries dataset from csv file'''
    
    def __init__(self, path, sequence_length, lag=1, 
                 mean=None, std=None, 
                 train_size=0.7, train=True):
        
        dataset = pd.read_csv(path)
        
        if len(dataset.columns) > 1:
            dataset.iloc[:,-1] = dataset.iloc[:,-1].shift(-lag, axis=0)
            dataset = dataset.iloc[:-lag]
        
        ttsidx = int(train_size*len(dataset.index))
        
        if mean is None and std is None:
            self.mean = dataset[:ttsidx].mean()
            self.std = dataset[:ttsidx].std()
            
        dataset  = (dataset - self.mean)/self.std

        if train:
            dataset = dataset.iloc[:ttsidx]
        else:
            dataset = dataset.iloc[ttsidx:]
            
        self.sequence_length = sequence_length
            
        if len(dataset.columns) > 1:
            self.targets = torch.tensor(dataset.iloc[:,-1].values, dtype=torch.float32)
            self.data = torch.tensor(dataset.iloc[:,:-1].values, dtype=torch.float32)
        else:
            self.targets = torch.tensor(dataset.iloc[lag:].values, dtype=torch.float32)
            self.data = torch.tensor(dataset.iloc[:-lag].values, dtype=torch.float32)         

        
    def __len__(self):
        return len(self.targets)

    
    def __getitem__(self, idx): 
        
        if idx >= self.sequence_length - 1:
            idx_start = idx - self.sequence_length + 1
            batch = self.data[idx_start:(idx+1)]
        else:
            padding = self.data[0].repeat(self.sequence_length-idx-1, 1)
            batch = self.data[0:(idx+1)]
            batch = torch.cat((padding, batch), 0)

        return batch, self.targets[idx]