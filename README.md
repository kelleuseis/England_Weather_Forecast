# England Weather Forecast :sun_behind_rain_cloud:

### Python Package for interacting with rainfall/river/tide/groundwater level data via DEFRA UK Flood Monitoring API.

## Contents

<code>dataload.py</code>: for accessing archive data

<code>live.py</code>: for accessing latest readings

<code>datasets.py</code>: contains station information, and for offline access of downloaded databases 

<code>geovisual.py</code>: for generating maps of level data using PyGMT

<code>preprocess.py</code>: for transforming CSV/DataFrames into datasets suitable for time series model training

## Installation Guide

To install the required dependencies run:

```bash
conda env create -f environment.yml
```

The package can then be installed via:

```bash
python setup.py install --user
```




