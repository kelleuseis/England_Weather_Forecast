import os

import numpy as np
import pandas as pd
import pygmt
from sklearn.neighbors import NearestNeighbors

import live


class Ellipsoid(object):
    """ Data structure for a global ellipsoid. """

    def __init__(self, a, b, F_0):
        self.a = a
        self.b = b
        self.n = (a - b) / (a + b)
        self.e2 = (a ** 2 - b ** 2) / a ** 2
        self.F_0 = F_0
        self.H = 0


class Datum(Ellipsoid):
    """ Data structure for a global datum. """

    def __init__(self, a, b, F_0, phi_0, lam_0, E_0, N_0, H):
        super().__init__(a, b, F_0)
        self.phi_0 = phi_0
        self.lam_0 = lam_0
        self.E_0 = E_0
        self.N_0 = N_0
        self.H = H


def get_easting_northing_from_gps_lat_long(phi, lam, rads=False):
    """ Get OSGB36 easting/northing from GPS latitude and longitude pairs.

    Parameters
    ----------
    phi: float/arraylike
        GPS (i.e. WGS84 datum) latitude value(s)
        
    lam: float/arrayling
        GPS (i.e. WGS84 datum) longitude value(s).
        
    rads: bool
        If true, specifies input is is radians.
        
    Returns
    -------
    numpy.ndarray
        Easting values (in m)
        
    numpy.ndarray
        Northing values (in m)
        
    Examples
    --------
    >>> get_easting_northing_from_gps_lat_long([55.5], [-1.54])
    (array([429157.0]), array([623009]))
    
    References
    ----------
    Based on the formulas in "A guide to coordinate systems in Great Britain".
    See also https://webapps.bgs.ac.uk/data/webservices/convertForm.cfm
    """
    dat = Datum(a=6377563.396, b=6356256.910, F_0=0.9996012717, 
                phi_0=np.deg2rad(49.0), lam_0=np.deg2rad(-2.), 
                E_0=400000, N_0=-100000, H=24.7)

    ell = Ellipsoid(a=6378137, b=6356752.3142, F_0=0.9996)

    if rads == False:
        phi = np.deg2rad(np.asarray(phi))
        lam = np.deg2rad(np.asarray(lam))

    n = (dat.a - dat.b) / (dat.a + dat.b)
    v = dat.a * dat.F_0 * (1 - dat.e2 * np.sin(phi) ** 2) ** (-0.5)

    rho = dat.a * dat.F_0 * (1 - dat.e2) * (1 - dat.e2 * np.sin(phi) ** 2) ** (-1.5)
    eta2 = v / rho - 1

    M = dat.b * dat.F_0 * ((1 + n + 5 / 4 * n ** 2 + 5 / 4 * n ** 3) * (phi - dat.phi_0) - (
            3 * n + 3 * n ** 2 + 21 / 8 * n ** 3) * np.sin(phi - dat.phi_0) * np.cos(phi + dat.phi_0) + \
                           (15 / 8 * n ** 2 + 15 / 8 * n ** 3) * np.sin(2 * (phi - dat.phi_0)) * np.cos(
                2 * (phi + dat.phi_0)) - 35 / 24 * n ** 3 * np.sin(3 * (phi - dat.phi_0)) * np.cos(3 * (phi - dat.phi_0)))

    I = M + dat.N_0
    II = v / 2 * np.sin(phi) * np.cos(phi)
    III = v / 24 * np.sin(phi) * np.cos(phi) ** 3 * (5 - np.tan(phi) ** 2 + 9 * eta2)
    IIIA = v / 720 * np.sin(phi) * np.cos(phi) ** 5 * (61 - 58 * np.tan(phi) ** 2 + np.tan(phi) ** 4)
    IV = v * np.cos(phi)
    V = v / 6 * np.cos(phi) ** 3 * (v / rho - np.tan(phi) ** 2)
    VI = v / 120 * np.cos(phi) ** 5 * (5 - 18 * np.tan(phi) ** 2 + np.tan(phi) ** 4 + 14 * eta2 - 58 * (np.tan(phi) ** 2) * eta2)

    N = I + II * (lam - dat.lam_0) ** 2 + III * (lam - dat.lam_0) ** 4 + IIIA * (lam - dat.lam_0) ** 6
    E = dat.E_0 + IV * (lam - dat.lam_0) + V * (lam - dat.lam_0) ** 3 + VI * (lam - dat.lam_0) ** 5

    return np.array(E), np.array(N)



def riverplt(plt_range=1.5, live_data=False, showplt=True, filename=None):
    '''
    Function to plot river values across England and Wales.
    Value is derived from current river level divided by typical
    max range.

    Parameters
    ----------------------------------------------
    plt_range:  float or int
                Sets max range of colorbar. Default value
                is 1.5.

    live_data:  bool
                Returns live data from Real Time API. Default
                option is False. NOTE: Getting live data may
                take a while.

    showplt:    bool
                Plots figure in kernel. Default True.

    filename:   str or None
                filename to save plot figure. Default None.

    Returns
    ----------------------------------------------
    Pandas Dataframe of river value interpolation. Pygmt 
    figure of interpolated river values across England and
    Wales.
    '''

    riverdata_path = os.sep.join((os.path.dirname(__file__),
                                  'data',
                                  'riverdata.csv'))
    river_df = pd.read_csv(riverdata_path)

    if live_data == True:
        try:
            stareflist = pd.read_csv(riverdata_path).stationReference
            print(f'Loading Data for {len(stareflist)} stations...')
            riverlive_df = live.get_live_station_data(station_reference=stareflist, param='level')
            print('Data Loaded!')
            river_df.latestReading = riverlive_df.latestReading.tolist()
        except:
            print('Connection Timed Out: Switching to default values')

    riversta_lat = river_df.lat.tolist()
    riversta_long = river_df.long.tolist()
    riversta_val = (river_df.latestReading / river_df.typicalRangeHigh).tolist()

    riversta_arr = pygmt.blockmean(x=riversta_long, y=riversta_lat, z=riversta_val, region="-5.5/2/50/55",
                                   spacing=0.05).to_numpy()
    rsinterp_grd = pygmt.surface(data=riversta_arr, region="-5.5/2/50/55", spacing=0.05)
    rsinterp_xyz = pygmt.grd2xyz(rsinterp_grd)

    fig = pygmt.Figure()
    fig.basemap(region="-5.5/2/50/55", projection="M15c", frame="ag")
    fig.grdimage(rsinterp_grd, cmap=pygmt.makecpt(cmap='panoply', series=f'-0.75/{plt_range}', background='o'))
    fig.coast(rivers='a', shorelines='1/0.5p', water='white')
    fig.colorbar(frame=["x+lStage"], position="+e")

    if showplt == True:
        fig.show()

    if filename is not None:
        fig.savefig(filename)

    return rsinterp_xyz,fig



def rainplt(plt_range=0.3, live_data=False, showplt=True, filename=None):
    '''
    Function to plot rainfall values across England and Wales.

    Parameters
    ----------------------------------------------
    plt_range:  float or int
                Sets max range of colorbar (in mm). Default
                value is 0.3.

    live_data:  bool
                Returns live data from Real Time API. Default
                option is False. NOTE: Getting live data may
                take a while.

    showplt:    bool
                Plots figure in kernel. Default True.

    filename:   str or None
                filename to save plot figure. Default None.

    Returns
    ----------------------------------------------
    Pandas Dataframe of rainfall interpolation. Pygmt 
    figure of interpolated rain values across England and
    Wales.
    '''

    raindata_path = os.sep.join((os.path.dirname(__file__),
                                 'data',
                                 'raindata.csv'))
    rain_df = pd.read_csv(raindata_path)

    if live_data == True:
        try:
            stareflist = pd.read_csv(raindata_path).stationReference
            print(f'Loading Data for {len(stareflist)} stations...')
            rainlive_df = live.get_live_station_data(station_reference=stareflist)
            print('Data Loaded!')
            rain_df.latestReading = rainlive_df.latestReading.tolist()
        except:
            print('Connection Timed Out: Switching to default values')

    rainsta_lat = rain_df.lat.tolist()
    rainsta_long = rain_df.long.tolist()
    rainsta_val = rain_df.latestReading.tolist()

    rainsta_arr = pygmt.blockmean(x=rainsta_long, y=rainsta_lat, z=rainsta_val, region="-5.5/2/50/55",
                                  spacing=0.05).to_numpy()
    rsinterp_grd = pygmt.surface(data=rainsta_arr, region="-5.5/2/50/55", spacing=0.05)
    rsinterp_xyz = pygmt.grd2xyz(rsinterp_grd)

    fig = pygmt.Figure()
    fig.basemap(region="-5.5/2/50/55", projection="M15c", frame="ag")
    fig.grdimage(rsinterp_grd, cmap=pygmt.makecpt(cmap='rainbow', series=f'0/{plt_range}', background='o'))
    fig.coast(rivers='a', shorelines="1/0.5p", water='white')
    fig.colorbar(frame=["x+lRainfall", "y+lmm"], position="+e")

    if showplt == True:
        fig.show()

    if filename is not None:
        fig.savefig(filename)

    return rsinterp_xyz,fig



def tideplt(showplt=True, filename=None):
    '''
    Function to plot tidal range values across England and Wales.

    Parameters
    ----------------------------------------------
    filename:   str or None
                filename to save plot figure. Default None.

    Returns
    ----------------------------------------------
    Pandas Dataframe of tidal range value interpolation.
    Pygmt figure of interpolated tide range values across 
    England and Wales.
    '''

    tidedata_path = os.sep.join((os.path.dirname(__file__),
                                 'data',
                                 'tidedata.csv'))
    tide_df = pd.read_csv(tidedata_path)

    tidesta_lat = tide_df.lat.tolist()
    tidesta_long = tide_df.long.tolist()
    tidesta_val = tide_df.latestReading.tolist()

    tsinterp_grd = pygmt.surface(x=tidesta_long, y=tidesta_lat, z=tidesta_val, region="-5.5/2/50/55", spacing=0.05)
    tsinterp_xyz = pygmt.grd2xyz(tsinterp_grd)

    fig = pygmt.Figure()
    fig.basemap(region="-5.5/2/50/55", projection="M15c", frame="ag")
    fig.grdimage(tsinterp_grd, cmap='panoply')
    fig.coast(rivers='a', shorelines="1/0.5p", land='white')
    fig.plot(x=tidesta_long, y=tidesta_lat, style="s0.2c", color=tidesta_val, cmap='panoply')
    fig.colorbar(frame=["x+lTidal Range", "y+lm(AOD)"])

    if showplt == True:
        fig.show()

    if filename is not None:
        fig.savefig(filename)
    
    return tsinterp_xyz,fig



def rrt_value(long, lat, from_live=False):
    '''
    Search for river, rain, tide and nearest coast data for lat-long inputs.
    For England and Wales.

    Parameters
    ---------------------------------------------------
    long:   str or arraylike
            Longitude of a list of points or a single
            point

    lat:    str or arraylike
            Latitude of a list of points or a single point

    from_live:  bool
                Returns live data from Real Time API. Default
                value is False. Only for river and rain
                values.

    Returns
    ---------------------------------------------------
    Easting, Northing, river, rain, distance to nearest
    coast (in m) and tide range data for each lat-long pair 
    in a Pandas DataFrame.
    '''
    eas, nor = get_easting_northing_from_gps_lat_long(lat, long)

    coordf = pd.DataFrame({'Longitude': long, 'Latitude': lat,
                           'Easting': eas, 'Northing': nor})
    coordf[['x', 'y']] = coordf[['Longitude', 'Latitude']].apply(
        lambda x: np.round(np.round(x / 0.05, 0) * 0.05, 2)
    )

    
    river_df,_ = riverplt(showplt=False,live_data=from_live)
    rain_df,_ = rainplt(showplt=False,live_data=from_live)

    tide_df,_ = tideplt(showplt=False)
    tide_df['Easting'], tide_df['Northing'] = get_easting_northing_from_gps_lat_long(
        tide_df.y.tolist(), tide_df.x.tolist()
    )
    tide_df = pygmt.select(tide_df, mask='k/s/k/s/k')
    coast = NearestNeighbors(n_neighbors=1)
    coast.fit(tide_df[['Easting', 'Northing']])

    def retrval(infodf, loc):
        try:
            return infodf[(infodf.x == loc.x) & (infodf.y == loc.y)].z.values[0]
        except IndexError:
            return np.nan

    coordf['riv_val'] = coordf.apply(lambda x: retrval(river_df, x), axis=1)
    coordf['rain_val'] = coordf.apply(lambda x: retrval(rain_df, x), axis=1)

    coordf['coast_dist'],coordf['tide_val'] = coast.kneighbors(coordf[['Easting','Northing']])
    coordf.tide_val = coordf.tide_val.apply(lambda x: tide_df.iloc[x].z)
    
    return coordf.drop(columns=['x','y'])



def plot_circle(long,lat,radius=25,gmt_fig=None):
    '''
    Function to plot circle(s) on a map (creating
    a new pygmt figure instance if necessary).

    Parameters
    ----------------------------------------------
    lat: float or arraylike
         Latitude of circle(s) to plot (degrees)
        
    lon: float or arraylike
         Longitude of circle(s) to plot (degrees)
        
    radius: float
            Radius of circle(s) to plot (km)
        
    map: pygmt.Figure
         Existing pygmt figure object

    Returns
    -----------------------------------------------
    Pygmt Figure object
    '''
    if gmt_fig is not None:
        gmt_fig.plot(x=long,y=lat,style=f'E{radius*2}',transparency=40,color='lightblue')
        gmt_fig.plot(x=long,y=lat,style='E5',transparency=40,color='red')
    else:
        gmt_fig = pygmt.Figure()
        gmt_fig.basemap(region="-5.5/2/50/55",projection="M15c",frame="ag")
        gmt_fig.plot(x=long,y=lat,style=f'E{radius*2}',transparency=40,color='lightblue')
        gmt_fig.plot(x=long,y=lat,style='E5',transparency=40,color='red')

    return gmt_fig