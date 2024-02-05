#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# eurogastp - Python tools for analyzing the European gas system
#
# Copyright notice
# ----------------
#
# Copyright (C) 2022 European Union
#
# Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
# the European Commission – subsequent versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the Licence.
# You may obtain a copy of the Licence at:
#
# https://joinup.ec.europa.eu/software/page/eupl5
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Licence is distributed on an "AS IS" basis, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Licence for the specific language governing permissions and limitations under
# the Licence.
#
"""Tools for downloading and analysing data from the ENTSOG Transparency
Platform (https://transparency.entsog.eu/).
"""

import pandas as pd
from tqdm.auto import tqdm
import pdb
import datetime as dt
from glob import glob
import os
import numpy as np
from collections import OrderedDict
import time
from importlib import reload
import http
import requests

st = pdb.set_trace

# map ENTSOG TP indicator names to names of the respective columns in the
# topology file
ind_col_map = {
    "Nomination": "nom",
    "Renomination": "renom",
    "Allocation": "alloc",
    "Physical Flow": "flow",
    "GCV": "gcv",
    "Wobbe Index": "wob",
    "Firm Technical": "firm",
    "Firm Booked": 'firmbooked',
    "Firm Available": "firmavail",
    "Interruptible Total": "inttot",
    "Interruptible Booked": "intbooked",
    "Interruptible Available": "intavail",
    "Planned interruption of firm capacity": "planinterruptfirm",
    "Unplanned interruption of firm capacity": "unplaninterruptfirm",
    "Planned interruption of interruptible capacity": "planinterruptint",
    "Actual interruption of interruptible capacity": "unplaninterruptint"
    }

eu_nodes_hgas = ['AT', 'BE', 'BG', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FR', 'GR', 'HR',
                 'HU', 'IE', 'SK', 'PT', 'IT', 'PL', 'LU', 'MT', 'CY', 'LV', 'LT',
                 'FI', 'SI', 'RO', 'SE', 'NL', 'PLYAM', 'TBP', 'TAP', 'IGB', 'BBL']

eu_nodes_lgas = ['BEL', 'DEL', 'FRL', 'NLL']

eu_nodes = eu_nodes_hgas + eu_nodes_lgas

non_eu_nodes = ['RU', 'BY', 'UA', 'TR', 'MA', 'DZ', 'TN', 'LY', 'UK', 'NO',
                'BA', 'MK', 'MD', 'RS', 'CH', 'AZ', 'GE', 'ME', 'AL', 'RUKAL',
                'IUK', 'TANAP', 'NI']

north_african_nodes = ['MA', 'DZ', 'TN', 'LY']

russian_origin_nodes = ['RU', 'BY', 'UA', 'TR']

balkan_nodes = ['BA', 'ME', 'RS', 'AL', 'MK']

def download_entsog_tp(start_date, end_date, topo, edges=None, dir_name=None,
                       indicators=['Physical Flow', 'Firm Technical',
                                   'Firm Booked', 'GCV', 'Nomination',
                                   'Renomination'],
                       proxy=None,
                       delay=0, overwrite=False, max_points_per_request=1,
                       show_api_call=False):
    """Download current raw data from the ENTSOG Transparency Platform using its
    API. This will create a folder named ENTSOG_TP_data_YYYY-MM-DD in the
    current working directory, where YYYY-MM-DD is the current date. In there,
    a separate folder for every year of data is created. In each subfolder, a
    separate downloaded file for each edge of the target topology as defined in
    the topology file *topo_file* is created.
    
    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        topo : DataFrame holding the topology data, as retrieved by the
               load_topo() function
        
        edges : List of edges to download, as specified in the topology file;
                if None, downloading all edges defined in the topology file
        
        dir_name : Name of the folder to store the downloaded files. By default,
                   using the name "ENTSOG_TP_data_YYYY-MM-DD", where YYYY-MM-DD
                   is the current date
        
        indicators : list of strings, specifying the indicators to download
                     (see the ind_col_map dictionary for a list of allowed
                     indicator names/short forms)
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        delay : time in seconds to wait between API calls
        
        overwrite : if True, overwrite existing files; otherwise, skip download
                    for already existing files (default)
        
        max_points_per_request : maximum number of network points put into one
                                 HTML request; if an edge has more network
                                 points defined, data will be split into
                                 multiple raw files. Default: 3

        show_api_call : if true, dump each API call into the console; for
                        debugging purposes
    Returns:
    
        error : If at least one of the downloads did not work (status code other
                than 200 or 404), return 1, otherwise return 0.
    """
    col_ind_map = {y: x for x, y in ind_col_map.items()}
    indicators = [col_ind_map.get(ind, ind) for ind in indicators]  # long names
    inds = [ind_col_map[i] for i in indicators]  # short names
    
    if not edges is None and not _is_iter(edges):
        edges = [edges]
    
    # set proxy
    if proxy:
        os.environ['http_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTPS_PROXY'] = proxy

    # set directory for downloaded data
    today = dt.date.today()
    date_str = today.strftime("%Y-%m-%d")

    # load list of edges of the target topology
    if edges is None:
        edges = topo.edge_name.unique()
    
    if dir_name is None:
        today = dt.date.today()
        date_str = today.strftime("%Y-%m-%d")
        dir_name = 'ENTSOG_TP_data_{}'.format(date_str)

    # download data
    years = range(end_date.year, start_date.year - 1, -1)
    #failed = set()
    error = 0
    for year in years:
        if min(years) == max(years):
            from_date = start_date
            to_date = end_date
        elif year == min(years):
            from_date = start_date
            to_date = '{}-12-31'.format(year)
        elif year == max(years):
            from_date = '{}-01-01'.format(year)
            to_date = end_date
        else:
            from_date = '{}-01-01'.format(year)
            to_date = '{}-12-31'.format(year)
        print('Period from {} until {}'.format(from_date, to_date))

        # create new dir for this week's download
        subdir_name = '{}/{}/'.format(dir_name, year)
        os.makedirs(subdir_name, exist_ok=True)
        
        print('Downloading data for {} edges.'.format(len(edges)))
        #status_codes = {}
        for edge_name in tqdm(edges):
            npoints = topo[topo.edge_name == edge_name]
            
            # exclude network points where all indicator cols are zero
            npoints = npoints[(npoints[inds] != 0).any(axis=1)]
            
            # need to do several requests if there are many network points in
            # this edge
            npgroups = list(_split_list(npoints, max_points_per_request))
            n_npgroups = len(npgroups)
            
            for npi, npgroup in enumerate(npgroups):
                nstr = str(npi + 1) if n_npgroups > 1 else ""
                filename = f'{subdir_name}/{edge_name}{nstr}.xlsx'
                if not overwrite and os.path.exists(filename):
                    continue
                
                ips = ','.join([''.join([t.operatorKey, t.pointKey, t.directionKey]).lower() for t in npgroup.itertuples()])
                api_call = 'https://transparency.entsog.eu/api/v1/operationalData.xlsx?forceDownload=true&' + \
                    f'pointDirection={ips}' + \
                    f'&from={from_date}&to={to_date}' + \
                    '&indicator={}&'.format(','.join([i.replace(' ', '%20') for i in indicators])) + \
                    'periodType=day&timezone=CET&' + \
                    'periodize=0&limit=-1&isTransportData=true&dataset=1'        
                if show_api_call:
                    print(api_call)
                
                try:
                    r = requests.get(api_call)  # verify=False, allow_redirects=False, stream=True)
                    
                    #r.raise_for_status()
                    if r.status_code == 404:
                        # This is completely normal in case no data is (up to now, or
                        # not anymore) reported for a certain IP for the chosen time
                        # interval, but we still want to include those points so that
                        # we can analyse also those periods for which there is data.
                        pass
                    elif r.status_code != 200:  # 200 would be good
                        print(f'Warning: Request for {year} {edge_name}{nstr} returned status code {r.status_code}')
                        #status_codes[edge_name] = r.status_code
                        #failed.add(edge_name)
                        error = 1
                    elif r.content == b'{"message":"No Data Available"}':
                        #print(f'Skipping {year} {edge_name}{nstr}, no data available')
                        pass
                    else:
                        with open(filename, 'wb') as f:
                            f.write(r.content)
                
                except http.client.RemoteDisconnected:
                    print(f'Warning: Request for {year} {edge_name} failed without response')
                    error = 1
                except requests.exceptions.ProxyError:
                    print(f'Warning: HTTP Error for {year} {edge_name}: Too many retries')
                    error = 1
                    time.sleep(60)  # wait extra amount of time
                
                time.sleep(delay)
    print('Done')
    return error

def _split_list(lst, n):  
    for i in range(0, len(lst), n): 
        yield lst[i:i + n]
        
def load_raw(dir_name=None, year=None,
             dir_name2='ENTSOG_TP_data_previous_years'):
    """Load raw ENTSOG TP data from folder.
    
    Parameters:
    
        dir_name  : path to the directory that contains the raw data downloaded
                    from the ENTSOG Transparency Platform. If not specfied, it
                    will look for a directory called ENTSOG_TP_data_YYYY-MM-DD,
                    where YYYY-MM-DD is the current date
        
        year      : specify to load a specific year / specific years (the
                    subfolders containing the respective data). Multiple years
                    can be specified using common globbing rules (see
                    documentation of glob.glob for more information). Default:
                    Load all years (all subdirectories)
        
        dir_name2 : secondary directory with data that should be merged with the
                    data from the primary one; it is thought to be holding data
                    from previous years that do not to be updated as often as
                    data from the current year
    
    Returns:
    
        raw :       DataFrame containing raw ENTSOG TP data, converted to GWh/d
                    (in case of GCV: GWh/MNm^3)
    """
    if dir_name is None:
        today = dt.date.today()
        date_str = today.strftime("%Y-%m-%d")
        dir_name = f'ENTSOG_TP_data_{today}'
    if year is None:
        year = '*'
    raw = pd.DataFrame()
    if os.path.isdir(dir_name2):
        print('Loading data from {}...'.format(dir_name2))
        for filename in tqdm(glob('{}/{}/*.xlsx'.format(dir_name2, year))):
            data = pd.read_excel(filename, usecols='C,E:G,I:K,M,Q,R',
                                 parse_dates=[1, 2])
            raw = pd.concat([raw, data])
    print('Loading data from {}...'.format(dir_name))
    for filename in tqdm(glob('{}/{}/*.xlsx'.format(dir_name, year))):
        try:
            data = pd.read_excel(filename, usecols='C,E:G,I:K,M,Q,R',
                                 parse_dates=[1, 2])
        except:
            print('Error: Failed to load raw file {}'.format(filename))
            raise
        raw = pd.concat([raw, data])
    #raw.drop_duplicates()
    raw.value = raw.value / 1e6  # convert to GWh/d / GWh/m3
    return raw

def raw_to_file(dir_name=None, dir_name2='ENTSOG_TP_data_previous_years',
                out_name=None):
    """Load raw data from given directory and save it to a single file (HDF5)
    for easier and faster access. Also loading data from the secondary directory
    *dir_name2* and merging it with the primary dataset. The resulting file
    will have the name [dir_name].h5.
    
    HDF5 file format is deemed necessary as Excel files can only contain ~1e6
    rows, and csv files would be unnecessarily large and slow to access. On the
    other hand, csv files would be better to exchange, as the data is put into
    HDF5 files in pickled form and might thus only be used with the same Pandas
    version.
    
    Parameters:

        dir_name  : path to the directory that contains the raw data downloaded
                    from the ENTSOG Transparency Platform. If not specfied, it
                    will look for a directory called ENTSOG_TP_data_YYYY-MM-DD,
                    where YYYY-MM-DD is the current date.
        
        dir_name2 : secondary directory with data that should be merged with the
                    data from the primary one; it is thought to be holding data
                    from previous years that do not to be updated as often as
                    data from the current year
    
        out_name  : name of the HDF5 to save the data to. If None, it will be
                    set to [dir_name].h5.
    Returns:
    
        Nothing.
    """
    if dir_name is None:
        today = dt.date.today()
        date_str = today.strftime("%Y-%m-%d")
        dir_name = 'ENTSOG_TP_data_{}'.format(date_str)
    if out_name is None:
        out_name = dir_name + '.h5'
    raw = load_raw(dir_name, dir_name2=dir_name2)
    print('Saving to {}...'.format(out_name))
    raw.to_hdf(out_name, key='raw', mode='w')
    print('Done.')

def load_raw_file(filename):
    """Load raw file. Can be either an Excel spreadsheet file (*.xls, *.xlsx), a
    file containing comma-separated values (*.csv), or a HDF5 file (*.h5). The
    file type is recognized by the corresponding filename ending.
    
    Parameters:
    
        filename : filename (with path) to the file containing the raw data.
        
    Returns:
        
        raw : raw dataset
    """
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        raw = pd.read_excel(filename, index_col=0)
    elif filename.endswith('.csv'):
        raw = pd.read_csv(filename, index_col=0)
    else:
        raw = pd.read_hdf(filename, key='raw')
    return raw

def load_topo(topo_file='topo/ENTSOG_TP_Network_v3.xlsx',
              sheets=['ITP', 'PRD', 'LNG', 'UGS', 'DIS', 'FNC', 'VTP']):
    """Load topology file.
    
    The topology file contains essentially a mapping from the data provided on
    the ENTSOG Transparency Platform onto a "target topology". It is defined in
    form of a spreadsheet file (Excel, or in principle any other spreadsheet
    file supported by the Pandas read_excel() function).
    
    Parameters:
    
        topo_file : path to the topology file. Default:
                    topo/ENTSOG_TP_Network_v3.xlsx
        
        sheets    : sheets to load from the spreadsheet file, corresponding to
                    the network point type. By default, loading all types of
                    network points (ITP, PRD, LNG, UGS, DIS, FNC, VTP).
                    
    Returns:
    
        topo : returning the topology mapping from the file, with all sheets
               merged into a single Pandas DataFrame
    """
    topo_raw = pd.read_excel(topo_file, sheets)
    topo = pd.concat(topo_raw)
    topo = topo[~topo.edge_name.isna()]
    return topo

def get_corridors(topo):
    """Return major natural gas pipeline inflow corridors to the European Union.
    
    The corridors are:
    - North Africa (Morocco, Algeria, Tunisia, Libya)
    - UK
    - North Sea (Norway)
    - East (gas of mainly Russian origin: Russia, Belarus, Ukraine, Türkiye)
    - Caspian (Azerbaijan)
    
    Parameters:
    
        topo : DataFrame with the topology mapping, as loaded via load_topo()
    
    Returns:
    
        corridors : collections.OrderedDict of lists of edges, corresponding to
                    the different inflow corridors
    """
    corridors = OrderedDict()
    corridors['North Africa'] = topo[topo.from_node.isin(['DZ', 'MA', 'TN', 'LY']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    corridors['UK'] = topo[topo.from_node.isin(['UK', 'IUK']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    corridors['North Sea'] = topo[topo.from_node.isin(['NO']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    corridors['East'] = topo[topo.from_node.isin(['RU', 'BY', 'UA', 'TR']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    corridors['Caspian'] = topo[topo.from_node.isin(['AZ', 'TANAP']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    return corridors

def get_routes(topo):
    """Return major natural gas pipeline inflow routes to the European Union.
    
    Some corridors (see get_corridors()) are further split into different
    routes, leading to this list of routes.
    
    The routes are:
    - North Africa -> ES
    - North Africa -> IT
    - UK
    - North Sea
    - East -> Nord Stream
    - East -> Baltic+Finland
    - East -> Yamal
    - East -> Ukraine
    - East -> Türkiye
    - Caspian
    
    Parameters:
    
        topo : DataFrame with the topology mapping, as loaded via load_topo()
    
    Returns:
    
        routes : collections.OrderedDict of lists of edges, corresponding to
                    the different inflow routes
    """
    routes = OrderedDict()
    routes['North Africa -> ES'] = topo[topo.from_node.isin(north_african_nodes) & topo.to_node.isin(['ES'])].edge_name.unique()
    routes['North Africa -> IT'] = topo[topo.from_node.isin(north_african_nodes) & topo.to_node.isin(['IT'])].edge_name.unique()
    routes['UK -> EU'] = topo[topo.from_node.isin(['UK', 'IUK']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    routes['North Sea'] = topo[topo.from_node.isin(['NO']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    routes['East -> Nord Stream'] = topo[topo.from_node.isin(['RU']) & topo.to_node.isin(['DE'])].edge_name.unique()
    routes['East -> Baltic+Finland'] = topo[topo.from_node.isin(russian_origin_nodes) & topo.to_node.isin(['FI', 'EE', 'LV', 'LT'])].edge_name.unique()
    routes['East -> Yamal'] = topo[topo.from_node.isin(['BY']) & topo.to_node.isin(['PL', 'PLYAM'])].edge_name.unique()
    routes['East -> Ukraine'] = topo[topo.from_node.isin(['UA']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    routes['East -> Türkiye'] = topo[topo.from_node.isin(['TR']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    routes['Caspian'] = topo[topo.from_node.isin(['AZ', 'TANAP']) & topo.to_node.isin(eu_nodes_hgas)].edge_name.unique()
    return routes

def filter_data(df, indicator=None, operatorKey=None, pointKey=None,
                directionKey=None, edge_name=None, from_node=None,
                to_node=None):
    """Convenience function to filter datasets such as raw, rexd, perd, and
    topo.
    
    Parameters:
    
    df : a DataFrame as retrieved from certain functions (load_raw(),
         reindex_by_period_endtime(), periodize(), reindex_and_periodize(),
         load_topo())
    
    all other parameters : column headers typically found in abovementioned
                           DataFrames; can be passed a single string with an
                           existing column name or a list of column names
    
    Returns:
    
        df2 : filtered DataFrame
    """
    df2 = df.copy()
    if indicator is not None:
        if not isinstance(indicator, list):
            indicator = [indicator]
        col_ind_map = {y: x for x, y in ind_col_map.items()}
        indicator = [col_ind_map.get(ind, ind) for ind in indicator]
        df2 = df2[df2.indicator.isin(indicator)]
    if operatorKey is not None:
        if not isinstance(operatorKey, list):
            operatorKey = [operatorKey]
        df2 = df2[df2.operatorKey.isin(operatorKey)]
    if pointKey is not None:
        if not isinstance(pointKey, list):
            pointKey = [pointKey]
        df2 = df2[df2.pointKey.isin(pointKey)]
    if directionKey is not None:
        if not isinstance(directionKey, list):
            directionKey = [directionKey]
        df2 = df2[df2.directionKey.isin(directionKey)]
    if edge_name is not None:
        if not isinstance(edge_name, list):
            edge_name = [edge_name]
        df2 = df2[df2.edge_name.isin(edge_name)]
    if from_node is not None:
        if not isinstance(from_node, list):
            from_node = [from_node]
        df2 = df2[df2.from_node.isin(from_node)]
    if to_node is not None:
        if not isinstance(to_node, list):
            to_node = [to_node]
        df2 = df2[df2.to_node.isin(to_node)]
    return df2

def _reindex_grp_by_period_endtime(df, start_date, end_date):
    index = pd.DatetimeIndex(sorted(pd.concat([df.periodFrom,
                                               df.periodTo]).unique()))
    df2 = df.sort_values(by=['periodTo', 'lastUpdateDateTime'])
    df2.drop_duplicates(subset='periodTo', keep='last', inplace=True)
    df2.set_index('periodTo', inplace=True)
    fill_keys = ['indicator', 'operatorKey', 'operatorLabel', 'pointKey',
                 'pointLabel', 'directionKey']
    df3 = df2.reindex(index=index)
    df3[fill_keys] = df3[fill_keys].ffill().bfill()
    df3.index.name = 'periodTo'
    df5 = df3.reset_index()
    
    # cut start
    length = df5.shape[0]
    if length > 0:
        first_date = df5.at[df5.index[0], 'periodTo'].to_pydatetime().date()
        if length > 1:
            second_date = \
                    df5.at[df5.index[1], 'periodTo'].to_pydatetime().date()
            df6 = df5.copy()
            if start_date > first_date:
                if start_date < second_date:
                    delta = start_date - first_date
                    df5.at[df5.index[0], 'periodTo'] = \
                            df5.at[df5.index[0], 'periodTo'] + delta
                else:
                    df5.at[df5.index[1], 'value'] = np.nan
                    df5.drop(df5.index[0], inplace=True)
        else:
            if start_date > first_date:
                df5.drop(df5.index[0], inplace=True)
    
    # cut end
    length = df5.shape[0]
    if length > 0:
        last_date = df5.at[df5.index[-1], 'periodTo'].to_pydatetime().date()
        if length > 1:
            second_last_date = \
                    df5.at[df5.index[-2], 'periodTo'].to_pydatetime().date()
            if end_date + dt.timedelta(1) < last_date:
                if end_date + dt.timedelta(1) > second_last_date:
                    delta = last_date - (end_date + dt.timedelta(1))
                    df5.at[df5.index[-1], 'periodTo'] = \
                            df5.at[df5.index[-1], 'periodTo'] - delta
                else:
                    df5.drop(df5.index[-1], inplace=True)
        else:
            if end_date + dt.timedelta(1) < last_date:
                df5.drop(df5.index[-1], inplace=True)
    
    df6 = df5.set_index('periodTo')
    return df6

def reindex_by_period_endtime(raw, start_date, end_date):
    """Pre-filter data downloaded from the ENTSOG Transparency Platform
    around the desired time period and re-index it by period end times.
    This essentially removes the column periodFrom and makes periodTo
    the index of the DataFrame. It also inserts additional rows with
    NaN values for periods with missing data (important for showing
    gaps when plotting). It will also add a row with a NaN value at the
    top to mark the beginning of the first period.
    
    The results can conveniently be plotted with a step plot, i.e.
    using the pandas expression
    
        rexd.value.plot(drawstyle='steps')
    
    Input parameters:
        
        raw : pandas.DataFrame containing the raw data from the ENTSOG TP
        
        start_date, end_date : datetime.date objects indicating period of
                               interest
        
    Returns:
        rexd : pandas.DataFrame containing the reindexed data
    """
    
    # 1. pre-filter data around period of interest
    start_time = pd.Timestamp(start_date)
    end_time = pd.Timestamp(end_date) + (dt.timedelta(1))
    df = raw[(raw.periodTo >= start_time) &
             (raw.periodFrom <= end_time)]
    
    # 2. re-index by period end time
    grp_keys = ['indicator', 'operatorKey', 'pointKey', 'directionKey']
    grp = df.groupby(grp_keys, as_index=False, group_keys=False)
    rexd = grp.apply(_reindex_grp_by_period_endtime,
                     start_date=start_date, end_date=end_date)
    rexd.drop('periodFrom', axis=1, inplace=True)
    #rexd.index.name = 'periodTo'
        
    return rexd

def _shift_last_row_to_first(a):
    b = a.copy()
    b[:-1] = a[1:]
    b[-1] = a[0]
    return b

def _periodize_grp2(t, start_date, end_date):
    # convert index to periodFrom
    t2 = t.copy()
    t2[['value', 'lastUpdateDateTime']] = \
            _shift_last_row_to_first(t[['value', 'lastUpdateDateTime']].values)
    t2.index.name = 'periodFrom'
    t2['date'] = t.index.normalize()
    # I know we do something wrong here, but what to do instead? If the limits
    # of the gas day are changing, we'll have to make some assumption
    # This case will be very seldom anyway
    # (see for example 1 Oct 2021, Firm Booked, BG-TSO-0001, PRD-00170, entry)
    t3 = t2.drop_duplicates(subset='date', keep='last').set_index('date')
    new_index = pd.date_range(start_date, end_date)
    new_index.name = 'date'
    t4 = t3.reindex(new_index, method='ffill')
    return t4

def periodize(rexd, start_date, end_date):
    """Periodize a previously reindexed dataset. The result will be a
    dataframe that contains one value per day (also no NaN value as first
    entry anymore).
    
    The results can be plotted the usual way in line plots or other types
    of plots, i.e. using the Pandas expression
    
        perd.value.plot()
    
    Input parameters:
        
        rexd : pandas.DataFrame containing the reindexed data from the
               ENTSOG TP, generated using the function
               reindex_by_period_endtime()
        
        start_date, end_date : datetime.date objects indicating period of
                               interest
                               
    Returns:
        perd : pandas.DataFrame containing the periodized data
    """
    grp_keys = ['indicator', 'operatorKey', 'pointKey', 'directionKey']
    grp = rexd.groupby(grp_keys, as_index=False, group_keys=False)
    perd = grp.apply(_periodize_grp2, start_date=start_date, end_date=end_date)
    return perd

def reindex_and_periodize(raw, start_date, end_date):
    """Convenience function that calls both reindex_by_period_endtime() and
    periodize() on a given raw dataset downloaded from the ENTSOG
    Transparency Platform.
    
    Input parameters:
        
        raw : pandas.DataFrame containing the raw data from the ENTSOG TP
        
        start_date, end_date : datetime.date objects indicating period of
                               interest
        
    Returns:
        
        perd : pandas.DataFrame containing the re-indexed and periodized data
    """
    rexd = reindex_by_period_endtime(raw, start_date, end_date)
    perd = periodize(rexd, start_date, end_date)
    return perd
    
def select_and_aggregate(edges, topo, df, indicator, quiet=False):
    """Select data (as edges of target topology) and aggregate.
    
    Input parameters:
    
        edges     : list of edge names to aggregate (either str or list of str)
        
        topo      : topology mapping
        
        df        : input data from ENTSOG Transparency Platform, already
                    reindexed and periodized
    
        indicator : indicator of interest (e.g. Firm Booked, Firm Technical,
                    Physical Flow, ...)
                    see the dictionary ind_col_map for possible values (both
                    keys and values can be used to refer to an indicator)
    
    Returns:
    
        pandas.DataFrame with aggregated data of selected indicator
    """
    col_name = ind_col_map.get(indicator, indicator)
    col_ind_map = {y: x for x, y in ind_col_map.items()}
    ind_name = col_ind_map.get(indicator, indicator)
    dfi = df[df.indicator == ind_name].reset_index()
    if not _is_iter(edges):
        edges = [edges]
    ind = pd.DataFrame(columns=['edge', 'date', 'value'])
    for edge in edges:
        edges2 = topo[(topo.edge_name == edge) &
                      (topo[col_name] != 0)]
        
        # do not tolerate unclassified raw data
        if edges2[col_name].isna().sum():
            raise(ValueError('Check topology file, there are NaNs in the ' +
                             'column for indicator {}. '.format(indicator) +
                             'Need to make a decision of how to use the ' +
                             'data'))
            
        # identify aggregation strategy
        if 1 in edges2[col_name].values:
            # check that there is not more than one "1" in the table
            if (edges2[col_name] == 1).sum() > 1:
                raise(ValueError('There is more than one number 1 for edge ' +
                                 '{} for indicator {}'.format(edge, indicator)))
            edges2 = edges2[edges2[col_name] == 1]
            strategy = "take"
        elif len(edges2[col_name]) == 0:
            strategy = "ignore"
        else:
            # check that there is not more than one aggregation strategy used
            # (min, av or sum)
            if len(edges2[col_name].unique()) > 1:
                raise(ValueError('There is more than one aggregation ' +
                                 'strategy used for edge ' +
                                 '{} for indicator {}'.format(edge,
                                                              indicator)))
            strategy = edges2[col_name].unique()[0]
        
        dfs = pd.DataFrame()
        for e in edges2.itertuples():
            df2 = dfi[(dfi.pointKey == e.pointKey) &
                      (dfi.operatorKey == e.operatorKey) &
                      (dfi.directionKey == e.directionKey)]
            dfs = pd.concat([dfs, df2])
        
        if len(dfs):  # if not empty
            if strategy == "take":
                val = dfs[['date', 'value']].set_index('date')
            elif strategy == "sum":
                val = dfs.groupby('date').value.sum()
            elif strategy in ["av", "mean"]:
                val = dfs.groupby('date').value.mean()
            elif strategy == "min":
                val = dfs.groupby('date').value.min()
            elif strategy == "max":
                val = dfs.groupby('date').value.max()
            else:
                raise(ValueError('Unknown aggregation strategy: ' +
                                 '{} (in edge {})'.format(strategy, edge)))

            dfv = pd.DataFrame(val).reset_index()
            dfv['edge'] = edge
            ind = pd.concat([ind, dfv])
        else:
            if not quiet:
                print('Warning: The edge {} '.format(edge) +
                      'was not found in the dataset')
    
    ind = ind.reset_index(drop=True).set_index(['date', 'edge']).unstack()
    ind.columns = ind.columns.get_level_values(1)
    #ind = ind.rename({'value': col_name}, axis=1)
    #if ind.shape[1] == 1:
    #    ind = ind.iloc[:, 0]
    return ind

def filter_nodes(topo, from_node=None, to_node=None):
    """Filter topology file by pair of nodes *from_node* and *to_node*. Can also
    specify multiple nodes for *from_node* or *to_node*.
    """
    df = topo.copy()
    if from_node is not None:
        if not _is_iter(from_node):
            from_node = [from_node]
        df = df[df.from_node.isin(from_node)]
    if to_node is not None:
        if not _is_iter(to_node):
            to_node = [to_node]
        df = df[df.to_node.isin(to_node)]
    return df.edge_name.unique().tolist()

def _zscore(df, ddof=0):
    return (df - df.mean()) / df.std(ddof=ddof)

def remove_outliers(df, threshold=3, ddof=0, inplace=False):
    """Remove outliers. This is done by replacing values with a Z-score of 3 or
    larger by NaN.
    
    Parameters:
    
        df        : pandas.DataFrame containing the data
        
        threshold : threshold for Z-score values; data with a Z-score larger
                    than or equal to `threshold` are being replaced by NaN
        
        ddof      : Delta degrees of freedom. This value is passed to the
                    function pandas.DataFrame.std() (standard deviation).
                    Default value is 0 (data is normalized by N instead of N-1).
        
        inplace   : if True, replace outliers by NaN in-place, modifying the
                    original dataset `df`
    
    Returns:
    
        df2     : copy of the original DataFrame `df` with the identified
                  outliers replaced by NaN; only if inplace=False, otherwise
                  None is returned
    """
    df2 = df if inplace else df.copy()
    df2[(np.abs(_zscore(df2, ddof=ddof)) >= threshold)] = np.nan
    return None if inplace else df2

def _is_iter(obj):
    """Checks if *obj* is iterable but not a string.
    """
    return not isinstance(obj, str) and hasattr(obj, '__iter__')

def get_display_names(edge_names, topo):
    """For the given *edge_names*, return the corresponding edge display names
    as defined in the topology *topo* (field "edge_display_name"). Edge names
    not existing in the topology are silently passed through.
    """
    cols = ['edge_name', 'edge_display_name']
    nice_names = dict(topo[cols].drop_duplicates().values.tolist())
    names = [nice_names.get(n, n) if nice_names.get(n, n) else n for n in edge_names]
    return names

def display(df, topo):
    """Return version of the data frame *df* with the edge names in the column
    headers replaced by the corresponding edge display name as defined in the
    topology *topo* (field "edge_display_name"). Column headers not existing in
    the topology are silently passed through.
    """
    df2 = df.copy()
    df2.columns = get_display_names(df2.columns, topo)
    return df2
