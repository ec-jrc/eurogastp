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
"""Tools for downloading and analysing data from the GIE AGSI+
(https://agsi.gie.eu/) and ALSI (https://alsi.gie.eu/) transparency platforms.
"""

import pandas as pd
from tqdm.auto import tqdm
import pdb
import datetime as dt
import os
import numpy as np
import requests
import time
import itertools
from importlib import reload

st = pdb.set_trace

def download_gie_alsi(start_date, end_date, api_key,
                      proxy=None, timeout=60, delay=0,
                      countries=['be', 'hr', 'fr', 'gr', 'it', 'lt', 'nl', 'pl',
                                 'pt', 'es', 'gb*']):
    """Download data per country from the GIE ALSI (LNG) transparency platform.
    
    The GIE ALSI database contains data starting from 2012-01-01.
        
    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
        
        countries : list of countries for which LNG data shall be downloaded;
                    by default, downloading all countries (be, hr, fr, gr, it,
                    lt, nl, pl, pt, es, gb*); gr stands for Greece, gb* stands
                    for pre-Brexit Great Britain
        
    Returns:
    
        Dataframe, with the columns explained as follows:
    
        lngInventory : aggregated amount of LNG in the LNG tanks status at end
                       of gas day [ 1e3 m^3 ]
        sendOut      : aggregated gas flow out of the LNG facility, send-out
                       during gas day [GWh/d]
        dtmi         : declared total maximum inventory (lng storage capacity)
                       [1e3 m^3]
        dtrs         : declared total reference send-out (send-out capacity)
                       [GWh/d]
    """
    headers = {"x-key": api_key}
    if proxy:
        os.environ['http_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    lng_dict = {}
    for c in tqdm(countries):
        url = f'https://alsi.gie.eu/api/data/{c}?from={start_date}&till={end_date}'
        response = requests.get(url, headers=headers, timeout=timeout)
        df1 = pd.DataFrame(response.json()).replace('-', np.nan)
        df1['code'] = c
        lng_dict[c] = df1
        time.sleep(delay)
    df = pd.concat(lng_dict.values())
    float_cols = ['lngInventory', 'sendOut', 'dtmi', 'dtrs']
    df[float_cols] = df[float_cols].astype(float)
    df['gasDayStartedOn'] = pd.to_datetime(df['gasDayStartedOn'])
    df = df.drop(['info'], axis=1)
    df = df.set_index(['code', 'gasDayStartedOn'])
    df = df.sort_index()
    return df

def download_gie_alsi_per_terminal(start_date, end_date, api_key, 
                                   eics_file='topo/LSO_EIC.xlsx',
                                   proxy=None, timeout=60, delay=0,
                                   eics_engine=None):
    """Download data from the GIE ALSI (LNG) transparency platform per LNG
    terminal.
    
    The GIE ALSI database contains data starting from 2012-01-01.
        
    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        eics_file : a spreadsheet file containing the list of EIC code of each
                    facility; default: topo/LSO_EIC.xlsx
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
    
    Returns:
    
        Dataframe, with the columns explained as follows:
    
        lngInventory : aggregated amount of lng in the lng tanks status at end
                       of gas day [ 1e3 m^3 ]
        sendOut      : aggregated gas flow out of the LNG facility, send-out
                       during gas day [GWh/d]
        dtmi         : declared total maximum inventory (lng storage capacity)
                       [1e3 m^3]
        dtrs         : declared total referece send-out (send-out capacity)
                       [GWh/d]
    """
    if not start_date:
        start_date = dt.date(2012, 1, 1)
    if not end_date:
        today = dt.date.today()
        end_date = today - dt.timedelta(2)
    url = f'https://alsi.gie.eu/api/data/21W0000000000419/GB*/21X0000000013554?from={start_date}&till={end_date}'
    
    headers = {"x-key": api_key}
    if proxy:
        os.environ['http_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    
    eics = pd.read_excel(eics_file, index_col='Name', engine=eics_engine)
    facs = eics[eics.Type == 'LNG Terminal'].drop('TVB (Virtual balancing LNG tank)')
    float_cols = ['lngInventory', 'sendOut', 'dtmi', 'dtrs']
    
    lng_dict = {}
    for row in tqdm(list(facs.iterrows())):
        i, fac = row
        url = f'{fac.URL}?from={start_date}&till={end_date}'
        response = requests.get(url, headers=headers, timeout=timeout)
        df1 = pd.DataFrame(response.json()).replace('-', np.nan)
        df1[float_cols] = df1[float_cols].astype(float)
        df1['gasDayStartedOn'] = pd.to_datetime(df1['gasDayStartedOn'])
        df1 = df1.drop(['info'], axis=1)
        df1['Facility'] = i
        df1['Country'] = fac.Country[:2]
        if i in lng_dict:  #fac.Country.endswith('*'):
            lng_dict[i].update(df1)
        else:
            lng_dict[i] = df1
        time.sleep(delay)
    
    df = pd.concat(lng_dict.values())
    df = df.set_index(['Country', 'Facility', 'gasDayStartedOn'])
    df = df.sort_index()
    return df

def download_gie_agsi(start_date, end_date, api_key,
                      proxy=None, timeout=60, delay=0,
                      countries=['at', 'be', 'bg', 'hr', 'cz', 'dk', 'fr', 'de',
                                 'hu', 'it', 'lv', 'nl', 'pl', 'pt', 'ro', 'se',
                                 'sk', 'es']):
    """Download data from the GIE AGSI+ transparency platform per country.
    
    Units: GWh or GWh/d, depending on the column.
    
    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
    
        countries : list of countries for which UGS data shall be downloaded;
                    by default, downloading all countries (at, be, bg, hr, cz,
                    dk, fr, de, hu, it, lv, nl, pl, pt, ro, se, sk, es);
        
    Returns:
    
        Dataframe, with the columns explained as follows:
    
        gasInStorage       : Volume of gas in storage at end of gas day [ GWh ]
        consumption        : Annual consumption [ GWh ]
        consumptionFull    : Filling level compared to annual consumption [ % ]
        injection          : Amount of gas injected during gas day [ GWh/d ]
        withdrawal         : Amount of gas withdrawn during gas day [ GWh/d ]
        workingGasVolume   : 
        injectionCapacity  : Declared total maximum technical injection (DTMTI)
                             [ GWh/d ]
        withdrawalCapacity : Declared total maximum technical withdrawal
                             (DTMTW) [ GWh/d ]
        status             : either "confirmed" or "estimated"
        trend              : Daily increase or decrease of gas in storage [ % ]
        full               : percentage of working gas volume in storage [ % ]
    """
    headers = {"x-key": api_key}
    if proxy:
        os.environ['http_proxy'] = proxy
        os.environ['HTTP_PROXY'] = proxy
        os.environ['https_proxy'] = proxy
        os.environ['HTTPS_PROXY'] = proxy
    # API calls are now limited to 30 days.
    # In case specified period exceeds 30 days, make multiple calls.
        
    num_days = (end_date - start_date).days + 1
    num_days

    ugs_dict = {}
    for c in countries:
        ugs_dict[c] = pd.DataFrame()

    for c, offset_start in tqdm(list(itertools.product(countries, range(0, num_days, 30)))):
        offset_end = min(offset_start + 29, num_days - 1)
        from_date = start_date + dt.timedelta(offset_start)
        to_date = start_date + dt.timedelta(offset_end)
        #print(c, from_date, to_date)
        url = f'https://agsi.gie.eu/api?type=&country={c}&from={from_date}&till={to_date}'
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code != 200:
            print(f'Warning: Request {c},{from_date} resulted in HTML status code {response.status_code}.')
        if 'message' in response.json():
            print(f'{c},{from_date}: {response.json()["message"]}')
        ugs_dict[c] = ugs_dict[c].append(pd.DataFrame(response.json()['data'])[::-1])
        time.sleep(delay)

    df = pd.concat(ugs_dict.values())
    float_cols = ['consumption', 'consumptionFull', 'gasInStorage', 'injection',
                  'withdrawal', 'workingGasVolume', 'injectionCapacity',
                  'withdrawalCapacity', 'trend', 'full']
    df[float_cols] = df[float_cols].replace('-', '0').astype(float)
    df['gasDayStart'] = pd.to_datetime(df['gasDayStart'])
    df = df.drop(['name', 'url', 'info'], axis=1)
    df[['consumption', 'workingGasVolume']] = \
            df[['consumption', 'workingGasVolume']] * 1e3  # GWh
    df = df.set_index(['code', 'gasDayStart'])
    df = df.sort_index()
    return df

def update_gie_agsi_archive(start_date, end_date, api_key,
                            archive_file='GIE_TPs_Archive/GIE_AGSI_archive_GWh_d.xlsx',
                            proxy=None, timeout=60, delay=0):
    """Update an existing GIE AGSI+ archive, or create a new one if it does not
    yet exist. Is overwriting data for existing dates and adding data for new
    dates.

    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        archive_file : path to where the archive shall be kept; default:
                       GIE_TPs_Archive/GIE_AGSI_archive_GWh_d.xlsx
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
    
    Returns:
    
        Nothing.
    """
    df_new = download_gie_agsi(start_date, end_date, api_key=api_key,
                               delay=delay, proxy=proxy)
    if os.path.exists(archive_file):
        df = pd.read_excel(archive_file, index_col=[0, 1])
    else:
        df = pd.DataFrame()
    # take over dates from the old df that do not exist in the new one
    df_new2 = df_new.append(df[~df.index.isin(df_new.index)]).sort_index()    
    # create directories along path if they do not yet exist
    subdir_name = os.path.dirname(archive_file)
    os.makedirs(subdir_name, exist_ok=True)
    # save data
    df_new2.to_excel(archive_file, index_label=None)

def update_gie_alsi_archive(start_date, end_date, api_key,
                            archive_file='GIE_TPs_Archive/GIE_ALSI_archive_GWh_d.xlsx',
                            proxy=None, timeout=60, delay=0):
    """Update an existing GIE ALSI archive, or create a new one if it does not
    yet exist. Is overwriting data for existing dates and adding data for new
    dates.

    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        archive_file : path to where the archive shall be kept; default:
                       GIE_TPs_Archive/GIE_ALSI_archive_GWh_d.xlsx
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
    
    Returns:
    
        Nothing.
    """
    df_new = download_gie_alsi(start_date, end_date, api_key=api_key,
                               delay=delay, proxy=proxy)
    if os.path.exists(archive_file):
        df = pd.read_excel(archive_file, index_col=[0, 1])
    else:
        df = pd.DataFrame()
    # take over dates from the old df that do not exist in the new one
    df_new2 = df_new.append(df[~df.index.isin(df_new.index)]).sort_index()    
    # create directories along path if they do not yet exist
    subdir_name = os.path.dirname(archive_file)
    os.makedirs(subdir_name, exist_ok=True)
    # save data
    df_new2.to_excel(archive_file, index_label=None)

def update_gie_alsi_archive_per_terminal(start_date, end_date, api_key,
                                         eics_file='topo/LSO_EIC.xlsx',
                                         archive_file='GIE_TPs_Archive/GIE_ALSI_archive_per_terminal_GWh_d.xlsx',
                                         proxy=None, timeout=60, delay=0,
                                         eics_engine=None):
    """Update an existing GIE ALSI archive, or create a new one if it does not
    yet exist. Is overwriting data for existing dates and adding data for new
    dates.

    Parameters:
    
        start_date, end_date : datetime.date objects specifying the time frame
                               for the downloaded data
        
        api_key : personal API key; must be requested from GIE
        
        eics_file : a spreadsheet file containing the list of EIC code of each
                    facility; default: topo/LSO_EIC.xlsx
        
        archive_file : path to where the archive shall be kept; default:
                       GIE_TPs_Archive/GIE_ALSI_archive_per_terminal_GWh_d.xlsx
        
        proxy : if specified, set proxy to the given address; could include
                username, password and port number
                (example: "http://user:password@proxy-address:1234")
        
        timeout : set timeout for the API call, seconds; default: 60 seconds
        
        delay : time in seconds to wait between API calls
    
    Returns:
    
        Nothing.
    """
    df_new = download_gie_alsi_per_terminal(start_date, end_date, api_key=api_key,
                                            delay=delay, eics_file=eics_file,
                                            proxy=proxy, eics_engine=eics_engine)
    if os.path.exists(archive_file):
        df = pd.read_excel(archive_file, index_col=[0, 1, 2])
    else:
        df = pd.DataFrame()
    # take over dates from the old df that do not exist in the new one
    df_new2 = df_new.append(df[~df.index.isin(df_new.index)]).sort_index()    
    # create directories along path if they do not yet exist
    subdir_name = os.path.dirname(archive_file)
    os.makedirs(subdir_name, exist_ok=True)
    # save data
    df_new2.to_excel(archive_file, index_label=None)
