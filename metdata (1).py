#!/usr/bin/env python3

from logging import warning
from io import StringIO
from os.path import basename, splitext
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests

MAIN_URL = 'https://www.metoffice.gov.uk/research/climate/maps-and-data/historic-station-data'

# persistent mapping of station names to DataFrame
dfs = {}


def all_urls():
    ''' Generator yielding the dataset URLs from the main page. '''
    html = requests.get(MAIN_URL).text
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a'):
        href = link.get('href')
        if href.startswith(
                'https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/'
        ):
            yield href


def all_names():
    ''' Return a list of all the dataset names derived from the URLs on MAIN_URL. '''
    return [url_name(url) for url in all_urls()]


def url_name(url):
    ''' Infer the station key from a URL. '''
    return splitext(basename(url))[0].removesuffix('data')


def name_url(name):
    ''' The URL corresponding to a dataset name such as "ballypatrick". '''
    return f'https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/{name}data.txt'


def conv_if(s, conv):
    ''' Return `None` for "--" or "---", otherwise `conv(s)`.
      Strips any trailing asterisk.
  '''
    if s is None:
      return None
    s_ = s.rstrip('a*$').strip()
    if s_ in ('', '-', '--', '---'):
        return None
    try:
        value = conv(s_)
    except ValueError as e:
        warning("conv:%s(%r): %s, returning None", conv, s, e)
        return None
    return value


def fetch_df(url):
    ''' Fetch the data from a URL and convert into a DataFrame. '''
    print("fetch", url)
    rsp = requests.get(url)
    f = StringIO(rsp.text)
    lat = None
    long = None
    # scan the header lines for interesting info
    for line in f:
        if line.lstrip().startswith('yyyy '):
            # first column header line, exit loop
            break
        # look for the latitude/longitude
        m = re.search(r'Lat\s+(-?\d+\.\d+).*Lon\s+(-?\d+\.\d+)', line)
        if m:
            lat = float(m[1])
            long = float(m[2])
    # read the second column header line, check it is what we expect
    hdr2 = f.readline()
    assert 'degC' in hdr2
    # parse the reset of the file with read_fwf
    df = pd.read_fwf(
        f,
        infer_nrows=200,
        names=[
            'yyyy', 'mm', 'tmax_C', 'tmin_C', 'af_days', 'rain_mm', 'sun_hours'
        ],
        ##dtype={
        ##    'yyyy': int,
        ##    'mm': int,
        ##    'tmax_C': float,
        ##    'tmin_C': float,
        ##    'af_days': int,
        ##    'rain_mm': float,
        ##    'sun_hours': float,
        ##},
        converters={
            'yyyy': lambda s: conv_if(s, int),
            'mm': lambda s: conv_if(s, int),
            'tmax_C': lambda s: conv_if(s, float),
            'tmin_C': lambda s: conv_if(s, float),
            'af_days': lambda s: conv_if(s, int),
            'rain_mm': lambda s: conv_if(s, float),
            'sun_hours': lambda s: conv_if(s, float),
        },
    )
    # annotate the DataFrame with the interesting info
    df.lat = lat
    df.long = long
    ##df = pd.read_csv(url, sep=',', header=5)
    return df


def get_df(name):
    ''' Return the DataFrame for a dataset name or URL.
        Keeps a cache of previously fetched DataFrames.
    '''
    if '/' in name:
        # we were given a URL, get its name
        url = name
        name = url_name(url)
    else:
        # given a name, get the corresponding URL
        url = name_url(name)
    try:
        return dfs[name]
    except KeyError:
        df = dfs[name] = fetch_df(url)
        df['station'] = [name] * len(df)
        return df


def load_all():
    ''' Load all the datasets in the dfs dict. '''
    for name in all_names():
        print("load", name)
        get_df(name)


if __name__ == '__main__':
    df1 = get_df('ballypatrick')
    print("lat =", df1.lat, "long =", df1.long)
    print(df1)
    print(df1.keys())
    load_all()