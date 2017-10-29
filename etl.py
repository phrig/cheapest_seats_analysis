import geopandas as gpd
import numpy as np
import os
import pandas as pd
import re

base = 'https://data.pa.gov/api/views/'
urls = {'filer': base + '53wp-ib3s/rows.csv?accessType=DOWNLOAD',
        'expense': base + 'btk5-mx6k/rows.csv?accessType=DOWNLOAD',
        'debt': base + '8bef-t5zg/rows.csv?accessType=DOWNLOAD',
        'receipt': base + 'w5um-i7zu/rows.csv?accessType=DOWNLOAD',
        'contributions':  base + 'wb79-wsa4/rows.csv?accessType=DOWNLOAD'}


def load_contributions(save=True):
    fname = 'Campaign_Finance_Disclosure_Contributions_Data_2017_State.csv'
    path = 'data/contrib.pkl'

    if os.path.exists(path):
        print('Loading data frame from {}.'.format(path))
        df = pd.read_pickle(path)
        print('Data frame loaded.')
        return df

    print('Downloading {} from {}.\n'.format(fname, urls['contributions']))
    df = pd.read_csv(urls['contributions'], dtype={'Employer Zip Code': str})

    print('Processing data frame...')
    df.fillna('', inplace=True)
    dates = pd.to_datetime(df['Contribution Date'].astype(str),
                           format='%Y%m%d.0', errors='coerce')
    df['Contribution Date'] = dates

    df['amount'] = df['Contribution Amount'].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Contributor')
    df['address'] = df.loc[:,
                           'Contributor Address 1':
                           'Contributor Zip Code'].apply(' '.join, axis=1)
    if save:
        df.to_pickle(path)
        print('Data frame loaded and saved to {}.'.format(path))
    else:
        print('Data frame loaded.')

    return df


def amount_to_float(amount):
    """Converts a dollar amount to a float."""
    if type(amount) == float and np.isnan(amount):
        return np.nan
    pattern = re.compile(r'\$(.*)')
    g = re.match(pattern, amount)
    if g:
        return float(g.groups()[0])
    else:
        return np.nan


def extract_lat_long(x):
    """Extract lat and long from an address."""
    pattern = re.compile(r'\((.*), (.*)\)')
    lat, long = np.nan, np.nan
    if type(x) == float and np.isnan(x):
        return lat, long
    for y in x.split('\n'):
        g = re.match(pattern, y)
        if g:
            lat, long = g.groups()
    return float(lat), float(long)


def get_lat_long(x, string=None):
    lat, long = extract_lat_long(x[string + ' Location 1'])
    if np.isnan(lat):
        lat, long = extract_lat_long(x[string + ' Location 2'])
    return '{} {}'.format(lat, long)


def get_df_lat_long(df, string):
    new_pd = df.apply(get_lat_long, axis=1, string=string)
    new_pd = new_pd.str.split(expand=True).astype(float)
    df[['lat', 'long']] = new_pd.copy()
    return df
