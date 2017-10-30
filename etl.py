import functools
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import re
import shapely

base = 'https://data.pa.gov/api/views/'
urls = {'filer': base + '53wp-ib3s/rows.csv?accessType=DOWNLOAD',
        'contributions':  base + 'wb79-wsa4/rows.csv?accessType=DOWNLOAD',
        'debt': base + '8bef-t5zg/rows.csv?accessType=DOWNLOAD',
        'expense': base + 'btk5-mx6k/rows.csv?accessType=DOWNLOAD',
        'receipt': base + 'w5um-i7zu/rows.csv?accessType=DOWNLOAD'}


def local_loader(path):
    def wrapper(loader):
        @functools.wraps(loader)
        def f():
            if os.path.exists(path):
                print('Loading data frame from {}.'.format(path))
                df = pd.read_pickle(path)
                print('Data frame loaded.')
            else:
                df = gpd.GeoDataFrame(loader())
                print('Data frame loaded.')
                print('Writing dataframe to pickle at {}'.format(path))
                df.to_pickle(path)
                print('Done.\n')
            return df
        return f
    return wrapper


def process_df(df, date=None, amount=None, suffix=None):
    print('Processing data frame...')
    df[date] = format_date(df[date])
    df['amount'] = df[amount].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df['address'] = format_address(df, suffix)
    df['geometry'] = format_geometry(df, suffix)
    return df


@local_loader('data/debt.pkl')
def load_debt():
    fname = 'Campaign_Finance_Disclosure_Debt_Data_Current_State.csv'
    df = load_from_url(fname, urls['debt'])
    return process_df(df,
                      date='Debt Accrual Date',
                      amount='Debt Amount',
                      suffix='Debt Reporting')


@local_loader('data/receipt.pkl')
def load_receipt():
    fname = 'Campaign_Finance_Disclosure_Receipt_Data_Current_State.csv'
    df = load_from_url(fname, urls['receipt'])
    return process_df(df,
                      date='Receipt Date',
                      amount='Receipt Amount',
                      suffix='Receipt')


@local_loader('data/expense.pkl')
def load_expense():
    fname = 'Campaign_Finance_Disclosure_Expense_Data_Current_State.csv'
    df = load_from_url(fname, urls['expense'])
    return process_df(df,
                      date='Expense Date',
                      amount='Expense Amount',
                      suffix='Expense')


@local_loader('data/contrib.pkl')
def load_contributions(save=True):
    fname = 'Campaign_Finance_Disclosure_Contributions_Data_2017_State.csv'
    df = load_from_url(fname,
                       urls['contributions'],
                       dtype={'Employer Zip Code': str})
    return process_df(df,
                      date='Contribution Date',
                      amount='Contribution Amount',
                      suffix='Contributor')


@local_loader('data/filer.pkl')
def load_filer():
    fname = 'Campaign_Finance_Disclosure_Filer_Data_Current_State.csv'
    df = load_from_url(fname,
                       urls['filer'],
                       dtype={'Phone Number': str})

    print('Processing data frame...')
    df['type'] = df['Filer Type'].apply(get_filer_type).astype('category')
    df['filer_id'] = df['Filer Identification Number']
    df['address'] = format_address(df, 'Filer')
    df['geometry'] = format_geometry(df, 'Filer')
    return df


def get_filer_type(x):
    if x == 1.0:
        return 'candidate'
    elif x == 2.0:
        return 'committee'
    elif x == 3.0:
        return 'lobbyist'
    else:
        return 'unknown'


def load_from_url(fname, url, **kwargs):
    """Load remote csv file."""
    print('Downloading {} from {}.\n'.format(fname, url))
    df = pd.read_csv(url, **kwargs)
    df.fillna('', inplace=True)
    return df


def format_date(series, format='%Y%m%d.0'):
    """Convert date string as float to datetime object."""
    return pd.to_datetime(series.astype(str),
                          format=format,
                          errors='coerce')


def format_address(df, prefix):
    """Join separate address fields as a single field."""
    start = '{} Address 1'.format(prefix)
    end = '{} Zip Code'.format(prefix)
    return df.loc[:, start: end].apply(' '.join, axis=1)


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


def format_geometry(df, prefix):
    """Format a GeoSeries of coordinates."""
    df = df.apply(get_coord, axis=1, prefix=prefix)
    return gpd.GeoSeries(df, name='geometry')


def get_coord(x, prefix=None):
    """Get coordinates from a location."""
    loc = x['{} Location 1'.format(prefix)]
    lat, long = get_lat_long(loc)
    if np.isnan(lat):
        loc = x['{} Location 2'.format(prefix)]
        lat, long = get_lat_long(loc)
    if np.isnan(lat):
        return None
    else:
        return shapely.geometry.Point([long, lat])


def get_lat_long(x):
    """Get latitude and longitude from a string."""
    pattern = re.compile(r'\((.*), (.*)\)')
    lat, long = np.nan, np.nan
    if type(x) == float and np.isnan(x):
        return lat, long
    for y in x.split('\n'):
        g = re.match(pattern, y)
        if g:
            lat, long = g.groups()
    return float(lat), float(long)
