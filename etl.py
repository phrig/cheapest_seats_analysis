from functools import wraps
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


def local_loader(path):
    def wrapper(loader):
        @wraps(loader)
        def f():
            if os.path.exists(path):
                df = load_from_pickle(path)
            else:
                df = loader()
                print('Writing dataframe to pickle at {}'.format(path))
                df.to_pickle(path)
            return df
        return f
    return wrapper


@local_loader('data/debt.pkl')
def load_debt():
    fname = 'Campaign_Finance_Disclosure_Debt_Data_Current_State.csv'
    df = load_from_url(fname, urls['debt'])

    print('Processing data frame...')
    df['Debt Accrual Date'] = format_date(df['Debt Accrual Date'])
    df['amount'] = df['Debt Amount'].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Debt Reporting')
    df['address'] = format_address(
            df.loc[:, 'Debt Reporting Address 1': 'Debt Reporting Zip Code'])

    print('Data frame loaded.')
    return df


@local_loader('data/receipt.pkl')
def load_receipt():
    fname = 'Campaign_Finance_Disclosure_Receipt_Data_Current_State.csv'
    df = load_from_url(fname, urls['receipt'])

    print('Processing data frame...')
    df['Receipt Date'] = format_date(df['Receipt Date'])
    df['amount'] = df['Receipt Amount'].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Receipt')
    df['address'] = format_address(
        df.loc[:, 'Receipt Address 1': 'Receipt Zip Code'])

    print('Data frame loaded.')
    return df


@local_loader('data/filer.pkl')
def load_filer():
    fname = 'Campaign_Finance_Disclosure_Filer_Data_Current_State.csv'
    df = load_from_url(fname, urls['filer'], dtype={'Phone Number': str})

    print('Processing data frame...')
    df['type'] = df['Filer Type'].apply(get_filer_type).astype('category')
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Filer')
    df['address'] = format_address(
            df.loc[:, 'Filer Address 1': 'Filer Zip Code'])

    print('Data frame loaded.')
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


@local_loader('data/expense.pkl')
def load_expense():
    fname = 'Campaign_Finance_Disclosure_Expense_Data_Current_State.csv'

    df = load_from_url(fname, urls['expense'])

    print('Processing data frame...')
    df['Expense Date'] = format_date(df['Expense Date'])
    df['amount'] = df['Expense Amount'].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Expense')
    df['address'] = format_address(
            df.loc[:, 'Expense Address 1': 'Expense Zip Code'])

    print('Data frame loaded.')
    return df


@local_loader('data/contrib.pkl')
def load_contributions(save=True):
    fname = 'Campaign_Finance_Disclosure_Contributions_Data_2017_State.csv'

    df = load_from_url(fname,
                       urls['contributions'],
                       dtype={'Employer Zip Code': str})

    print('Processing data frame...')
    df['Contribution Date'] = format_date(df['Contribution Date'])
    df['amount'] = df['Contribution Amount'].apply(amount_to_float)
    df['filer_id'] = df['Filer Identification Number']
    df = get_df_lat_long(df, 'Contributor')
    df['address'] = format_address(
            df.loc[:, 'Contributor Address 1': 'Contributor Zip Code'])

    print('Data frame loaded.')
    return df


def load_from_pickle(path):
    print('Loading data frame from {}.'.format(path))
    df = pd.read_pickle(path)
    print('Data frame loaded.')
    return df


def load_from_url(fname, url, **kwargs):
    print('Downloading {} from {}.\n'.format(fname, url))
    df = pd.read_csv(url, **kwargs)
    df.fillna('', inplace=True)
    return df


def format_date(series, format='%Y%m%d.0'):
    return pd.to_datetime(series.astype(str),
                          format=format,
                          errors='coerce')


def format_address(df):
    return df.apply(' '.join, axis=1)


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
