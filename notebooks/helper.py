from typing import List
import re
import os
from itertools import chain
import datetime

import pandas as pd
import requests
from edtf import parse_edtf
import numpy as np

import headers


# Mapping of incorrect voyage numbers to correct voyage numbers
voyage_num_map = {
    4572.1: 4572.3,
    4633.3: 4633.4,
    1316.4: 1315.4,
    1662: 1662.1,
    496.2: 496.1,
    2925.5: 2925.1,
    4540.1: 4540.2,
    4671.1: 4671.2
}


voyage_num_map_str = {
    '4572.1': '4572.3',
    '4633.3': '4633.4',
    # '1316.4': '1315.4',
    # '1662': '1662.1',
    '0496.2': '0496.1',
    '2925.5': '2925.1',
    '4540.1': '4540.2',
    '4671.1': '4671.2'
}


datasets = [
    {
        'file': '../original/NT00444_SOLDIJBOEKEN.csv',
        'dir': '../original',
        'label': 'voyages'
    },
    {
        'file': '../original/NT00444_BEGUNSTIGDEN.csv',
        'dir': '../original',
        'label': 'beneficiaries'
    },
    {
        'file': '../original/NT00444_OPVARENDEN.csv',
        'dir': '../original',
        'label': 'contracts'
    },
    {
        'file': '../external/das.xlsx',
        'dir': '../external',
        'label': 'das'
    },
    {
        'file': '../external/voc_cluster_sub_file.csv.gz',
        'dir': '../external',
        'label': 'clusters'
    },
    {
        'file': '../external/rank_categories_updated.csv',
        'dir': '../external',
        'label': 'ranks'
    },
    {
        'file': '../external/reasonsEndService.xlsx',
        'dir': '../external',
        'label': 'reasons'
    },
]


# Melvin's date functions
def make_datetime(x):
    date_format = "%Y-%m-%d"
    try:
        return datetime.datetime.strptime(x, date_format).date()
    except (TypeError, ValueError):
        return np.nan


def calculate_delta(begin, end):
    if begin is not np.nan and end is not np.nan:
        delta = end - begin
        return delta.days
    else:
        return np.nan



def get_dataset_filepath(label: str):
    for dataset in datasets:
        if dataset['label'] == label:
            return dataset['file']
    raise KeyError(f'Unknown dataset label, options are {[ds["label"] for ds in datasets]}')


def fix_edtf(x):
    """Turn EDTF into Pandas parseable date."""
    if x.startswith('['):
        return str(parse_edtf(x).objects[0])
    elif '/' in x:
        return x.split('/')[0]
    else:
        return x


def get_source_type(chamber):
    """Return the type of source document on which a record is based."""
    if 'Regiment' in chamber:
        return 'regiment_book'
    elif 'Verzoekboeken' in chamber:
        return 'request_book'
    else:
        return 'pay_ledger'


def add_voyage_number_leading_zeros(number):
    if not isinstance(number, str):
        return number
    match = re.match(r'(\d{,4})\.', number)
    return (4 - len(match.group(1))) * '0' + number


def fill_missing_voyage_ids(voyage_row):
    num_id_map = {
        '0000.0': 0,
        '4800.1': 100001,
        '4801.1': 100002,
    }
    if voyage_row['das_voyage_num'] in num_id_map:
        return num_id_map[voyage_row['das_voyage_num']]
    elif isinstance(voyage_row['das_voyage_num'], str):
        return voyage_row['das_voyage_id']
    else:
        return 100003



def read_voyages_df():
    dataset_file = get_dataset_filepath('voyages')
    return pd.read_csv(dataset_file, index_col=None, header=None,
                       names=headers.voyages_headers_en, dtype={'das_voyage_num': 'string'})


def read_beneficiaries_df():
    dataset_file = get_dataset_filepath('beneficiaries')
    return pd.read_csv(dataset_file, index_col=None, header=None, names=headers.beneficiaries_headers)


#def read_das_df():
    das_file = '../external/das.xlsx'
    das_df = pd.read_excel(das_file, engine='openpyxl', dtype={'voyId': int, 'voyNumberDAS': 'string'})
    das_df.rename(columns={'voyId': 'das_voyage_id', 'voyNumberDAS': 'das_voyage_num'}, inplace=True)
    return das_df

