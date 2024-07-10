import pandas as pd
import subprocess
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
import os
import logging
import reverse_geocoder as rg
import subprocess

from vcis.utils.utils import CDR_Properties, CDR_Utils

geo = False
properties = CDR_Properties()
utils = CDR_Utils() 

table_name_non_geo = 'datacrowd.loc_location_cdr_main_new'

################################################################################################
#DSBULK INSERTION AREA
file_name = '890.csv'
file_path = properties.passed_filepath_excel + file_name
country_path = properties.passed_filepath_excel + 'countries_iso_codes'


df = pd.read_csv(file_path)
if geo==True:
    country_codes = utils.read_csv_files(country_path)


    df = utils.add_reverseGeocode_columns(df)

    df.rename(columns={'country': 'country_alpha2'},inplace=True)

    df = pd.merge(df, country_codes, on='country_alpha2', how='inner')
    df.rename(columns={'sub_region_code': 'subregion_code'},inplace=True)
    df['subregion_code'] = df['subregion_code'].astype(int)

    df['service_provider_id'] = '9'

    df = df[['device_id',
        'location_name',
        'usage_timeframe',
        'location_latitude',
        'location_longitude',
        'country_code' ,
        'country_alpha2' ,
        'region_code' ,
        'subregion_code'
        ]]

    sub = df['region_code'].iloc[0]
    region = df['subregion_code'].iloc[0]


    columns = df.columns

    df.to_csv(file_path)


    df_device_combo_list = []
    df['date'] = pd.to_datetime(df['usage_timeframe'], unit='ms').dt.date
    print(df['date'].min(),df['date'].max())
    dates = utils.get_month_year_combinations_u(df)
    for date in dates:
        month, year = date 
        table_name = f"datacrowd.geo_data_{year}_{month}_{sub}_{region}"

        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])

        # Filter DataFrame based on month
        month_df = df[df['date'].dt.month == int(month)]

        month_df.drop(columns=['date'], inplace=True)

        temp_path = properties.passed_filepath_excel + 'temp.csv'
        month_df.to_csv(temp_path, index=False)

        mapping1 = ", ".join([f"{i}={col}" for i, col in enumerate(columns)])
        mapping2 = ", ".join([f"{col}" for col in columns])
        mapping3 = ", ".join([f":{col}" for col in columns])

        query_combo = f"""dsbulk load -url {temp_path} -delim ',' -m "{mapping1}" -query "INSERT INTO {table_name} ({mapping2}) VALUES ({mapping3})";"""

        # Execute the command
        subprocess.run(query_combo, shell=True)

elif geo==False:
    columns = df.columns
    mapping1 = ", ".join([f"{i}={col}" for i, col in enumerate(columns)])
    mapping2 = ", ".join([f"{col}" for col in columns])
    mapping3 = ", ".join([f":{col}" for col in columns])

    query_combo = f"""dsbulk load -url {file_path} -delim ',' -m "{mapping1}" -query "INSERT INTO {table_name_non_geo} ({mapping2}) VALUES ({mapping3})";"""

    # Execute the command
    subprocess.run(query_combo, shell=True)