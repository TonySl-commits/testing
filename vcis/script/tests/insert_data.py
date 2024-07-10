#Change the file path in the commented locations.

import pandas as pd
import subprocess
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
import os
import logging
import reverse_geocoder as rg
import subprocess
from cassandra.cluster import Cluster
from datetime import datetime
from cassandra.concurrent import execute_concurrent
from pyspark.sql import SparkSession


def convert_timestamp_to_date_value(variable:int):
    variable = pd.to_datetime(pd.to_numeric(variable), unit='ms')
    return variable

def add_reverseGeocode_columns(df):
    temp_filepath = "temp_geocode.csv"
    df.to_csv(temp_filepath, index=False)
    subprocess.run(["python", '/u01/jupyter-scripts/Riad/CDR_Trace/src/cdr_trace/script/tests/execution_code.py'])
    df = pd.read_csv(temp_filepath)
    os.remove(temp_filepath)
    return df

def get_month_year_combinations(df):
    df['date'] = pd.to_datetime(df['usage_timeframe'],unit = 'ms')
    df['m'] = df['date'].dt.month
    df['y'] = df['date'].dt.year
    dates = df[['m', 'y']].drop_duplicates()
    dates = dates.values.tolist()
    print(dates)
    return dates 

def get_country_alpha2(location_latitude, location_longitude):
    result = rg.search((location_latitude, location_longitude))
    print(result)
    return result[0]['cc']

def read_csv_files(folder_path):
    dfs = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            try:
                dff = pd.read_csv(file_path)
                dfs.append(dff)
            except Exception as e:
                print(f"Error reading file '{file_name}': {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        print("No CSV files found in the specified folder.")
        return None
    
def create_statistics_dataframe(df):
    statistics_df = pd.DataFrame()

    statistics_df['country_code'] = df['country_code']
    statistics_df['subregion_code'] = df['subregion_code']
    statistics_df['month_no'] = df['month_no']
    statistics_df['year_no'] = df['year_no']

    statistics_df = pd.merge(statistics_df, country_codes, on='country_code', how='inner')
    
    sdf_col = statistics_df.columns
    print("sdf_col:",sdf_col)
    
    statistics_df = statistics_df[['country_name','country_code','region_code','subregion_code','month_no', 'year_no']]

    statistics_df['hits'] = df.groupby(['country_code', 'region_code', 'subregion_code', 'month_no', 'year_no'])['device_id'].transform('count')

    statistics_df = statistics_df[['country_name','country_code','region_code','subregion_code','month_no', 'year_no' , 'hits']]
        
    statistics_df = statistics_df.drop_duplicates(subset=['country_name', 'country_code', 'region_code', 'subregion_code', 'month_no', 'year_no', 'hits'])
    
    return statistics_df

################################################################################################
#DSBULK INSERTION AREA
file_path = '/u01/jupyter-scripts/Riad/CDR_Trace/senario_dup_devices.csv' #change path of file here
country_path = '/u01/jupyter-scripts/Rick/Csv-Xlsx-files/csv_dsbulk/countries_iso_codes.csv'

df = pd.read_csv(file_path)
df.dropna(inplace=True)
columns_before = df.columns
print("columns before" ,columns_before)
df = add_reverseGeocode_columns(df)
columns_after = df.columns
print("columns after" ,columns_after)
df.rename(columns={'country': 'country_alpha2'},inplace=True)
country_codes = read_csv_files(country_path)

df = pd.merge(df, country_codes, on='country_alpha2', how='inner')
df.rename(columns={'sub_region_code': 'subregion_code'},inplace=True)
df['subregion_code'] = df['subregion_code'].astype(int)
cols = df.columns

#Columns:
#df = df[['device_id','location_name','usage_timeframe','location_latitude','location_longitude', 'country_code' , 'country_alpha2' ,'region_code' , 'subregion_code']]

df = df[['device_id','location_name','usage_timeframe','location_latitude','location_longitude', 'country_code' , 'country_alpha2' ,'region_code' , 'subregion_code']]
print(df)

#add month and year and day columns to the data
df['datee'] = pd.to_datetime(df['usage_timeframe'], unit='ms')
# Extract month, year, and day
df['month_no'] = df['datee'].dt.month
df['year_no'] = df['datee'].dt.year
df['day_no'] = df['datee'].dt.day
df['hour_no'] = df['datee'].dt.hour

df.drop('datee', axis=1, inplace=True)

geo = True

#Check for regions, subregions
distinct_combinations = df[['country_alpha2', 'region_code', 'subregion_code']].drop_duplicates()
print("These are the Distinct region, subregion, and country_alpha2 combinations:")
print(distinct_combinations)

#Split dataframes operations
# Filter the DataFrame to get rows with region_code = 150 and subregion_code = 155
# European = df[(df['region_code'] == 150) & (df['subregion_code'] == 155)]

# # Remove the filtered rows from the original DataFrame
# new_df = df.drop(European.index)

# # Get distinct region, subregion, and country_alpha2 combinations
# European_distinct_combinations = European[['country_alpha2', 'region_code', 'subregion_code']].drop_duplicates()

# # Print the distinct combinations
# print("Distinct region, subregion, and country_alpha2 combinations:")
# print(European_distinct_combinations)

# # Get distinct region, subregion, and country_alpha2 combinations
# new_DF_distinct_combinations = new_df[['country_alpha2', 'region_code', 'subregion_code']].drop_duplicates()

# # Print the distinct combinations
# print("Distinct region, subregion, and country_alpha2 combinations:")
# print(new_DF_distinct_combinations)


#Automatically takes the first region,subregion of the csv
region = df['region_code'].iloc[0]
sub_region = df['subregion_code'].iloc[0]

# We can assign them manually too, if all the csv is consisted of only one region subregion.
# sub = '19'

## region = '21'

df['service_provider_id'] = '9'
columns = df.columns
df.to_csv('dsbulk_test_1.csv')
df_device_combo_list = []
df['date'] = pd.to_datetime(df['usage_timeframe'], unit='ms').dt.date
print(df['date'].min(),df['date'].max())

if geo==True:
    dates = get_month_year_combinations(df)
    for date in dates:
        month, year = date 
        table_name = f"datacrowd.geo_data_{year}_{month}_{region}_{sub_region}"  # Construct table name based on year and month
        #table_name = f"datacrowd.geo_data_{year}_{month:02d}_{int(sub)}_{int(region)}"  # Construct table name based on year and month

        print(table_name)
        # Convert 'date' column to datetime format if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])

        # Filter DataFrame based on month
        month_df = df[df['date'].dt.month == int(month)]

        month_df.drop(columns=['date'], inplace=True)

        temp_path = '/u01/jupyter-scripts/Rick/Csv-Xlsx-files/csv_dsbulk/temp.csv'
        month_df.to_csv(temp_path, index=False)

        # Define mapping1, mapping2, and mapping3
        # Define mapping1, mapping2, and mapping3, excluding the 'usage_timeframe' column
        mapping1 = ", ".join([f"{i}={col}" for i, col in enumerate(columns)])
        mapping2 = ", ".join([f"{col}" for col in columns])
        mapping3 = ", ".join([f":{col}" for col in columns])

        # Construct dsbulk load command with the target table name
        query_combo = f"""dsbulk load -url {temp_path} -delim ',' -m "{mapping1}" -query "INSERT INTO {table_name} ({mapping2}) VALUES ({mapping3})";"""

        last_columns = df.columns
        print ("check the dataframe" , df )
        print ("check the columns lastly" , last_columns )
        
        statistics_df = create_statistics_dataframe(df)
        print(statistics_df)

subprocess.run(query_combo, shell=True) 
        