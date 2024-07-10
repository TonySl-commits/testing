from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.utils.utils import CDR_Utils
import pandas as pd 
import glob
import os

cassandra_tools = CassandraTools()
cassandra_spark_tools = CassandraSparkTools()
utils = CDR_Utils()
server = '10.1.10.110'


folder = '/u01/csvfiles/GEO_DATA/'

subfolders = [f.path for f in os.scandir(folder) if f.is_dir()]
# print(subfolders)
# # List all CSV files in the directory
for directoryPath in subfolders :
    print(directoryPath)
    try:
        df_grouped_csv = pd.read_csv('stat_data.csv')
        df_grouped_csv.drop(columns=['Unnamed: 0'], inplace=True)
    except Exception as e :
        print(e)
        print('first run')
    csv_files = glob.glob(directoryPath + '/*.csv')

    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()

    # Loop through the list of CSV files
    for file_name in csv_files:
        # Read each CSV file into a DataFrame
        df = pd.read_csv(file_name)
        # Concatenate the DataFrame to the all_data DataFrame
        all_data = pd.concat([all_data, df], ignore_index=True)
        print("#########",'concat data',len(all_data))
    # print(all_data.columns)
    # Now, all_data contains data from all CSV files in the directory
    # print(all_data.columns)
    # all_data.head(10).to_csv('sample_all.csv')
    if len(all_data)>0:
        all_data = utils.convert_ms_to_datetime(all_data)
        # print(all_data['usage_timeframe'])


        df_grouped = all_data.groupby(['service_provider_id','country_code','year_no','month_no','day_no','hour_no']).size().reset_index(name='NumberOfHits')
        df_grouped['DATA_TYPE'] ='GEO DATA'
        df_grouped.rename(columns={'service_provider_id': 'SERVICE_PROVIDER_ID','country_code':'COUNTRY_CODE','YEAR_NO':'year_no','month_no':'MONTH_NO','day_no':'DAY_NO','hour_no':'HOUR_NO'}, inplace=True)
        print("#########",'grouped data',len(df_grouped))
    try:
        df_grouped_csv = pd.concat([df_grouped, df_grouped_csv], ignore_index=True)
        print("#########",'grouped data total',len(df_grouped_csv))
        df_grouped_csv.to_csv('stat_data.csv')
    except Exception as e:
        print(e)
        df_grouped.to_csv('stat_data.csv')
        print('first run save')