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
x = 0
for directoryPath in subfolders :
    print('folders remaining:',len(subfolders)-x)
    x +=1
    print(directoryPath)
    try:
        df_grouped_csv = pd.read_csv('stat_data.csv')
    except Exception as e :
        print(e)
        print('first run')
    try:
        df_grouped_csv.drop(columns=['Unnamed: 0'], inplace=True)
    except:
        pass

    csv_files = glob.glob(directoryPath + '/*.csv')
    all_data = pd.DataFrame()
    i=0
    for file_name in csv_files:
        df = pd.read_csv(file_name, dtype={13: str})
        if len(df)>0:
            # df = utils.convert_ms_to_datetime(df)
            df_grouped = df.groupby(['service_provider_id','country_code','year_no','month_no','day_no','hour_no']).size().reset_index(name='NumberOfHits')
            df_grouped['DATA_TYPE'] ='GEO DATA'
            df_grouped.rename(columns={'service_provider_id': 'SERVICE_PROVIDER_ID','country_code':'COUNTRY_CODE','year_no':'YEAR_NO','month_no':'MONTH_NO','day_no':'DAY_NO','hour_no':'HOUR_NO'}, inplace=True)
            # print(df_grouped.columns)
            print("#########",'grouped data',len(df_grouped))

            all_data = pd.concat([all_data, df_grouped], ignore_index=True)

    if len(all_data)>0:
        # df_grouped = all_data.groupby(['SERVICE_PROVIDER_ID', 'COUNTRY_CODE', 'YEAR_NO', 'MONTH_NO', 'DAY_NO', 'HOUR_NO']).agg({'NumberOfHits': 'sum'})
        # all_data.to_csv(f'df_grouped_{i}.csv')
        # print(i)
        # i+=1
        # print(df_grouped)
        try:
            df_grouped_csv = pd.concat([all_data, df_grouped_csv], ignore_index=True)
            print("#########",'grouped data total',len(df_grouped_csv))
            df_grouped_csv.to_csv('stat_data.csv')
        except Exception as e:
            print(e)
            df_grouped.to_csv('stat_data.csv')
            print('first run save')

