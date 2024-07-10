from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.utils.utils import CDR_Utils
import pandas as pd 
import glob

cassandra_tools = CassandraTools()
cassandra_spark_tools = CassandraSparkTools()
utils = CDR_Utils()
server = '10.1.10.110'
try:
    df_grouped_csv = pd.read_csv('stat_data.csv')
    df_grouped_csv.drop(columns=['Unnamed: 0'], inplace=True)
except:
    print('first run')

df_sql = pd.read_csv('/u01/csvfiles/Oracle_table.csv')
print(df_sql.columns)
# df_sql.head(10).to_csv('sample.csv')
# Specify the directory path
directoryPath = '/u01/csvfiles/GEO_DATA/geo_data_2024_1_142_145/'

# List all CSV files in the directory
csv_files = glob.glob(directoryPath + '/*.csv')

# Initialize an empty DataFrame to store all data
all_data = pd.DataFrame()

# Loop through the list of CSV files
for file_name in csv_files:
    # Read each CSV file into a DataFrame
    df = pd.read_csv(file_name)
    # Concatenate the DataFrame to the all_data DataFrame
    all_data = pd.concat([all_data, df], ignore_index=True)
print(all_data.columns)
# Now, all_data contains data from all CSV files in the directory
# print(all_data.columns)
# all_data.head(10).to_csv('sample_all.csv')

all_data = utils.convert_ms_to_datetime(all_data)
print(all_data['usage_timeframe'])


df_grouped = all_data.groupby(['service_provider_id','country_code','year_no','month_no','day_no','hour_no']).size().reset_index(name='NumberOfHits')
df_grouped['DATA_TYPE'] ='GEO DATA'
df_grouped.rename(columns={'service_provider_id': 'SERVICE_PROVIDER_ID','country_code':'COUNTRY_CODE','YEAR_NO':'year_no','month_no':'MONTH_NO','day_no':'DAY_NO','hour_no':'HOUR_NO'}, inplace=True)
print(df_grouped)
try:
    df_grouped_csv = pd.concat([df_grouped, df_grouped_csv], ignore_index=True)
    df_grouped_csv.to_csv('stat_data.csv')
except:
    df_grouped.to_csv('stat_data.csv')
    print('first run save')