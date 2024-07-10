from vcis.utils.utils import CDR_Utils
from vcis.utils.utils import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools

import pandas as pd

cotraveler_functions = CDR_Utils()
properties = CDR_Properties()
cassandra_tools = CassandraTools()

df = pd.read_csv(properties.passed_filepath_excel + 'data.csv',index_col=False)
df = cotraveler_functions.add_reverseGeocode_columns(df)
print(df)
print(df.columns)
df.rename(columns={'country': 'country_alpha2'},inplace=True)
df_countries = cassandra_tools.get_table(table_name='countries_iso_codes',server='10.1.2.205')
merged_df = pd.merge(df, df_countries, on='country_alpha2', how='inner')

print(merged_df.columns)

merged_df = merged_df[['location_latitude', 'location_longitude','device_id', 'usage_timeframe', 'device_name','country_code','country_name']]

print(merged_df)
merged_df.to_csv('result.csv')