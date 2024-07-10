import pandas as pd 
from vcis.databases.cassandra.cassandra_tools import CassandraTools
cassandra_tools = CassandraTools()
df_grouped_csv = pd.read_csv('/u01/jupyter-scripts/Joseph/CDR_git/Trace/src/cdr_trace/script/generation/stat_data.csv')
df_grouped_csv.drop(columns=['Unnamed: 0'], inplace=True)

print(df_grouped_csv)

result = df_grouped_csv.groupby(['SERVICE_PROVIDER_ID', 'COUNTRY_CODE', 'YEAR_NO', 'MONTH_NO', 'DAY_NO', 'HOUR_NO'], as_index=False)['NumberOfHits'].sum()
print(result)
result['DATA_TYPE'] ='GEO DATA'
result.to_csv('/u01/jupyter-scripts/Joseph/CDR_git/Trace/src/cdr_trace/script/generation/stat_data_2.csv')

result2 = result.groupby([ 'COUNTRY_CODE'], as_index=False)['NumberOfHits'].sum()
print(result2)

df_countries = cassandra_tools.get_table(table_name='countries_iso_codes',server='10.1.2.205')
print(df_countries.columns)
df_countries.rename(columns={'country_code': 'COUNTRY_CODE'},inplace=True)
df_countries['COUNTRY_CODE'] = df_countries['COUNTRY_CODE'].astype(int)
merged_df = pd.merge(result2, df_countries, on='COUNTRY_CODE', how='inner')

merged_df = merged_df[['COUNTRY_CODE','country_name','NumberOfHits']]
print(merged_df)