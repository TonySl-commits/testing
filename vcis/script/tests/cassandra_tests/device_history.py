from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools


utils = CDR_Utils()
propeties = CDR_Properties()
cassandra_tools = CassandraTools()

geo_id = 'b95d5d9c-444e-4b4d-ad8d-0b61ac1ec4e6'
start_date = '2023-01-01'
end_date = '2024-05-29'
# geo_id = ['8fa120eb-8f89-6e1e-24ca-4d8fb200e988','1351ad1f-97da-2eaa-58c5-a50944cf7ea7']
geo_id = '8e09f8c4-7158-4e15-accf-14df14740190'
start_date = '2023-01-18'
end_date = '2024-01-30'

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
print(start_date)

df = cassandra_tools.get_device_history_geo(device_id = geo_id ,
                                  start_date= start_date ,
                                  end_date= end_date ,
                                  server="10.1.10.110",
                                  return_all_att=True)
df['location_name'] = '8e09f8c4-7158-4e15-accf-14df14740190'
print(df.columns)
print(df.head())
df.to_csv(propeties.passed_filepath_cdr + 'device_history.csv', index=False)

# utils.get_device_history_geo_plot(df_geo=df,start_date=start_date,end_date=end_date)
