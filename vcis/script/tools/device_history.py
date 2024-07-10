from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools


utils = CDR_Utils()
propeties = CDR_Properties()
cassandra_tools = CassandraTools()

geo_id_nathalie = '4e79560f-e59a-4d7b-8b91-6dddbd571c57'
geo_id_pedro = '26e170d1-9bbc-4b1f-b3e9-4a7fd265a033'
geo_id_zaher = '24ff6be5-6e16-473f-93ae-1062bf8e2180'
geo_id_from_fadi_device1 = '810fc600-f3ed-6407-1a89-edb010470d9d'
geo_id_from_fadi_device2 = 'f812f4e7-83fc-499b-9aab-04b3ac93307f'

start_date = '2022-12-01'
end_date = '2024-02-29'

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

# df_nathalie = cassandra_tools.get_device_history_geo(device_id = geo_id_nathalie ,
#                                   start_date= start_date ,
#                                   end_date= end_date ,
#                                   server="10.1.10.110",
#                                   return_all_att=True)

# df_nathalie.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_nathalie1.csv", index=False)

# df_pedro = cassandra_tools.get_device_history_geo(device_id = geo_id_pedro ,
#                                   start_date= start_date ,
#                                   end_date= end_date ,
#                                   server="10.1.10.110",
#                                   return_all_att=True)

# df_pedro.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_pedro1.csv", index=False)

# df_zaher = cassandra_tools.get_device_history_geo(device_id = geo_id_zaher ,
#                                   start_date= start_date ,
#                                   end_date= end_date ,
#                                   server="10.1.10.110",
#                                   return_all_att=True)

# df_zaher.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_zaher.csv", index=False)

geo_id_from_fadi_device1 = cassandra_tools.get_device_history_geo(device_id = geo_id_from_fadi_device1 ,
                                  start_date= start_date ,
                                  end_date= end_date ,
                                  server="10.1.2.205",
                                  return_all_att=True)

geo_id_from_fadi_device1.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_from_fadi_device1.csv", index=False)

geo_id_from_fadi_device2 = cassandra_tools.get_device_history_geo(device_id = geo_id_from_fadi_device2 ,
                                  start_date= start_date ,
                                  end_date= end_date ,
                                  server="10.1.2.205",
                                  return_all_att=True)

geo_id_from_fadi_device2.to_csv("C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history_from_fadi_device2.csv", index=False)

# utils.get_device_history_geo_plot(df_geo=df,start_date=start_date,end_date=end_date)
