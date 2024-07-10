from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
import pandas as pd
import random
import numpy as np

class CDR_data_generation():
    def __init__(self, verbose: bool = False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.cassandra_tools = CassandraTools()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.verbose = verbose
    def cdr_data_generation(self, passed_geo_id:str, passed_start_date:str, passed_end_date:str):
        device_history = self.cassandra_tools.get_device_history_geo(passed_geo_id , passed_start_date , passed_end_date)
        bts_table = self.cassandra_spark_tools.get_spark_data(passed_table_name=self.properties.bts_table_name)
        bts_table = bts_table.toPandas()
        bts_table = bts_table[['bts_cell_name','cgi_id' ,'location_latitude', 'location_longitude', 'location_azimuth']]
        bts_table = bts_table.drop_duplicates()
        bts_table['location_azimuth'] = bts_table['location_azimuth'].replace(['Indoor','indoor','INDOOR'],0).astype(int)
        bts_table['location_latitude'] = bts_table['location_latitude'].astype(float)
        bts_table['location_longitude'] = bts_table['location_longitude'].astype(float)

        device_history['location_latitude'] = device_history['location_latitude'].astype(float)
        device_history['location_longitude'] = device_history['location_longitude'].astype(float)
	
        device_history['cgi_id'] = device_history.apply(lambda row: self.utils.get_closest_bts(device_lat = row['location_latitude'], device_lon = row['location_longitude'] , bts_table=bts_table), axis=1)

        # Store the generated data in a new dataframe called device_generated
        device_generated = device_history.copy()
        device_generated['usage_timeframe_cdr'] = device_generated['usage_timeframe'] + np.random.randint(-5000, 5000, size=len(device_generated))


        device_generated = device_generated [['cgi_id', 'usage_timeframe_cdr']]

        merged_data = pd.merge(device_generated, bts_table, left_on='cgi_id', right_on='cgi_id', how='inner')
        merged_data = merged_data[['cgi_id', 'usage_timeframe_cdr', 'location_latitude', 'location_longitude', 'location_azimuth']]
        merged_data['location_azimuth']=merged_data['location_azimuth'].replace('Indoor',0).astype(int)
        # Create a new dataframe as a sample of merged_data (randomly select 70% of the rows)
        frac_value = random.uniform(0.6, 0.9)
        merged_data = merged_data.sample(frac=frac_value, random_state=42)
        merged_data.reset_index(drop=True,inplace = True)
        imsi_id = self.utils.generate_random_imsi()
        merged_data['imsi_id'] = imsi_id
        merged_data['imei_id'] = self.utils.generate_random_imei()
        merged_data['phone_number'] = self.utils.generate_random_phone_number()
        merged_data['service_provider_id'] = '2'
        merged_data['type_id'] = '1'
        merged_data['data_source_id'] = '1'
        merged_data = merged_data.rename(columns={'usage_timeframe_cdr': 'usage_timeframe'})
        merged_data.to_csv('/u01/jupyter-scripts/Joseph/CDR_trace/data/cdr_generated/device_generated.csv')
        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping_2.items():
            merged_data[column_name] = merged_data[column_name].astype(column_type)
        merged_data.dropna(inplace=True)
        merged_data= merged_data.reindex(columns=['imsi_id', 'usage_timeframe', 'cgi_id', 'data_source_id', 'imei_id', 'location_azimuth', 'location_latitude', 'location_longitude', 'phone_number', 'service_provider_id', 'type_id'])

        # Insert data into Cassandra
        self.cassandra_tools.insert_into_cassandra(passed_dataframe = merged_data , passed_table_name = 'loc_location_cdr_main_new')

        print(f'#### Inserted {len(merged_data)} rows into Cassandra for this Geo ID {passed_geo_id} under this IMSI {imsi_id}. ####')

        return passed_geo_id, imsi_id
