from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.script.nodes.azimuth_fix import ProcessBTSData

import time
import warnings
warnings.filterwarnings('ignore')

#########################################################################################################

utils = CDR_Utils()
properties = CDR_Properties()

# Load Data
#########################################################################################################

server = '10.1.10.66'
bts_data = utils.get_spark_data(passed_table_name = properties.cdr_bts_test_table_name, 
                                passed_connection_host = server)
bts_data = bts_data.toPandas()
print('INFO:   Data Loaded.')

# Process Data
#########################################################################################################

bts_data[properties.location_latitude] = bts_data[properties.location_latitude].astype(float)
bts_data[properties.location_longitude] = bts_data[properties.location_longitude].astype(float)

# Get polygon vertices for each BTS Sector
#########################################################################################################

start = time.time()

new_bts_data = utils.get_polygon_vertices(data = bts_data, distance_km = 0.4)
new_bts_data.drop(columns=['index'], inplace = True)
print(new_bts_data)
print(new_bts_data.columns)

end = time.time()

time_passed = round(end - start, 2)
print(f'INFO:  Time passed --> {time_passed} seconds.')
print('INFO:   Polygon Vertices Computed.')

# Save results in cassandra
#########################################################################################################

bts_data_processor = ProcessBTSData()
bts_data_processor.data = new_bts_data

final_bts_data = bts_data_processor.save_results_in_cassandra(data = bts_data_processor.data,
                                                              passed_table_name = properties.cdr_bts_polygon_vertices_table_name)

print('INFO:   Results saved in cassandra.')

# Read saved results from cassandra
#########################################################################################################

stored_results = bts_data_processor.read_results_from_cassandra(properties.cdr_bts_polygon_vertices_table_name)
print(stored_results)

print('INFO:   Results read from cassandra.')
print('INFO:   Process Complete.')
