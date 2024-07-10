from vcis.script.nodes.azimuth_fix import ProcessBTSData
from vcis.utils.properties import CDR_Properties

import time
import warnings
warnings.filterwarnings('ignore')

##########################################################################################################

start = time.time()

# Import BTS Data from cassandra
##########################################################################################################

bts_data_processor = ProcessBTSData()
properties = CDR_Properties()

data = bts_data_processor.read_and_process_data_from_cassandra()

# Compute BTS Sector Angle 
##########################################################################################################

result = bts_data_processor.compute_left_and_right_sector_angles()

bts_data = bts_data_processor.merge_results()

# Save results in cassandra
##########################################################################################################

final_bts_data = bts_data_processor.save_results_in_cassandra(data = bts_data_processor.data, 
                                                              passed_table_name = properties.cdr_bts_test_table_name)

# Read saved results from cassandra
##########################################################################################################

stored_results = bts_data_processor.read_results_from_cassandra(properties.cdr_bts_test_table_name)
print(stored_results)

# Check time spent
##########################################################################################################

end = time.time()
time_passed = round(end - start, 2)
print(f'INFO:  Time passed --> {time_passed} seconds.')
