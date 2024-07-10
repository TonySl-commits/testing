import json
import uvicorn

import warnings
warnings.filterwarnings('ignore')

from fastapi import FastAPI, Body
from IPython.display import display

from vcis.utils.utils import CDR_Utils
from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis

##########################################################################################################




device_ids = api_data['device_id']
table_id = api_data['table_id']
server = api_data['server']
start_date = api_data['start_date']
end_date = api_data['end_date']

##########################################################################################################

print('INFO:     API Request Approved!!!')

##########################################################################################################

start_date = str(start_date)
end_date = str(end_date)

##########################################################################################################

cotraveler_functions = CotravelerFunctions()
utils = CDR_Utils()
cassandra_tools = CassandraTools()

##########################################################################################################

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(start_date)

##########################################################################################################

# Perform analysis on devices
analyzer = GeoLocationAnalyzer(table_id = table_id)

# device_list = cotraveler_functions.separate_list(device_ids, 1)
df_history = cassandra_tools.get_device_history_geo_chunks(device_ids, start_date, end_date, server)

if df_history.empty:
    print('INFO:    No data fetched from cassandra. Process Halted!')
else:
    analyzer.separate_devices_into_objects(df_history)
    
    analyzer.perform_analysis_on_all_devices()

    print('INFO:    END POINT REACHED!')
    ##########################################################################################################

#     # Perform Confidence Analysis
#     geo_confidence_analyzer = GeospatialConfidenceAnalysis()
#     print('INFO:    GEOSPATIAL CONFIDENCE ANALYSIS STARTED!')

#     confidence_analysis = geo_confidence_analyzer.evaluate_confidence_in_analysis_for_all_devices(df_history, table_id)
    
#     ##########################################################################################################

#     reporting = ReportGenerator(confidence_analysis)
#     reporting.get_aoi_report(analyzer, table_id)

#     ##########################################################################################################

#     print('INFO:  AOI Reports Successfully Created!!!')

# ##########################################################################################################
    
# if __name__ == "__main__":
#     uvicorn.run(app, host = "10.1.10.110", port = 4096, reload=False)
