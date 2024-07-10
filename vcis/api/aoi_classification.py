import json
import uvicorn

import warnings
warnings.filterwarnings('ignore')

from fastapi import FastAPI, Body
from IPython.display import display

from vcis.utils.utils import CDR_Utils , CDR_Properties
from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
# from vcis.aoi_classification.aoi_correlation import AOI_Correlation
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis
from vcis.reporting.report_main.load_or_create_simulation_reprot import LoadOrCreateSimulationReport

# python -m uvicorn aoi-oop:app --host 10.1.8.87 --port 8000 --reload

##########################################################################################################
global properties
properties = CDR_Properties()

app = FastAPI()

@app.post("/aoi")

async def read_root(string_entity: str = Body(...)):

    # Get the api data
    api_data = json.loads(string_entity)

    device_ids = api_data['device_id']
    table_id = api_data['table_id']
    server = api_data['server']
    api_start_date = api_data['start_date']
    api_end_date = api_data['end_date']
    try:
        region = api_data['region']
        sub_region = api_data['sub_region']
    except:
        region = 142
        sub_region = 145
    ##########################################################################################################

    print('INFO:     API Request Approved!!!')

    ##########################################################################################################
    
    api_start_date = str(api_start_date)
    api_end_date = str(api_end_date)

    ##########################################################################################################

    cotraveler_functions = CotravelerFunctions()
    utils = CDR_Utils(verbose=True)
    cassandra_tools = CassandraTools()
    load_or_create_simulation_report = LoadOrCreateSimulationReport()

    ##########################################################################################################

    start_date = utils.convert_datetime_to_ms_str(api_start_date)
    end_date = utils.convert_datetime_to_ms_str(api_end_date)

    ##########################################################################################################

    # Perform analysis on devices
    analyzer = GeoLocationAnalyzer(table_id = table_id)

    session = cassandra_tools.get_cassandra_connection(server)
    df_history = cassandra_tools.get_device_history_geo_chunks(device_ids, start_date, end_date,region,sub_region, server, session)

    analyzer.separate_devices_into_objects(df_history)

    # Perform AOI Detection on all devices
    analyzer.perform_analysis_on_all_devices()

    ##########################################################################################################
    
    # Perform AOI Classification
    analyzer.find_Home_AOI_for_all_devices()

    result_aoi = analyzer.identify_work_AOI_for_all_devices()

    #########################################################################################################

    # Save AOI classification results
    analyzer.save_aoi_classification_results()

    ##########################################################################################################

    # Perform AOI suspiciousness Analysis
    # aoi_suspiciousness_analysis = analyzer.evaluate_suspiciousness_of_all_devices()
    
    ##########################################################################################################

    # Perform Confidence Analysis
    geo_confidence_analyzer = GeospatialConfidenceAnalysis()
    print('INFO:    GEOSPATIAL CONFIDENCE ANALYSIS STARTED!')

    confidence_analysis = geo_confidence_analyzer.perform_confidence_test(data=df_history, 
                                                                          test_id=table_id,
                                                                          time_period={'START_DATE': api_start_date, 'END_DATE': api_end_date})
    
    ##########################################################################################################

    print('INFO:  AOI Detection & Classification Engine Successfully Completed!!!')

    ##########################################################################################################

    reporting = ReportGenerator(confidence_analysis)
    reporting.get_aoi_report(analyzer, table_id)

    ##########################################################################################################

    load_or_create_simulation_report.load_or_create_simulation_report(table_id=table_id)
    
    ##########################################################################################################
    print('INFO:  AOI Reports Successfully Created!!!')

    ##########################################################################################################

##########################################################################################################
    
if __name__ == "__main__":
    uvicorn.run(app, host = properties.api_host, port= properties.api_port_aoi, reload=False)
