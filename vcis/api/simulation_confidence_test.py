import json
import time
import uvicorn

import warnings
warnings.filterwarnings('ignore')

from fastapi import FastAPI, Body

from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis
from vcis.utils.data_processing_utils import DataProcessing

# python -m uvicorn aoi-oop:app --host 10.1.8.87 --port 8000 --reload

##########################################################################################################

app = FastAPI()

@app.post("/confidence")

async def read_root(string_entity: str = Body(...)):

    # Get the api data
    api_data = json.loads(string_entity)
    simulation_id = api_data['simulation_id']
    
    ##########################################################################################################

    oracle_tools = OracleTools()
    processor = DataProcessing()

    ##########################################################################################################

    report_config_query = f"SELECT * FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID ={simulation_id}"
    report_info = oracle_tools.get_oracle_query(report_config_query)

    report_info = processor.remove_unnecessary_columns(report_info)
    processor.print_table(report_info)

    # Get the report type from the simulation table
    report_type = report_info['LOC_REPORT_TYPE'][0]
        
    if report_type == 11:
        simulation_table = oracle_tools.get_table("SSDX_ENG", "DATACROWD_CDR_DATA", simulation_id, True)
    else:    
        simulation_table = oracle_tools.get_table("SSDX_ENG", "TMP_REPORT_COORDINATES_6", simulation_id, True)

    # Get the report type
    report_type_query = f"SELECT REPORT_TYPE, REPORT_NAME FROM TECHDBA.CYB_REPORT_HTML_TEMPLATES where REPORT_TYPE={report_type}"
    report_type_table = oracle_tools.get_oracle_query(report_type_query)
    
    simulation_type = report_type_table['REPORT_NAME'][0]
    print(simulation_type)

    ##########################################################################################################

    geo_confidence_analyzer = GeospatialConfidenceAnalysis()
    print('\nINFO:    GEOSPATIAL CONFIDENCE ANALYSIS STARTED!\n')

    start_date = report_info['FILTER_BDATE'].dt.date.values[0]
    end_date = report_info['FILTER_EDATE'].dt.date.values[0]

    confidence_analysis = geo_confidence_analyzer.perform_confidence_test(
        data=simulation_table, 
        test_id=simulation_id, 
        time_period={'START_DATE': start_date, 'END_DATE': end_date},
        analysis_mode=simulation_type,
        verbose=True
        
    )

    confidence_analysis.drop(columns=['DEVICE_ID_GEO'], inplace=True)

    confidence_analysis.to_csv('confidence_analysis.csv', index=False)

    print('\nINFO:    GEOSPATIAL CONFIDENCE ANALYSIS COMPLETE!\n\n', confidence_analysis)

    return "Done"

##########################################################################################################
    
if __name__ == "__main__":
    uvicorn.run(app, host = "10.1.2.106", port= 2002, reload=False) # "10.1.10.110"