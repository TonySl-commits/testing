import warnings
warnings.filterwarnings('ignore')

from vcis.aoi_classification.geolocation_data_analyzer import GeoLocationAnalyzer
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.confidence_analysis.geo_confidence_analysis import GeospatialConfidenceAnalysis
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools

# python -m uvicorn aoi-oop:app --host 10.1.8.87 --port 8000 --reload

##########################################################################################################
class AOI_Classification():
    def __init__(self):
        self.utils = CDR_Utils()
        self.cassandra_tools = CassandraTools()
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools()
        
    def aoi_classification(self,device_ids:list, table_id:int, server:str = '10.10.10.101', start_date:str = '2020-01-01', end_date:str = '2020-01-01',region:str='142',sub_region:str='145'):


        ##########################################################################################################

        print('INFO:     API Request Approved!!!')

        # ##########################################################################################################
        
        # start_date = str(start_date)
        # end_date = str(end_date)

        ##########################################################################################################

        cotraveler_functions = CotravelerFunctions()
        utils = CDR_Utils()
        cassandra_tools = CassandraTools()

        ##########################################################################################################

        session = self.cassandra_tools.get_cassandra_connection(server=server)
        
        # start_date = utils.convert_datetime_to_ms_str(start_date)
        # end_date = utils.convert_datetime_to_ms_str(start_date)

        ##########################################################################################################

        # Perform analysis on devices
        analyzer = GeoLocationAnalyzer(table_id = table_id)

        # device_list = cotraveler_functions.separate_list(device_ids,1)

        df_history = cassandra_tools.get_device_history_geo_chunks(device_list_separated = device_ids, start_date=start_date,end_date= end_date, server = server,session=session)


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

        confidence_analysis = geo_confidence_analyzer.evaluate_confidence_in_analysis_for_all_devices(df_history, table_id)
        
        ##########################################################################################################

        print('INFO:  AOI Detection & Classification Engine Successfully Completed!!!')

        ##########################################################################################################

        reporting = ReportGenerator(confidence_analysis)
        device_report_items_dictionary = reporting.get_aoi_report(analyzer, table_id)

        ##########################################################################################################

        # print('INFO:  AOI Reports Successfully Created!!!')

        ##########################################################################################################

        return device_report_items_dictionary
    
    ##########################################################################################################
