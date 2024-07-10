import pandas as pd
import os
import vcis
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
import json


class CDR_Properties():
    
    # Initialize the class variables
    def __init__(self):
        #General Paths
        self.passed_filepath = os.path.dirname(os.path.dirname(os.path.dirname(vcis.__path__[0])))+ '/'
        self.passed_filepath_VCIS = self.passed_filepath + 'VCIS/'
        self.passed_filepath_src = self.passed_filepath_VCIS + 'src/'
        self.passed_filepath_data = self.passed_filepath_src + 'data/'
        self.passed_filepath_alfa = self.passed_filepath_data + 'bts/Site Database W41.xlsx'
        self.passed_filepath_cdr = self.passed_filepath_data +'cdr/' 
        self.passed_filepath_cdr_format1 = self.passed_filepath_cdr +'format1/'
        self.passed_filepath_cdr_format2 = self.passed_filepath_cdr +'format2/'
        self.passed_filepath_cdr_format3 = self.passed_filepath_cdr + 'format3/'
        self.passed_filepath_cdr_multi = self.passed_filepath_cdr + "multi_files_format/"
        self.passed_filepath_cdr_map = self.passed_filepath_data +'maps/'
        self.passed_filepath_excel = self.passed_filepath_data +'excel/'
        self.passed_filepath_reports = self.passed_filepath_data +'reports/'
        self.passed_filepath_json_paths = self.passed_filepath_data +'json_paths/'
        self.passed_filepath_trace_map = self.passed_filepath_data +'maps/'
        self.passed_filepath_tmaps = self.passed_filepath_data + 'triangulation_maps/'
                
        self.passed_filepath_bts = self.passed_filepath_data + "bts/"

        self.passed_filepath_results = self.passed_filepath_cdr +'results/' 

        self.passed_filepath_exec = (vcis.__path__[0]) + '/exec/'
        self.passed_filepath_exec_geocode = self.passed_filepath_exec + 'reverse_geocode.py'

        self.passed_filepath_temp_geocode = self.passed_filepath_excel + 'temp_data_rg.csv'
        
        
        #GPX Files Paths
        self.passed_filepath_gpx = self.passed_filepath_data +'gpx/'
        self.passed_filepath_gpx_folder = self.passed_filepath_gpx +'monitor/'
        self.passed_filepath_gpx_csv = self.passed_filepath_gpx +'csv/'
        self.passed_filepath_gpx_map = self.passed_filepath_gpx +'map/'
        self.passed_filepath_gpx_log = self.passed_filepath_gpx +'logs/'
        self.correlation_map_path = self.passed_filepath_data + 'maps/'

        self.passed_filepath_calls_in = self.passed_filepath_cdr_multi + "in_calls.txt"
        self.passed_filepath_calls_out = self.passed_filepath_cdr_multi + "out_calls.txt"
        self.passed_filepath_sms_in = self.passed_filepath_cdr_multi + "in_sms.txt"
        self.passed_filepath_sms_out = self.passed_filepath_cdr_multi + "out_sms.txt"
        self.passed_filepath_volte_in = self.passed_filepath_cdr_multi + "voltein.txt"
        self.passed_filepath_volte_out = self.passed_filepath_cdr_multi + "volteout.txt"
        self.passed_filepath_data_session = self.passed_filepath_cdr_multi + "data_session.txt"
        self.passed_filepath_data_session_roam = self.passed_filepath_cdr_multi + "data_session_roam.txt"
        self.passed_filepath_data_session_s = self.passed_filepath_cdr_multi + "data_session_s.txt"

        # Report properties
        self.passed_filepath_reports_png = self.passed_filepath_reports + 'png/'
        self.passed_filepath_reports_results = self.passed_filepath_reports + 'results/'
        self.passed_filepath_reports_information_page = self.passed_filepath_reports + 'information_page/'
        self.passed_filepath_reports_flags = self.passed_filepath_reports_png + 'flags/'
        self.report_valoores_png_path = self.passed_filepath_reports_png + 'valoores.png'
        self.passed_filepath_reports_html= self.passed_filepath_reports_results
        self.oracle_analytcis_report_table = 'tmp_Analytics_Report'
        self.oracle_analytcis_simulation_blocks_table = 'Analytics_Simulation_Blocks'
        self.passed_report_name_html_geo = 'demo_report_2'
        self.passed_report_name_html_cdr = 'demo_report_2'
        self.passed_report_name_html_aoi = 'demo_report_2'
        self.passed_report_name_html_simulation = 'simulation_report'
        self.passed_report_name_html_simulation_ai = 'simulation_report_ai'
        
        with open(self.passed_filepath_VCIS + 'config.json', 'r') as config_file:
            config = json.load(config_file)

        self.cassandra_server = config['cassandra_server']
        self.cassandra_key_space = config['cassandra_key_space']

        self.oracle_server = config['oracle_server']
        self.oracle_port = config['oracle_port']
        self.oracle_key_space = config['oracle_key_space']
        self.oracle_username = config['oracle_username']
        self.oracle_password = config['oracle_password']

        with open(self.passed_filepath_VCIS + 'database_mapping.json', 'r') as cassandra_mapping_file:
            cassandra_tables = json.load(cassandra_mapping_file)['cassandra_tables']

        self.cdr_table = cassandra_tables['cdr_table']
        self.bts_table = cassandra_tables['bts_table']
        self.bts_nodes_table = cassandra_tables['bts_nodes_table']
        self.bts_polygon_table = cassandra_tables['bts_polygon_table']
        self.cdr_main_table = cassandra_tables['cdr_main_table']
        self.cdr_bts_polygon_vertices_table = cassandra_tables['cdr_bts_polygon_vertices_table']
        self.lebanon_nodes_table = cassandra_tables['lebanon_nodes_table']

        with open(self.passed_filepath_VCIS + 'database_mapping.json', 'r') as oracle_mapping_file:
            oracle_tables = json.load(oracle_mapping_file)['oracle_tables']

        self.co_traveler_table = oracle_tables['co_traveler_table']
        self.correlation_table = oracle_tables['correlation_table']
        self.common_devices_table = oracle_tables['common_devices_table']
        self.common_devices_group_table = oracle_tables['common_devices_group_table']
        self.aoi_table = oracle_tables['aoi_table']
        self.aoi_poly_table = oracle_tables['aoi_poly_table']
        self.geo_confidence_analysis_table = oracle_tables['geo_confidence_analysis_table']
        self.aoi_procedure_table = oracle_tables['aoi_procedure_table']

        #API properties loaded from config.json
        self.api_host = config['api_host']
        self.api_port_cotraveler = config['api_port_cotraveler']
        self.api_port_correlation = config['api_port_correlation']
        self.api_port_aoi = config['api_port_aoi']
        self.api_port_confidence = config['api_port_confidence']
        self.api_report = config['api_report']
        self.api_port_agent = config['api_port_agent']
        self.app_port = config['app_port']
        self.api_port_common_devices = config['api_port_common_devices']
        self.api_report_pdf = config['api_report_pdf']
        self.api_port_rdp = config['api_port_rdp']
        self.api_port_reverse_geocoder = config["api_port_reverse_geocoder"]
        self.api_port_overshooting_detection = config["api_port_overshooting_detection"]
        self.oracle_lib_dir = '"/opt/oracle/instantclient_21_12"'

        self.oracle_procedure_query_aoi = """
                                        BEGIN
                                        SSDX_ENG.P_AOI_ENGINE('{}');
                                        END;
                                      """
        self._oracle_table_schema_query_cotraveler = """CREATE TABLE SSDX_TMP.{} ( \
            Rank NUMBER NOT NULL, \
            DEVICE_ID VARCHAR(255) NOT NULL, \
            GRID_SIMILARITY NUMBER, \
            HITS_SIMILARITY NUMBER, \
            COUNT NUMBER, \
            COMMON_LOCATIONS_HITS CLOB, \
            SEQUENCE_DISTANCE NUMBER, \
            LONGEST_SEQUENCE NUMBER \
        )"""

        self._oracle_table_schema_query_correlation = """CREATE TABLE SSDX_TMP.{} ( \
            Rank NUMBER NOT NULL, \
            DEVICE_ID VARCHAR(255) NOT NULL, \
            GRID_SIMILARITY NUMBER, \
            HITS_SIMILARITY NUMBER, \
            COUNT NUMBER, \
            COMMON_LOCATIONS_HITS CLOB, \
            distance NUMBER, \
            SEQUENCE_DISTANCE NUMBER, \
            LONGEST_SEQUENCE NUMBER \
        )"""

        self._oracle_table_schema_query_common_devices = """CREATE TABLE SSDX_TMP.{} (\
            DEVICE_ID VARCHAR(255) NOT NULL,\
            LOCATION_LATITUDE NUMBER,\
            LOCATION_LONGITUDE NUMBER,\
            USAGE_TIMEFRAME NUMBER,\
            MAIN_ID VARCHAR(255),\
            SERVICE_PROVIDER_ID NUMBER,\
            LOCATION_NAME VARCHAR(255)\
        )"""

        self._oracle_table_schema_query_common_devices_group = """CREATE TABLE SSDX_TMP.{} (\
            GROUP_ID NUMBER,\
            GROUP_IDS CLOB,\
            HTML_GRAPHS CLOB,\
            USAGE_TIMEFRAME NUMBER\
        )"""

        self._oracle_table_schema_query_aoi_result = """CREATE TABLE SSDX_TMP.{} ( \
            INDEX_NUMBER NUMBER , \
            NAME_ID VARCHAR(255) , \
            LAT NUMBER, \
            LNG NUMBER, \
            NBR_HITS NUMBER, \
            PERCENTAGE NUMBER, \
            NBR_DAYS NUMBER, \
            AOI_TYPE VARCHAR(255), \
            LOCATION VARCHAR(255), \
            COORDS CLOB \
        )"""
        self._oracle_table_schema_query_aoi_poly = """CREATE TABLE SSDX_TMP.{} ( \
            INDEX_NUMBER NUMBER NOT NULL, \
            DEVICE_ID_GEO  VARCHAR(255), \
            SHAPE_NAME VARCHAR(255), \
            SHAPE_ID NUMBER, \
            SHAPE_TYPE VARCHAR(255), \
            LOCATION_COORDS CLOB, \
            SUSPICIOUSNESS_PERCENTAGE NUMBER, \
            FLAGGED_AREA_CONTRIBUTION NUMBER, \
            AREA_SUSPICIOUSNESS NUMBER, \
            DEVICE_SUSPICIOUSNESS NUMBER \
        )"""
        
        self._oracle_table_schema_query_geo_confidence_analysis = """CREATE TABLE SSDX_TMP.{} ( \
            DEVICE_ID_GEO VARCHAR(255), \
            SERVICE_PROVIDER_ID NUMBER , \
            START_DATE DATE , \
            END_DATE DATE, \
            RECORDED_DAYS NUMBER, \
            RECORDED_HITS NUMBER, \
            TIME_PERIOD_CONSISTENCY NUMBER, \
            HOUR_OF_DAY_CONSISTENCY NUMBER, \
            DAY_OF_WEEK_CONSISTENCY NUMBER, \
            HITS_PER_DAY_CONSISTENCY NUMBER, \
            CONFIDENCE_SCORE NUMBER \
        )"""
        self._oracle_table_schema_query_analytics_report = """CREATE TABLE SSDX_TMP.{} ( \
            TYPE_PLOT VARCHAR2(255) , \
            HTML_FILE CLOB \
        )"""

        self._oracle_table_schema_query_analytics_report_blocks = """CREATE TABLE SSDX_TMP.{} ( \
            BLOCK_NAME VARCHAR2(255) , \
            BLOCK_TYPE VARCHAR2(255) , \
            BLOCK_ASSOCIATION VARCHAR2(255) , \
            DEVICE_ID VARCHAR2(255) , \
            BLOCK_CONTENT CLOB \
        )"""
        self._oracle_table_schema_query_agent_table = """CREATE TABLE SSDX_TMP.{} ( \
            ARRAY_CLOB CLOB \
        )"""

        self.oracle_timeline_query="""
                SELECT JSON_ARRAYAGG(JSON_ARRAY(D.LOCATION_LATITUDE,
                                                D.LOCATION_LONGITUDE,
                                                D.USAGE_TIMEFRAME,
                                                D.LOCATION_NAME,
                                                D.DEVICE_ID RETURNING CLOB) RETURNING CLOB) AS COORD
                FROM (SELECT ROW_NUMBER() OVER(ORDER BY COUNT(1) DESC) AS RANK,
                            D.LOCATION_NAME,
                            D.DEVICE_ID,
                            COUNT(1) AS HITS
                        FROM SSDX_ENG.TMP_REPORT_COORDINATES_6_{Simulation_id} D
                        GROUP BY D.DEVICE_ID, D.LOCATION_NAME
                        ORDER BY HITS DESC) T,
                        SSDX_ENG.TMP_REPORT_COORDINATES_6_{Simulation_id} D
                        WHERE T.DEVICE_ID = D.DEVICE_ID
                        AND T.RANK <= 100
            """

        #BTS FIXED IDS
        self.alfa_id = 10
        self.type_id = 4
        self.data_source_id_number = 2



        self.phone_number_before_rename = 'MSISDN/Number'
        self.phone_number = 'phone_number'
        self.bts_cell_id = 'bts_cell_id'
        self.status_code = 'status_code'
        self.status_bdate = 'status_bdate'
        self.location_altitude = 'location_altitude'
        self.creation_date = 'creation_date'
        self.created_by = 'created_by'
        self.update_date = 'update_date'
        self.updated_by = 'updated_by'
        self.equipment_id = 'equipment_id'
        self.bts_scrambling_code = 'bts_scrambling_code'
        self.logical_cell_code = 'logical_cell_code'
        self.device_frequency = 'device_frequency'
        self.network_type_code = 'network_type_code'
        self.rnc_code= 'rnc_code'
        self.bts_rnc_id = 'bts_rnc_id'
        self.is_splitter_used = 'is_splitter_used'
        self.routing_area_code = 'routing_area_code'
        self.bts_cell_internal_code = 'bts_cell_internal_code'
        self.bts_cell_name = 'bts_cell_name'
        self.location_area_code = 'location_area_code'
        self.tracking_area_code = 'tracking_area_code'
        self.site_name = 'site_name'
        self.cgi_id = 'cgi_id'
        self.location_longitude = 'location_longitude'
        self.location_latitude = 'location_latitude'
        self.location_azimuth = 'location_azimuth'
        self.bcch_code = 'bcch_code'
        self.earfcn_code = 'earfcn_code'
        self.uafrcn_code = 'uafrcn_code'
        self.bsi_code = 'bsi_code'
        self.bsc_code = 'bsc_code'
        self.rnc_code = 'rnc_code'
        self.mme_code = 'mme_code'
        self.evolved_node_b = 'evolved_node_b'
        self.service_provider_id = 'service_provider_id'
        self.location_lat = 'location_lat'
        self.location_long = 'location_long'
        self.sector = 'sector'
        self.country_division_name = 'country_division_name'
        self.location_azimuth_left_angle = 'left_angle'
        self.location_azimuth_right_angle = 'right_angle'

        self.nodes = 'nodes_in_sector'
        self.closest_point = 'closest_point'
        
        self.start_timestamp =      'Start'
        self.start_timestamp_ms =   'Start ms'
        self.end_timestamp =        'End'
        self.end_timestamp_ms =     'End ms'
        self.start_site_name =      'First Location Name'
        self.site_name_bf =         'eCGI'
        self.end_site_name =        'Last Location Name'
        self.imsi =                 'imsi_id'
        self.imei =                 'imei_id'
        self.usage_timeframe =      'usage_timeframe'
        self.bts_site_name =        'bts_cell_name'
        self.usage_timeframe_ms =   'usage_timeframe_ms'
        self.data_source_id = 'data_source_id'
        self.type_id_name = 'type_id'
        
        #Trace1 column names

        self.cell_id_start_d1 = 'START_LOCATION_ID'
        self.cell_id_end_d1 =   'END_LOCATION_ID'
        self.start_date_d123 =        'START_DATE'
        self.end_date_d13 =          'END_DATE'
        self.start_millisecond_d23 = 'START_MS'
        self.end_millisecond_d3 = 'END_MS'
        self.cell_id_d2 = 'CELL_ID'
        self.cell_id_start_d3 = 'CELL_ID_START'
        self.cell_id_end_d3 = 'CELL_ID_END'
        self.imsi_bf =                 'IMSI'
        self.imei_bf =                 'IMEI'
        self.data_type_bf = 'Data Type'
        self.data_type2_bf = 'Type'
        self.data_type = 'data_type'
        self.column_mapping_2g = {}

        self.column_mapping_3g = {'cell name':'Cell',
                                  'Long ':'Long',
                                  'lac':'LAC'
                                  }
        
        self.column_mapping_LTE = {'site':'Site',
                                   'type':'Type'
                                   }
        
        self.column_mapping =  {'CI': self.bts_cell_internal_code,
                                'Cell':self.bts_cell_name,
                                'Sector':self.sector,
                                'bts_tech':self.network_type_code,
                                'LAC':self.location_area_code,
                                'tac':self.tracking_area_code,
                                'Site':self.site_name,
                                'CGI':self.cgi_id,
                                'Long':self.location_longitude,
                                'Lat':self.location_latitude,
                                'Azimuth':self.location_azimuth,
                                'BCCH':self.bcch_code,
                                'EarfcnDL': self.earfcn_code,
                                'uarfcnDl':self.uafrcn_code,
                                'BSIC':self.bsi_code,
                                'SC':self.bsc_code,
                                'RNC':self.rnc_code,
                                'MME':self.mme_code,
                                'EnodeB ID':self.evolved_node_b,
                                'service_provider_id':self.service_provider_id,
                                'KAZA':self.country_division_name,
                                'Splitter':self.is_splitter_used,
                                'rac':self.routing_area_code,
                                'LCID':self.logical_cell_code
                            }
        
        
        self.start_records_column_mapping =  {self.start_site_name: self.bts_site_name,
                                self.start_timestamp :self.usage_timeframe,
                                self.start_timestamp_ms:self.usage_timeframe_ms,
                                self.phone_number_before_rename :self.phone_number

                            }
        
        self.end_records_column_mapping =  {self.end_site_name: self.bts_site_name,
                        self.end_timestamp : self.usage_timeframe,
                        self.end_timestamp_ms : self.usage_timeframe_ms,
                        self.phone_number_before_rename :self.phone_number
                    }

        self.data1_mapping = {
            self.cell_id_start_d1: self.cgi_id,
            self.cell_id_end_d1: self.cgi_id,
        }

        self.data2_mapping = {
            
        }
        
        self.data3_mapping = {
            
        }
        self.int_col = [
            self.bcch_code,
            self.uafrcn_code,
            self.bsi_code, 
            self.network_type_code, 
            self.status_code, 
            self.location_area_code, 
            self.tracking_area_code, 
            self.device_frequency,
            self.service_provider_id
            ]
        
        self.date_col = [
            self.status_bdate,
            self.update_date,
            self.creation_date
            ]
        
        self.str_col = [
            self.bts_cell_internal_code,
            self.site_name,
            self.cgi_id,
            self.location_longitude,
            self.location_latitude,
            self.location_altitude,
            self.location_azimuth,
            self.bsc_code,
            self.rnc_code,
            self.mme_code,
            self.evolved_node_b,
            self.country_division_name,
            self.is_splitter_used,
            self.bts_scrambling_code,
            self.routing_area_code,
            self.logical_cell_code
        ] 

        self.records_col = [
            self.start_timestamp,
            self.start_timestamp_ms,
            self.end_timestamp,
            self.end_timestamp_ms,
            self.start_site_name,
            self.end_site_name,
            self.phone_number,
            self.imsi,
            self.imei,
            self.data_type
        ]

        self.records_col_updated = [
            self.usage_timeframe,
            self.usage_timeframe_ms,
            self.bts_cell_name,
            self.cgi_id,
            self.location_latitude,
            self.location_longitude,	
            self.location_azimuth,
            self.phone_number,
            self.imsi,
            self.imei,
            self.data_type
            #self.nodes,
            #self.closest_point,
        ]
        self.records_start_col = [
            self.start_timestamp,
            self.start_timestamp_ms,
            self.start_site_name,
            self.phone_number,
            self.imsi,
            self.imei,
            self.data_type
        ]

        self.records_end_col = [
            self.end_timestamp,
            self.end_timestamp_ms,
            self.end_site_name,
            self.phone_number,
            self.imsi,
            self.imei,
            self.data_type
        ]
        
        
        self.data1_columns=[self.cell_id_start_d1,
                            self.cell_id_end_d1,
                            self.start_date_d123,
                            self.end_date_d13,
                            self.phone_number,
                            self.imsi_bf,
                            self.imei_bf,
                            self.data_type
                            ]

        self.data2_columns=[ 
                self.cell_id_d2,
                self.start_date_d123,
                self.start_millisecond_d23,
                self.phone_number,
                self.imsi_bf,
                self.imei_bf,
                self.data_type
        ]
                
        self.data3_columns=[
            self.cell_id_start_d3,
            self.cell_id_end_d3,
            self.start_date_d123,
            self.start_millisecond_d23,
            self.end_date_d13,
            self.end_millisecond_d3,
            self.phone_number,
            self.imsi_bf,
            self.imei_bf,
            self.data_type
        ]
        
        
        self.trace1_columns = [
                            self.cgi_id,
                            self.usage_timeframe,
                            self.location_latitude,
                            self.location_longitude,
                            self.location_azimuth,
                            self.phone_number,
                            self.imsi,
                            self.imei,
                            self.data_type
                        ]
        
        self.trace_columns = [
                    self.imei,
                    self.imsi,
                    self.cgi_id,
                    self.usage_timeframe,
                    self.location_latitude,
                    self.location_longitude,
                    self.location_azimuth,
                    self.phone_number,
                    self.data_type
                ]
        #################################################################################################################################

        self.cdr_columns_names = [self.imei,
                                  self.imsi,
                                  self.phone_number,
                                  self.cgi_id,
                                  self.usage_timeframe,
                                  self.type_id_name,
                                  self.data_source_id,
                                  self.service_provider_id
                                  ]
        self.cassandra_schema_mapping = {
                                            "imsi_id": StringType(),
                                            "usage_timeframe": LongType(),
                                            "cgi_id": StringType(),
                                            "data_source_id": IntegerType(),
                                            "imei_id": StringType(),
                                            "location_azimuth": IntegerType(),
                                            "location_latitude": DoubleType(),
                                            "location_longitude": DoubleType(),
                                            "phone_number": StringType(),
                                            "service_provider_id": IntegerType(),
                                            "type_id": IntegerType()
                                        }

        self.cassandra_schema_mapping_2 = {
                                            "imsi_id": 'object',
                                            "usage_timeframe": 'int64',
                                            "cgi_id": 'object',
                                            "data_source_id": 'int64',
                                            "imei_id": 'object',
                                            "location_azimuth": 'object',
                                            "location_latitude": 'float64',
                                            "location_longitude": 'float64',
                                            "phone_number": 'object',
                                            "service_provider_id": 'int64',
                                            "type_id": 'int64'
                                        } 
    
        
 
        #################################################################################################################################
        # cassandra queries
        #################################################################################################################################
        self.device_scan_query = """SELECT *  FROM  datacrowd.geo_data_year_month_region_subre WHERE expr(geo_index_year_month_region_subre_place_country, '{
                                                            "filter":[ {"type": "contains",
                                                            "field": "service_provider_id",
                                                            "values":  [ 1,2,3,4,5,6,7,8,9,10,7,12,8,6,16,17,18,19,20,21,22,15 ]},
                                                            { type: "geo_distance",
                                                            field: "place",
                                                            latitude: value1,
                                                            longitude: value2,
                                                            max_distance: "scan_distance m"},
                                                            { "type": "range",
                                                                "field": "usage_timeframe",
                                                                "lower": value3,
                                                                "upper": value4,
                                                                "include_lower": true,
                                                                "include_upper": true,
                                                                "doc_values": true }
                                                                ]}')"""


        self.device_history_query = """SELECT *  FROM  datacrowd.geo_data_year_month_region_subre 
                                                 WHERE device_id ='geo_id' and expr(geo_index_year_month_region_subre_place_country, '{
                                                            "filter":[ {"type": "contains",
                                                            "field": "service_provider_id",
                                                            "values":  [ 1,2,3,4,5,6,7,8,9,10,7,12,8,6,16,17,18,19,20,21,22,15 ]},
                                                            { "type": "range",
                                                                "field": "usage_timeframe",
                                                                "lower": value1,
                                                                "upper": value2,
                                                                "include_lower": true,
                                                                "include_upper": true,
                                                                "doc_values": true }
                                                                ]}')"""


        # self.device_history_list_query = """SELECT *  FROM  datacrowd.geo_data_year_month_region_subre
        #                                               WHERE device_id in (%s) and expr(geo_index_year_month_region_subre_place_country, '{
        #                                                     "filter":[ {
        #                                                     "type": "contains",
        #                                                     "field":"service_provider_id",                  "values":  [ 1,2,3,4,5,6,7,8,9,10,7,12,8,6,16,17,18,19,20,21,22,15 ]},
        #                                                     { "type": "range",
        #                                                         "field": "usage_timeframe",
        #                                                         "lower": value1,
        #                                                         "upper": value2,
        #                                                         "include_lower": true,
        #                                                         "include_upper": true,
        #                                                         "doc_values": true }
        #                                                         ]}')"""

        self.device_history_list_query = """SELECT *  FROM  datacrowd.geo_data_year_month_region_subre
                                                      WHERE expr(geo_index_year_month_region_subre_place_country, '{
                                                            "filter":[ {
                                                            "type": "contains",
                                                            "field":"service_provider_id",                  "values":  [ 1,2,3,4,5,6,7,8,9,10,7,12,8,6,16,17,18,19,20,21,22,15 ]},
                                                            { "type": "range",
                                                                "field": "usage_timeframe",
                                                                "lower": value1,
                                                                "upper": value2,
                                                                "include_lower": true,
                                                                "include_upper": true,
                                                                "doc_values": true },
                                                            { "type": "contains",
                                                                "field": "device_id",
                                                                "values": [replace_devices_list] }
                                                                ]}')"""

        self.polygon_activity_scan = """ SELECT *  FROM  datacrowd.table_name
                                        WHERE expr(index , '{
                                            "filter":[{
                                                type: "geo_shape",
                                                field: "place",
                                                shape: {
                                                    type: "wkt",
                                                    value: "POLYGON((replace_latitude_longitude_list))"} 
                                                    }
                                                ]}')"""

        self.polygon_activity_scan_with_time = """ SELECT *  FROM  datacrowd.geo_data_year_month_region_subre
                                WHERE expr(geo_index_year_month_region_subre_place_country , '{
                                    "filter":[{
                                        type: "geo_shape",
                                        field: "place",
                                        shape: {
                                            type: "wkt",
                                            value: "replace_polygon"} 
                                            },
                                            { "type": "range",
                                            "field": "usage_timeframe",
                                            "lower": replace_start_date,
                                            "upper": replace_end_date,
                                            "include_lower": true,
                                            "include_upper": true,
                                            "doc_values": true }
                                        ]}')"""

        self.polygon_activity_scan_1 = """ SELECT *  FROM  datacrowd.table_name
                                        WHERE expr(index , '{
                                            "filter":[{
                                                type: "geo_shape",
                                                field: "place",
                                                shape: {
                                                    type: "wkt",
                                                    value: "replace_polygon"} 
                                                    }
                                                ]}')"""

        self.nodes_activity_scan = """SELECT *  
                                            FROM  
                                                datacrowd.table_name
                                            WHERE expr(index , '{
                                            "filter":[
                                                    {type: "geo_distance",
                                                    field: "place",
                                                    latitude: replace_latitude,
                                                    longitude: replace_longitude,
                                                    max_distance:"scan_distance m"}
                                                ]}')
                                            """
        
        self.bts_node_query = """SELECT  *  FROM datacrowd.bts_nodes_projection
                                    WHERE expr(bts_nodes_projection_idx01,
                                    '{   "filter":[
                                    {"type": "contains",
                                      "field": "osmid",
                                        "values": [replace_osmid_list]} 
                                    ]}');  """

        
        self.bts_query = f"""SELECT *  FROM  datacrowd.{self.bts_table} WHERE cgi_id = 'replace_cgi_id' """
        
        self.connected_to_bts_query = """SELECT  *  FROM datacrowd.loc_location_cdr_main_new
                                            WHERE expr(idx02_cdr_main_new,
                                            '{   "filter":[
                                            {"type": "contains",
                                              "field": "cgi_id",
                                              "values": replace_list_cgis}]}')  ;
"""
                                            # { "type": "range",
                                            #   "field": "usage_timeframe",
                                            #   "lower": replace_lower_time,
                                            #   "upper": replace_upper_time,
                                            #   "include_lower": true,
                                            #   "include_upper": true,
                                            #    "doc_values": true }  
                                            # ]}')  ; """
        
        #################################################################################################################################


        self.country_codes_list = ["1313", "1050", "1023", "122", "111", "00974", "00966", "93", "355", "213", "1-684", "376", "244", "1-264", "672", "1-268", 
                                    "54", "374", "297", "61", "43", "994", "1-242", "973", "880", "1-246",
                                    "375", "32", "501", "229", "1-441", "975", "591", "387", "267", "55", 
                                    "246", "1-284", "673", "359", "226", "257", "855", "237", "1", "238", 
                                    "1-345", "236", "235", "56", "86", "61", "61", "57", "269", "682", "506",
                                    "385", "53", "599", "357", "420", "243", "45", "253", "1-767", "1-809", 
                                    "1-829", "1-849", "670", "593", "20", "503", "240", "291", "372", "251", 
                                    "500", "298", "679", "358", "33", "689", "241", "220", "995", "49", "233", 
                                    "350", "30", "299", "1-473", "1-671", "502", "44-1481", "224", "245", "592", 
                                    "509", "504", "852", "36", "354", "91", "62", "98", "964", "353", "44-1624", 
                                    "972", "39", "225", "1-876", "81", "962", "7", "254", "686", "383", "965", 
                                    "996", "856", "371","266", "231", "218", "423", "370", "352", "853", 
                                    "389", "261", "265", "60", "960", "223", "356","00971" , "692", "222", "230", "262", 
                                    "52", "691", "373", "377", "976", "382", "1-664", "212", "258", "95", "264", 
                                    "674", "977", "31", "599", "687", "64", "505", "227", "234", "683", "850", 
                                    "1-670", "47", "968", "92", "680", "970", "507", "675", "595", "51", "63", 
                                    "64", "48", "351", "1-787", "1-939", "974", "242", "262", "40", "7", "250", 
                                    "590", "290", "1-869", "1-758", "590", "508", "1-784", "685", "378", "239", 
                                    "966", "00966","221", "381", "248", "232", "65", "1-721", "421", "386", "677", "252", 
                                    "27", "82", "211", "34", "94", "249", "597", "47", "268", "46", "41", "963", 
                                    "886", "992", "255", "66", "228", "690", "676", "1-868", "216", "90", "993", 
                                    "1-649", "688", "1-340", "256", "380", "971", "44", "1", "598", "998", "678", 
                                    "379", "58", "84", "681", "212", "967", "260", "263"
                                    ]
        
        #################################################################################################################################

        self.report_banner_html =  """
                <div style="display: flex; justify-content: center; align-items: center; padding: 20px;">
                    <div style="flex: 1; text-align: center;">
                        <h1 style="font-size: 40px; font-family: 'Times New Roman', Times, serif; color: #002147; margin: 0;">{} - {}</h1>
                    </div>
                    <div style="margin-left: 20px;">
                        <img src="data:image/png;base64,{}" alt="Company Logo" width="84" height="17">
                    </div>
                </div>
                """

        # self.report_banner_html = """
        # <div class="six">
        #     <h1 style="text-align: center; color:#222; font-size:30px; font-weight:400; text-transform: uppercase; word-spacing: 1px; letter-spacing:2px; color:#002147;">
        #         {}
        #         <span style="display: block; line-height:2em; padding-bottom:15px; text-transform: none; font-size:.7em; font-weight: normal; font-style: italic; font-family: Times New Roman', Times, serif; color:#999; letter-spacing:-0.005em; word-spacing:1px; letter-spacing:none;">{}</span>
        #     </h1>
        #     <div style="margin-right: 20px;">
        #         <img src="data:image/png;base64,{}" alt="Company Logo" width="84" height="17">
        #     </div>
        # </div>
        
        # """

        self.report_devider_html = """<hr style="border: none; border-top: 5px solid red; width: 100%; margin-left: auto; margin-right: auto;">"""
        
        self.report_drowpdown_html = """<div style="padding-left: 0px;font-size: 10px;display: flex;color: #3C3633;">
            <h1>Possible CoTravelers</h1>
            </div>
            """
            
        self.test_html = """<div style="padding-left: 6px; font-size: 14px; color: #3C3633; border: 1px solid #000000; border-radius: 7px; text-align: center;">
            <h3 style="margin-bottom: 5px; font-family: Arial, sans-serif;">Main Device ID</h3>
            <h1 style="margin-top: 0; font-family: Arial, sans-serif;">3355f290-11b9-41ab-aaad-3d7668007e98</h1>
            </div>"""

        
        
        
        self.device_history_template = """
            <b>Simulation Name</b> : {report_saved_name}<br>
            <b>Simulation Type</b> : Device History<br>
            <b>Date</b> : {start_date} To {end_date}<br>
            <b>Number of Devices</b> : {device_or_number}<br>
            <b>Total Number of Hits</b> : {total_number_of_hits}<br>
            <b>Location</b> : {country_name},{city_name}<br><br>
            TThis is a Device History query type tracing all the historical activity of (<b>{device_or_number}</b>) device(s). <b>{report_saved_name}</b> is performed in (Location) between (<b>{start_date}</b>) and (<b>{end_date}</b>) where the activity of (<b>{device_or_number}</b>) is recorded passing through (<b>{country_name},{city_name}</b>).
        """
        # self.activity_scan_template = """
        #     When opting for this query type, the user has to select at least 1 AOI. This query type shows all the activity in the selected AOIs related to all data types. Activity scan is performed in <b>{city_name}</b> between <b>2022-08-06</b> and <b>{end_date}</b> where we found <b>{device_or_number}</b>
        # """
        self.activity_scan_template = """
            <b>Simulation Name</b> : {report_saved_name}<br>
            <b>Simulation Type</b> : Activity Scan By Hits<br>
            <b>Date</b> : {start_date} To {end_date}<br>
            <b>Number of Devices</b> : {device_or_number}<br>
            <b>Total Number of Hits</b> : {total_number_of_hits}<br>
            <b>Location</b> : {country_name},{city_name}<br><br>
            This is an Activity Scan query type showing all the activity of devices in (1) the selected Area(s) related to all data types. {report_saved_name} is performed in (Location) between <b>{start_date}</b> and <b>{end_date}</b> where we found (<b>{device_or_number}</b>) devices and a total of (<b>{total_number_of_hits}</b>) hits.
        """

        self.AOI_report_template = """
            The AOI Activity type provides the user the ability to select at least 1 AOI . This query type shows all the activity in the selected AOIs related to all data types.AOI is performed between {start_date} and {end_date} over {number} days where we found the following  {device_or_number} 
            """
        self.DHP_template="""
            <b>Simulation Name</b> : {report_saved_name}<br>
            <b>Simulation Type</b> : Device History Pattern<br>
            <b>Date Range</b> : {start_date} To {end_date}<br>
            <b>Number of Devices</b> : {device_or_number}<br>
            <b>Location</b> : {country_name},{city_name}<br><br>
            Device History Pattern is performed on this location <b>{report_saved_name}</b> between <b>{start_date}</b> and <b>{end_date}</b> where we found the following  <b>{device_or_number}</b> across the following locations <b>{country_name},{city_name}</b>.
            
            """
        self.DTP_template="""
            <b>Simulation Name</b> : {report_saved_name}<br>
            <b>Simulation Type</b> : Device Travel Pattern<br>
            <b>Date</b> : {start_date} To {end_date}<br>
            <b>Number of Devices</b> : {device_or_number}<br>
            <b>Total Number of Hits</b> : {total_number_of_hits}<br>
            <b>Location</b> : {country_name},{city_name}<br><br>
            This is a Device History Pattern query type showing the activity of all device(s) in (1) selected Area(s) related to all data types. <b>{report_saved_name}</b> is performed in (<b>{country_name},{city_name}</b>) between (<b>{start_date}</b>) and (<b>{end_date}</b>) where we found (<b>{device_or_number}</b>) devices. 
            """
        self.fixed_element_template="""
        When opting for this query type ,The user has to select 1 or more AOI and choose Fixed Element Scan query type.After simulation execution this query shows all the existing fixed elements in the selected AOI.Fixed Element Scan is performed on this location {report_saved_name} between {start_date} and {end_date} where we found the following device{s} {device_id}

            """
        
        self.device_history_pdf_template = """The Device History type provides the user the ability to gain the flexibility to select any Area of Interest. Upon selection, a devices field is presented on the interface, prompting users to input the device IDs for analysis. This enables users to specify the devices they wish to monitor. The query type then provides a comprehensive overview of the global activity of the specified devices within a specific timestamp. In this report, a Device History is performed under this name {report_saved_name} between {start_date} and {end_date} and for {device_or_number} who passed through {city_name} 
        """
        
        self.activity_scan_pdf_template = """When opting for this query type, the user has to select at least 1 AOI. This query type shows all the activity in the selected AOIs related to all data types. Activity scan is performed in {city_name} between {start_date} and {end_date} where we found {device_or_number}"""        

        self.css_set_column_width = """
                                        <style>
                                            .table-style {
                                            border-collapse: collapse;
                                            width: 100%;
                                            }

                                            .table-style th, .table-style td {
                                            border: 1px solid #ddd;
                                            padding: 8px;
                                            text-align: center;
                                            color: #002147; /* Text color for table cells */
                                            font-family: 'Times New Roman', Times, serif; /* Font family for the entire table */
                                            font-size: 16px; /* Font size for the entire table */
                                            }

                                            .table-style th {
                                            background-color: #03045E;
                                            color: white;
                                            font-weight: bold; /* Font weight for table headers */
                                            }

                                            .table-style td {
                                            background-color: #f2f2f2; /* Background color for table cells */
                                            }

                                            .table-style {
                                            width: 100%;
                                            }

                                            /* .table-style th, .table-style td {
                                            width: 33%; */
                                            }
                                        </style>
                                        """
        


