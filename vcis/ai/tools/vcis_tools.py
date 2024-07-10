import pandas as pd
import json
import base64
import country_converter as coco
import plotly.express as px
from reportlab.platypus import Table
from reportlab.lib.units import inch

from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.plots.plots_tools import PlotsTools
from vcis.cotraveler.cotraveler_main import CotravelerMain
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.ai.models.pandasai.pandasai_model import PandasAIModel
from vcis.correlation.correlation_main import CorrelationMain
from vcis.aoi_classification.aoi_classification_engine import AOI_Classification
class vcisTools():
    def __init__(self, verbose: bool = False,passed_server:str = '10.1.10.110'):
        self.verbose = verbose
        self.utils = CDR_Utils(verbose=True)
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools()
        self.correlation_functions = CDRCorrelationFunctions()
        self.oracle_tools = OracleTools()
        self.cotraveler_main = CotravelerMain()
        self.report = ReportGenerator()
        self.pandasai = PandasAIModel()
        self.correlation_main = CorrelationMain()
        self.aoi_classification = AOI_Classification()
        self.server = passed_server

################################################################################################################### 
# VCIS Tools  
###################################################################################################################    

    def get_device_history_tool(self,device_id:str,start_date:str,end_date:str):

        start_date = self.utils.convert_datetime_to_ms(start_date)
        end_date = self.utils.convert_datetime_to_ms(end_date)
        device_history = self.cassandra_tools.get_device_history_geo(device_id,start_date,end_date,server=self.server)
        device_history['service_provider_id'] = device_history['service_provider_id'].apply(lambda x: f"GeoSpatial Provider {x}")

        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  device_history.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)


        device_history = self.utils.convert_ms_to_datetime(device_history)
        date_only = device_history['usage_timeframe'].dt.date
        device_history = self.utils.add_reverseGeocode_columns(device_history)
        print(device_history.columns)
        # Count the number of unique days
        num_days = len(date_only.unique())
        num_records = len(device_history)
        countries =  device_history['country'].unique()
        cities =  device_history['city'].unique()
        message = f"""
            Device History was successfully performed on this Device ID: {device_id}
            Quick Summary:
            - Number of Records: {num_records}
            - Number of Days: {num_days}
            - Countries: {', '.join(countries)}
            - Cities: {', '.join(cities)}
            """
        print(message)
        response = {"answer":message,
                    "action_type":0}
        return response
    
###################################################################################################################    
    def get_activity_scan(self,latitude:float,longitude:float,start_date:str,end_date:str,scan_distance:int):

        start_date = self.utils.convert_datetime_to_ms(start_date)
        end_date = self.utils.convert_datetime_to_ms(end_date)
        device_scan,devices_list = self.correlation_functions.get_device_list_from_device_scan(latitude = latitude,longitude = longitude,start_date = start_date,end_date = end_date,scan_distance = scan_distance,passed_server=self.server)
        device_scan['service_provider_id'] = device_scan['service_provider_id'].apply(lambda x: f"GeoSpatial Provider {x}")
        # device_scan = utils.convert_datetime_to_ms(device_scan)

        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  device_scan.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        print(oracle_table)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
        device_scan = self.utils.convert_ms_to_datetime(device_scan)
        date_only = device_scan['usage_timeframe'].dt.date
        device_scan = self.utils.add_reverseGeocode_columns(device_scan)
        print(device_scan.columns)
        # Count the number of unique days
        num_devices = len(device_scan['device_id'].unique())
        num_days = len(date_only.unique())
        num_records = len(device_scan)
        countries =  device_scan['country'].unique()
        cities =  device_scan['city'].unique()
        message = f"""
        Device Scan was successfully performed:
        Quick Summary:
        - Number of Devices: {num_devices}
        - Number of Records: {num_records}
        - Number of Days: {num_days}
        - Countries: {', '.join(countries)}
        - Cities: {', '.join(cities)}
        """
        print(message)
        response = {"answer":message,
                "action_type":1}
        return response
    
################################################################################################################### 

    def get_device_history_travel_pattern(self,latitude:float,longitude:float,start_date:str,end_date:str,scan_distance:int):
            start_date = self.utils.convert_datetime_to_ms(start_date)
            end_date = self.utils.convert_datetime_to_ms(end_date)
            df_dhtp = self.cassandra_tools.get_device_dhtp(latitude=latitude,longitude=longitude,start_date=start_date,end_date=end_date,scan_distance = scan_distance, server=self.server)
            print(df_dhtp)
            df_dhtp['service_provider_id'] = df_dhtp['service_provider_id'].apply(lambda x: f"GeoSpatial Provider {x}")
            oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
            list_of_lists =  df_dhtp.values.tolist()
            oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
            print(oracle_table)
            self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
            device_scan = self.utils.convert_ms_to_datetime(df_dhtp)
            date_only = device_scan['usage_timeframe'].dt.date
            device_scan = self.utils.add_reverseGeocode_columns(device_scan)
            print(device_scan.columns)
            # Count the number of unique days
            num_devices = len(device_scan['device_id'].unique())
            num_days = len(date_only.unique())
            num_records = len(device_scan)
            countries =  device_scan['country'].unique()
            cities =  device_scan['city'].unique()
            message = f"""
            Device History Travel Pattern was successfully performed:
            Quick Summary:
            - Number of Devices: {num_devices}
            - Number of Records: {num_records}
            - Number of Days: {num_days}
            - Countries: {', '.join(countries)}
            - Cities: {', '.join(cities)}
            """
            print(message)
            response =  {"answer":message,
                        "action_type":5}    
            return response
    
###################################################################################################################

    def get_simulation(self,simulation_name:str):

        query = f"SELECT LOC_REPORT_TYPE,LOC_REPORT_CONFIG_ID,loc_report_name FROM locdba.loc_report_config where loc_report_name like '%{simulation_name}%'  "

        result= self.oracle_tools.get_oracle_query(query)

        print(result.columns)
        report_type = result['LOC_REPORT_TYPE'][0]
        table_id = result['LOC_REPORT_CONFIG_ID'][0]
        if report_type==11:
            table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)
        table = table[[ 'LOCATION_LATITUDE', 'LOCATION_LONGITUDE', 'DEVICE_ID', 'USAGE_TIMEFRAME','LOCATION_NAME','SERVICE_PROVIDER_ID' ]].drop_duplicates()
        table['SERVICE_PROVIDER_ID'] = table['SERVICE_PROVIDER_ID'].apply(lambda x: f"GeoSpatial Provider {x}")
        table = self.utils.convert_datetime_to_ms(table,date_column_name = 'USAGE_TIMEFRAME')
        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  table.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
        table.columns = table.columns.str.lower()
        device_scan = self.utils.convert_ms_to_datetime(table)

        date_only = device_scan['usage_timeframe'].dt.date
        device_scan = self.utils.add_reverseGeocode_columns(device_scan)
        print(device_scan.columns)
        # Count the number of unique days
        num_devices = len(device_scan['device_id'].unique())
        num_days = len(date_only.unique())
        num_records = len(device_scan)
        countries =  device_scan['country'].unique()
        cities =  device_scan['city'].unique()
        messages = f"""
        {simulation_name} Simulation was successfully Loaded:
        Quick Summary:
        - Number of Devices: {num_devices}
        - Number of Records: {num_records}
        - Number of Days: {num_days}
        - Countries: {', '.join(countries)}
        - Cities: {', '.join(cities)}
        """
        print(messages)
        response = {"answer":messages,
                    "action_type":3}
        return response
    
###################################################################################################################

    def get_polygon_scan_saved_shape(self,shape_name:str,start_date:str,end_date:str):
         
        query = f"select object_shape_desc  ,SUBSTR(block_content,1,100000) object_shape_data  from LOCDBA.LOC_LOCATION_MAP_OBJECT_SHAPE where object_shape_data like '%{shape_name}%' "
        print(query)
        result= self.oracle_tools.get_oracle_query(query)
        shape = result['OBJECT_SHAPE_DATA'][0]
        shape = json.loads(shape)
        print(shape)
        latitude = shape['center']['lat']
        longitude = shape['center']['lng']
        scan_distance = shape['radius']


        start_date = self.utils.convert_datetime_to_ms(start_date)
        end_date = self.utils.convert_datetime_to_ms(end_date)
        device_scan,devices_list = self.correlation_functions.get_device_list_from_device_scan(latitude = latitude,longitude = longitude,start_date = start_date,end_date = end_date,scan_distance = scan_distance,passed_server=self.server)
        device_scan['service_provider_id'] = device_scan['service_provider_id'].apply(lambda x: f"GeoSpatial Provider {x}")
        # device_scan = utils.convert_datetime_to_ms(device_scan)

        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  device_scan.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)

        device_scan = self.utils.convert_ms_to_datetime(device_scan)
        date_only = device_scan['usage_timeframe'].dt.date
        device_scan = self.utils.add_reverseGeocode_columns(device_scan)
        print(device_scan.columns)
        # Count the number of unique days
        num_devices = len(device_scan['device_id'].unique())
        num_days = len(date_only.unique())
        num_records = len(device_scan)
        countries =  device_scan['country'].unique()
        cities =  device_scan['city'].unique()
        message = f"""
        Device Scan was successfully performed on {shape_name}:
        Quick Summary:
        - Number of Devices: {num_devices}
        - Number of Records: {num_records}
        - Number of Days: {num_days}
        - Countries: {', '.join(countries)}
        - Cities: {', '.join(cities)}
        """
        print(message)
        response = {"answer":message,
                "action_type":4}
        return response
###################################################################################################################

    def get_generate_report_from_simulation(self,simulation_name:str,description:str):
        query = f"SELECT LOC_REPORT_TYPE,LOC_REPORT_CONFIG_ID,loc_report_name FROM locdba.loc_report_config where loc_report_name like '%{simulation_name}%'  "

        result= self.oracle_tools.get_oracle_query(query)

        print(result.columns)
        report_type = result['LOC_REPORT_TYPE'][0]
        table_id = result['LOC_REPORT_CONFIG_ID'][0]
        if report_type==11:
            table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

        report_intoduction = self.report.get_simulation_report(main_table = result , report_table = table,table_id = table_id , file_name ='simulation_report',report_type=report_type,report_name=simulation_name,description = description)

        table = table[[ 'LOCATION_LATITUDE', 'LOCATION_LONGITUDE', 'DEVICE_ID', 'USAGE_TIMEFRAME','LOCATION_NAME','SERVICE_PROVIDER_ID' ]].drop_duplicates()
        table['SERVICE_PROVIDER_ID'] = table['SERVICE_PROVIDER_ID'].apply(lambda x: f"GeoSpatial Provider {x}")
        table = self.utils.convert_datetime_to_ms(table,date_column_name = 'USAGE_TIMEFRAME')
        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  table.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
        table.columns = table.columns.str.lower()
        device_scan = self.utils.convert_ms_to_datetime(table)

        date_only = device_scan['usage_timeframe'].dt.date
        device_scan = self.utils.add_reverseGeocode_columns(device_scan)
        print(device_scan.columns)
        # Count the number of unique days
        num_devices = len(device_scan['device_id'].unique())
        num_days = len(date_only.unique())
        num_records = len(device_scan)
        countries =  device_scan['country'].unique()
        cities =  device_scan['city'].unique()
        messages = f"""
        {simulation_name} Simulation was successfully Loaded:
        Quick Summary:
        - Number of Devices: {num_devices}
        - Number of Records: {num_records}
        - Number of Days: {num_days}
        - Countries: {', '.join(countries)}
        - Cities: {', '.join(cities)}
        - Discription was added:{report_intoduction}
        """
        print(messages)
        response = {"answer":messages,
                    "action_type":3}
        return response


###################################################################################################################
# Analytics VCIS Tools
###################################################################################################################

    def get_cotraveler(self,device_id:str,start_date:str,end_date:str):
        start_date = self.utils.convert_datetime_to_ms(start_date)
        end_date = self.utils.convert_datetime_to_ms(end_date)
        distance = 100
        table , df_merged_list, df_device, df_history,distance = self.cotraveler_main.cotraveler(device_id=device_id,
                start_date=start_date,
                end_date=end_date,
                local=True,
                distance=distance,
                server=self.server,
                region=142,
                sub_region = 145)
        
        table['COMMON_LOCATIONS_HITS'] = table['COMMON_LOCATIONS_HITS'].astype(str)
        table_insert = table[['RANK','DEVICE_ID','GRID_SIMILARITY','COUNT','COMMON_LOCATIONS_HITS','SEQUENCE_DISTANCE','LONGEST_SEQUENCE']]
        # table.to_csv('table.csv')
        
        df_history['service_provider_id'] = df_history['service_provider_id'].apply(lambda x: f"GeoSpatial Provider {x}")
        df_history = self.utils.convert_datetime_to_ms(df_history)

        self.oracle_tools.drop_create_insert(table_insert,
                                        self.properties.co_traveler_table,
                                        123,
                                        self.properties._oracle_table_schema_query_cotraveler
                                        )
        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  df_history.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
        
        print(oracle_table)
        response = {"answer":f'{len(table)} Possible CoTraveler where detected for this device id {device_id}',
                    "action_type":2} 
        return response
    
    def get_qa_over_simulation(self,question:str=None,simulation_name:str=None):
        query = f"SELECT LOC_REPORT_TYPE,LOC_REPORT_CONFIG_ID,loc_report_name FROM locdba.loc_report_config where loc_report_name like '%{simulation_name}%'  "

        result= self.oracle_tools.get_oracle_query(query)

        print(result.columns)
        report_type = result['LOC_REPORT_TYPE'][0]
        table_id = result['LOC_REPORT_CONFIG_ID'][0]
        if report_type==11:
            table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

        table = table[[ 'LOCATION_LATITUDE', 'LOCATION_LONGITUDE', 'DEVICE_ID', 'USAGE_TIMEFRAME','LOCATION_NAME','SERVICE_PROVIDER_ID' ]].drop_duplicates()
        table['SERVICE_PROVIDER_ID'] = table['SERVICE_PROVIDER_ID'].apply(lambda x: f"GeoSpatial Provider {x}")
        table = self.utils.convert_datetime_to_ms(table,date_column_name = 'USAGE_TIMEFRAME')
        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  table.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)
        table.columns = table.columns.str.lower()
        device_scan = self.utils.convert_ms_to_datetime(table)
        device_scan = self.utils.add_reverseGeocode_columns(device_scan)
        device_scan.columns = ['Unnamed: 0', 'location_latitude', 'location_longitude', 'device_id',
       'usage_timeframe', 'device_name', 'service_provider_id', 'provinance',
       'country', 'city', 'lat_long']
        llm = self.pandasai.get_model(local=False)
        df = self.pandasai.get_dataframe(device_scan,llm)
        response = df.chat(question)
        oracle_table = pd.DataFrame(columns=['ARRAY_CLOB'])
        list_of_lists =  response.values.tolist()
        oracle_table.loc[0, 'ARRAY_CLOB'] = json.dumps(list_of_lists)
        self.oracle_tools.drop_create_insert(oracle_table,'agent_table',123,self.properties._oracle_table_schema_query_agent_table)

        # with open(self.properties.passed_filepath_reports_png + "ba4eaf7e-33a6-4c65-8dc2-626345a3ab70.png", "rb") as image_file:
        #     encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        response = {"answer":str(response)} 
        return response
    
    def get_geofencing_countries (self,simulation_id):
        query = f"SELECT LOC_REPORT_TYPE,LOC_REPORT_CONFIG_ID,loc_report_name FROM locdba.loc_report_config where LOC_REPORT_CONFIG_ID like '%{simulation_id}%'  "

        result= self.oracle_tools.get_oracle_query(query)

        print(result.columns)
        report_type = result['LOC_REPORT_TYPE'][0]
        table_id = result['LOC_REPORT_CONFIG_ID'][0]
        if report_type==11:
            table = self.oracle_tools.get_table("SSDX_ENG","DATACROWD_CDR_DATA",table_id,True)
        else:    
            table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",table_id,True)

        table = table[[ 'LOCATION_LATITUDE', 'LOCATION_LONGITUDE', 'DEVICE_ID', 'USAGE_TIMEFRAME','LOCATION_NAME','SERVICE_PROVIDER_ID' ]].drop_duplicates()
        table['SERVICE_PROVIDER_ID'] = table['SERVICE_PROVIDER_ID'].apply(lambda x: f"GeoSpatial Provider {x}")
        table = self.utils.convert_datetime_to_ms(table,date_column_name = 'USAGE_TIMEFRAME')

        devices_list = table['DEVICE_ID'].unique().tolist()

    def get_everything(self,device_id_list:list=None,
                       start_date:str=None,
                       end_date:str=None,
                       server:str='10.1.10.110',
                       region:str='142',
                       sub_region:str='145',
                       local:str=None,
                       table_id:int=None,
                       cotraveler_distance:int=100,
                       correlation_distance:int=30):
        
        aoi_table = self.aoi_classification.aoi_classification(device_ids = device_id_list, table_id = table_id,start_date = start_date,end_date = end_date,server = server,region = region,sub_region = sub_region)
        # aoi_table.to_csv( 'aoi_table.csv',index=False)

        
        for device_id in device_id_list:
            table , df_merged_list ,df_device ,df_common,df_history, cotraveler_distance,heatmap_plots,cotraveler_barchart,cotraveler_user_prompt = self.cotraveler_main.cotraveler(device_id=device_id,
                                                                                        start_date=start_date,
                                                                                        end_date=end_date,
                                                                                        local=local,
                                                                                        distance=cotraveler_distance,
                                                                                        server=server,
                                                                                        region=region,
                                                                                        sub_region = sub_region
                                                                                        )
            top_id = self.correlation_main.gps_correlation(
                df_geo= None,
                device_id = device_id,
                start_date = start_date,
                end_date = end_date,
                local = local,
                distance = correlation_distance,
                server = server,
                region = region,
                sub_region = sub_region)
            print(top_id)
        print(device_id_list)
        print(type(device_id_list))
            
        correlation_description=''
        for index, row in top_id.iterrows():                
            # Format the information into a string
            description = f"device_id is correlated to this IMSI :{row['imsi_id']} IMEI:{row['imei_id']} Phone Number:{row['phone_number']} "
            correlation_description+=description
        print(correlation_description)
        table.to_csv( 'table.csv',index=False)
        df_merged_list.to_csv( 'df_merged_list.csv',index=False)
        df_device.to_csv( 'df_device.csv',index=False)
        df_common.to_csv( 'df_common.csv',index=False)
        df_history.to_csv( 'df_history.csv',index=False)
        

        print(top_id)

        return aoi_table, table ,df_merged_list, df_device,df_common, df_history, cotraveler_distance, top_id,heatmap_plots,cotraveler_barchart , correlation_description, cotraveler_user_prompt

