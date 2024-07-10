import pandas as pd
from IPython.display import display
import json

from vcis.aoi_classification.aoi_support_functions import analyze_suspiciousness
from vcis.aoi_classification.geolocation_data_list import GeoLocationDataList
from vcis.aoi_classification.geolocation_data import GeoLocationData
from vcis.cotraveler.cotraveler_functions import CotravelerFunctions
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.utils.properties import CDR_Properties
import math

class GeoLocationAnalyzer:
    def __init__(self,table_id: int = None):
        # Data
        self.table_id = table_id     

        # Tools
        self.geolocation_data_list = GeoLocationDataList()
        self.cotraveler_functions = CotravelerFunctions()
        self.properties = CDR_Properties()
        self.oracle_tools = OracleTools()

        # Results
        self.df_polygon = pd.DataFrame(columns=('INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS'))
        self.result_AOI = pd.DataFrame(columns=['index', 'NAME_ID', 'LAT', 'LNG', 'NBR_HITS', 'Percentage', 'NBR_DAYS','TYPE', 'LOCATION', 'COORDS'])
        self.aoi_confidence_analysis = pd.DataFrame(columns=['Device_ID_geo', 'Min_Date', 'Max_Date', 'NUMBER_OF_RECORDED_DAYS', 'NUMBER_OF_RECORDED_HITS', 'Consistency_Score', 'Adjusted_Consistency_Score'])
        self.result_POLY = pd.DataFrame(columns=('INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS'))

    def separate_devices_into_objects(self, df : pd.DataFrame):
        df.columns = ['Device_ID_geo', 'Latitude', 'Longitude','Timestamp', 'Location_Name', 'Service_Provider_ID']
        df['Latitude'] = df['Latitude'].astype(float)
        df['Longitude'] = df['Longitude'].astype(float)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')

        # Iterate through the unique device IDs and create a GeoLocationData object for each
        unique_device_ids = df['Device_ID_geo'].unique()

        for device_id_geo in unique_device_ids:
            filtered_data = df[df['Device_ID_geo'] == device_id_geo]
            filtered_data.reset_index(inplace=True)
            filtered_data.dropna(inplace=True)
            geolocation_data = GeoLocationData(device_id_geo, filtered_data)
            self.geolocation_data_list.add_geolocation_data(geolocation_data)
                    
    def analyze_clusters_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].analyze_clusters()

    def get_custom_clusters_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].get_custom_centroids()
            
    def determine_AOIs_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].determine_AOIs()
    
    
    def perform_analysis_on_all_devices(self):
        self.analyze_clusters_for_all_devices()
        self.get_custom_clusters_for_all_devices()
        self.determine_AOIs_for_all_devices()

        display('INFO:  AOI Detection Complete!')
            
    def find_Home_AOI_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].identify_home_AOIs()

        display('INFO:  Home AOI Classification Complete!')
        
    def identify_work_AOI_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].identify_work_AOIs()
            self.result_AOI = pd.concat([self.result_AOI, self.geolocation_data_list[device].result_AOI], ignore_index=True)
        
        self.result_AOI.rename(columns={'index':'INDEX_NUMBER','Percentage':'PERCENTAGE','TYPE':'AOI_TYPE'}, inplace=True)
        display('INFO:  Work AOI Classification Complete!')
        
        return self.result_AOI
    
    def save_aoi_classification_results(self):
        self.result_AOI['COORDS'] = self.result_AOI['COORDS'].astype(str)

        self.oracle_tools.drop_create_insert(self.result_AOI,
                                             self.properties.aoi_table,
                                             self.table_id,
                                             self.properties._oracle_table_schema_query_aoi_result)
        
        display('INFO:  AOI Table Successfully Saved!')

    
    def check_aois_intersection_with_polygons(self, df: pd.DataFrame):    
        display('INFO:   Checking AOIS intersection with polygons')

        if df.empty:
            is_suspicious = False
            print("No rows found in the table")
            df_polygon = pd.DataFrame(columns=('INDEX_NUMBER','DEVICE_ID_GEO','SHAPE_NAME','SHAPE_ID','SHAPE_TYPE','LOCATION_COORDS','SUSPICIOUSNESS_PERCENTAGE','FLAGGED_AREA_CONTRIBUTION','AREA_SUSPICIOUSNESS','DEVICE_SUSPICIOUSNESS'))
        else:
            is_suspicious = True
            df_polygon = df
            for i in range(len(df_polygon)):
                jason = json.loads(df_polygon['SHAPE'][i])
                df_polygon['SHAPE'][i] = jason[0]
                
        self.df_polygon = df_polygon

        return is_suspicious
                
    def evaluate_suspiciousness(self, is_suspicious):
        if is_suspicious:
            name_ids = self.result_AOI['NAME_ID'].unique().tolist()

            for name_id in name_ids:
                result_subset = self.result_AOI[self.result_AOI['NAME_ID'] == name_id]
                polygon_subset = self.df_polygon[self.df_polygon['NAME_ID'] == name_id]
                
                if not polygon_subset.empty:
                    # Analyze the suspiciousness of a device
                    df = analyze_suspiciousness(result_subset, polygon_subset)
                    self.result_POLY = pd.concat([self.result_POLY, df])
            
            self.result_POLY['LOCATION_COORDS'] = self.result_POLY['LOCATION_COORDS'].astype(str)

        # Insert data into cassandra
        self.oracle_tools.drop_create_insert(self.result_POLY,
                                             self.properties.oracle_aoi_poly_table_name,
                                             self.table_id,
                                             self.properties._oracle_table_schema_query_aoi_poly)

    def evaluate_suspiciousness_of_all_devices(self):
        suspicious_areas = self.oracle_tools.get_execute_oracle_procedure_result(self.properties.oracle_procedure_query_aoi,
                                                                    self.table_id,
                                                                    self.properties.oracle_aoi_procedure_table_name)

        is_suspicious = self.check_aois_intersection_with_polygons(suspicious_areas)
        self.evaluate_suspiciousness(is_suspicious)

        display('INFO:  Suspiciousness Evaluation Complete!')   
        return self.result_POLY

    def evaluate_analysis_confidence_for_all_devices(self):
        for device in range(self.geolocation_data_list.get_length()):
            self.geolocation_data_list[device].evaluate_confidence_in_analysis()
            self.aoi_confidence_analysis = pd.concat([self.aoi_confidence_analysis, 
                                                      self.geolocation_data_list[device].aoi_confidence_results], ignore_index=True)

        self.oracle_tools.drop_create_insert(self.aoi_confidence_analysis,
                                             self.properties.geo_confidence_analysis_table,
                                             self.table_id,
                                             self.properties._oracle_table_schema_query_geo_confidence_analysis)
        
        display('INFO:  Confidence Analysis Complete!')
        return self.aoi_confidence_analysis