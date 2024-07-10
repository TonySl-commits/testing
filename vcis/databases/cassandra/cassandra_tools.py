##########################################################################################################################################################################################
    # Imports
##############################################################################################################################
from cassandra.cluster import Cluster
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone


from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.oracle.oracle_tools import OracleTools

class CassandraTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.utils = CDR_Utils(verbose=verbose)
        self.oracle_tools = OracleTools(verbose=verbose)
        self.verbose = verbose
##############################################################################################################################
    # Cassandra Connection
##############################################################################################################################
    def get_cassandra_connection(self, server: str='10.1.10.110',
                             default_timeout: int=10000,
                             default_fetch_size: int=100000):
        cluster = Cluster([server])
        try:
            session = cluster.connect('datacrowd')
        except Exception as e:
            if self.verbose:
                print("Error in get_cassandra_connection:", e)
            raise ConnectionError(f"ERROR: Server {server} is not reachable. Details: {str(e)}")
        session.default_timeout = default_timeout
        session.default_fetch_size = default_fetch_size
        return session
    
##############################################################################################################################
    # Fucntion to inset data into cassandra table
##############################################################################################################################
    def insert_into_cassandra(self,
                              passed_dataframe: pd.DataFrame = None, # type: ignore
                              passed_key_space: str = 'datacrowd',
                              passed_table_name: str = 'loc_location_cdr_main_new',session=None):

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping_2.items():
            passed_dataframe[column_name] = passed_dataframe[column_name].astype(column_type) # type: ignore


        for _, row in passed_dataframe.iterrows():
            insert_statement = f"""
                INSERT INTO {passed_key_space + '.' + passed_table_name}
                (imsi_id, usage_timeframe, cgi_id, data_source_id, imei_id, 
                location_azimuth, location_latitude, location_longitude, 
                phone_number, service_provider_id, type_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            try:
                session.execute(insert_statement, (
                    str(row["imsi_id"]), row["usage_timeframe"], row["cgi_id"], 
                    row["data_source_id"], row["imei_id"], row["location_azimuth"], 
                    row["location_latitude"], row["location_longitude"], 
                    str(row["phone_number"]), row["service_provider_id"], row["type_id"],row['date_type']
                ))
            except Exception as e:
                if self.verbose:
                    print("Error in insert_into_cassandra:", e)
                continue

##############################################################################################################################
    # Scanning Functions
##############################################################################################################################
    def get_device_history_geo(self,device_id:str,
                               start_date:int ,
                               end_date:int ,
                               region:int =142,
                               sub_region:int = 145,
                               return_all_att = False,
                               session=None):
        
        df_device_combo_list = []
        query = self.properties.device_history_query
        query = query.replace('geo_id', str(device_id))
        query = query.replace('value1', str(start_date))
        query = query.replace('value2', str(end_date))
        query = query.replace('region', str(region))
        query = query.replace('subre', str(sub_region))

        year_month_combo = self.utils.get_month_year_combinations(start_date, end_date)

        for combo in year_month_combo:
            year = str(combo[0])
            month = str(combo[1])
            query_combo = query.replace('year', year)
            query_combo = query_combo.replace('month', month)
            max_retries = 4
            retry_count = 0
            while retry_count < max_retries:
                try:
                    data = session.execute(query_combo)
                    # Create DataFrame from queried data
                    df_device_combo = pd.DataFrame(data.current_rows)
                    text = str("Retrieved {:>4} rows at {:>4}-{:>2}").format(len(df_device_combo),year,month).center(50)

                    print(f"★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★  {text} ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★")

                    df_device_combo_list.append(df_device_combo)
                    break 
                except Exception as e:
                    if self.verbose:
                        print(f"Error in get_device_history_geo: {e}")
                    retry_count += 1
                    continue
                
        try:        
            df_device = pd.concat(df_device_combo_list)
            if return_all_att:
                return df_device
            df_device = df_device[[ 'device_id', 'location_latitude', 'location_longitude', 'usage_timeframe' ]].drop_duplicates()
        except Exception as e:
            df_device = pd.DataFrame()
            if self.verbose:
                print(f"Error in get_device_history_geo: {e}")
        return df_device
    
    def separate_list(self, lst, num_lists):
        sublist_size = len(lst) // num_lists
        remainder = len(lst) % num_lists
        sublists = []
        index = 0
        for i in range(num_lists):
            sublist = lst[index:index + sublist_size]
            if remainder > 0:
                sublist.append(lst[-remainder])
                remainder -= 1
            sublists.append(sublist)
            index += sublist_size
        return sublists
    
    def get_device_history_geo_chunk(self,device_id_list:list,
                                    start_date:int ,
                                    end_date:int ,
                                    region:int =142,
                                    sub_region:int = 145,
                                    session=None):

        df_device_combo_list = []
        query = self.properties.device_history_list_query
        query = query.replace('replace_devices_list', str(', '.join([f'"{device_id}"' for device_id in device_id_list])))
        query = query.replace('value1', str(start_date))
        query = query.replace('value2', str(end_date))
        query = query.replace('region', str(region))
        query = query.replace('subre', str(sub_region))
        
        year_month_combo = self.utils.get_month_year_combinations(start_date, end_date)
        for combo in year_month_combo:
            year = str(combo[0])
            month = str(combo[1])
            query_combo = query.replace('year', year)
            query_combo = query_combo.replace('month', month)
            max_retries = 1
            retry_count = 0
            while retry_count < max_retries:
                try:
                    data = session.execute(query_combo)

                    df_device_combo = pd.DataFrame(data.current_rows)
                    if self.verbose:
                        print(f"Retrieved {len(df_device_combo)} rows for {year}-{month}")
                    df_device_combo_list.append(df_device_combo)
                    break  
                except Exception as e:
                    if self.verbose:
                        print(f"Error executing query: {e}")
                    retry_count += 1
                    continue
        try:
            df_device = pd.concat(df_device_combo_list)
            df_device = df_device[['device_id', 'location_latitude', 'location_longitude', 'usage_timeframe', 'location_name', 'service_provider_id']].drop_duplicates()
        except Exception as e:
            if self.verbose:
                print("Error in get_device_history_geo_chunk: ", e)
            df_device = pd.DataFrame()
        return df_device
    
    def get_device_history_geo_chunks(self, device_list_separated : list, start_date : int, end_date : int ,region:int=142 ,sub_region:int=145 ,session=None):

        if len(device_list_separated) >25:
            n = int(np.ceil(len(device_list_separated)/25))
            device_list_separated = self.separate_list(device_list_separated,n)
            df_history_list = []
            for list_device in device_list_separated:
                df_history_device = self.get_device_history_geo_chunk(device_id_list = list_device, start_date= start_date, end_date=end_date,region=region,sub_region=sub_region,session=session) 
                df_history_list.append(df_history_device)
            device_history= pd.concat(df_history_list)
        else:
            df_history_device = self.get_device_history_geo_chunk(device_id_list = device_list_separated, start_date= start_date, end_date=end_date,region=region,sub_region=sub_region,session=session) 
            return df_history_device
        return device_history

    def get_device_history_imsi(self,
    
                             imsi_id:str,
                             start_date:str ,
                             end_date:str ,
                             server:str = '10.1.10.66',
                             default_timeout:int = 10000,
                             default_fetch_size:int = 50000,
                             session=None):

        try:
            query = f"""SELECT *  FROM  datacrowd.loc_location_cdr_main_new
                    WHERE imsi_id= '{imsi_id}' and
                    usage_timeframe >{start_date} and usage_timeframe < {end_date}  allow filtering"""
            data = session.execute(query)
        except Exception as e:
            if self.verbose:
                print("Error in get_device_history_imsi:", e)
            query = f"""SELECT *  FROM  datacrowd.loc_location_cdr_main_new
                    WHERE imsi_id= '{imsi_id}' and
                    usage_timeframe >{start_date} and usage_timeframe < {end_date} limit {default_fetch_size} allow filtering """
         
            data = session.execute(query)
        df_device = pd.DataFrame(data.current_rows)
        return df_device
    
    
    def get_lebanon_nodes(self,
                        server:str = '10.1.10.66',
                        default_timeout:int = 10000,
                        default_fetch_size:int = 50000,
                        session=None):
        
        query = f"""SELECT *  FROM  datacrowd.{self.properties.lebanon_nodes_table}"""
        data_nodes = session.execute(query)
        df_nodes =pd.DataFrame(data_nodes.current_rows)
        return df_nodes
    
    def get_bts_nodes(self,
                        server:str = '10.1.10.66',
                        default_timeout:int = 10000,
                        default_fetch_size:int = 50000,session=None):

        query = f"""SELECT *  FROM  datacrowd.{self.properties.bts_nodes_table_name}"""
        data_bts_nodes = session.execute(query)
        df_bts_nodes = pd.DataFrame(data_bts_nodes.current_rows)
        return df_bts_nodes
    
    def device_scan_month_year_query_builder(self, start_date, end_date, year, month, scan_distance, latitude, longitude,region,subre):
        query = self.properties.device_scan_query

        year = str(year)
        month = str(month)

        lat = str(latitude)
        longitude = str(longitude)

        start_date = str(start_date)
        end_date = str(end_date)

        scan_distance = str(scan_distance)
        region = str(region)
        subre = str(subre)
        query = query.replace('year', year) \
            .replace('month', month) \
            .replace('value1', lat) \
            .replace('value2', longitude) \
            .replace('value3', start_date) \
            .replace('value4', end_date) \
            .replace('scan_distance', scan_distance) \
            .replace('region', region) \
            .replace('subre', subre)

        return query    
    def get_bts_table(self,
                session =None,
                default_timeout:int = 10000,
                default_fetch_size:int = 100000):

        query = f"""SELECT *  FROM  datacrowd.{self.properties.bts_table}"""
        data_bts = session.execute(query)
        df_bts = pd.DataFrame(data_bts.current_rows)    
        return df_bts
    
    def get_table(self,table_name,
                session=None,
                default_timeout:int = 10000,
                default_fetch_size:int = 50000):

        query = f"""SELECT *  FROM  datacrowd.{table_name}"""
        data_bts = session.execute(query)
        df_bts = pd.DataFrame(data_bts.current_rows)    
        return df_bts
    
    def get_device_list_from_device_scan(self, start_date, end_date, latitude, longitude, scan_distance : int = 200, passed_server : str = '10.1.10.110', default_timeout : int = 10000, default_fetch_size : int = 10000,region:int=142,subre:int=145,session=None):
        
        device_list = []
        df_list = []
        
        year_month_combinations  = self.utils.get_month_year_combinations(start_date, end_date)
        # data = self.utils.convert_datetime_to_ms(data,'start_time')
        # data = self.utils.convert_datetime_to_ms(data,'end_time')

        for year, month in year_month_combinations:
            df = pd.DataFrame()
            query = self.device_scan_month_year_query_builder(start_date = start_date,
                                                end_date = end_date,
                                                year = year,
                                                month = month,
                                                scan_distance = scan_distance,
                                                latitude = latitude,
                                                longitude = longitude,
                                                region=region,
                                                subre=subre)

            try:
                df = session.execute(query)
                df = pd.DataFrame(df.current_rows)

            except Exception as e:
                if self.verbose:
                    print("Error in get_device_list_from_device_scan:", e)

            if len(df) != 0:
                df = df[['location_latitude', 'location_longitude', 'device_id', 'usage_timeframe','location_name','service_provider_id']].drop_duplicates()
                print(f"Data Retrieved {len(df)}")
                df_list.append(df)

        common_df = pd.concat(df_list)
        device_list = common_df['device_id'].unique().tolist()

        return common_df, device_list
    def get_device_dhtp(self,
                        latitude:float = None,
                        longitude:float = None,
                        start_date:float = None,
                        end_date:float = None,
                        scan_distance:int = None,
                        server:str = '10.1.2.205'):
        
        df_can, device_list = self.get_device_list_from_device_scan(latitude = latitude,longitude= longitude , start_date=start_date , end_date= end_date ,passed_server = server,scan_distance= scan_distance)
        

        df_dhtp = self.get_device_history_geo_chunk(device_id_list = device_list ,start_date=start_date , end_date=end_date,server=server)

        return df_dhtp
    
    def get_scenario_data(self,scenario_id=None,start_date=None,end_date=None,region=None,sub_region=None,session=None,server= '10.1.10.110'):
        if session == None:
            self.get_cassandra_connection(server =server )
        table = self.oracle_tools.get_table("SSDX_ENG","TMP_REPORT_COORDINATES_6",scenario_id,True)
        print(table)
        device_id_list = table['DEVICE_ID'].unique().tolist()
        df_history = self.get_device_history_geo_chunks(device_list_separated=device_id_list,
                                                                        start_date=start_date,
                                                                        end_date=end_date,
                                                                        region=region,
                                                                        sub_region=sub_region,
                                                                        session=session)
        return df_history




# def get_bts_node_neighbors(self,
#                         server:str = '10.1.10.110',
#                         default_timeout:int = 10000,
#                         default_fetch_size:int = 50000,session=None):

#         query = f"""SELECT *  FROM  datacrowd.loc_location_bts_neighbors"""
#         data_bts_nodes = session.execute(query)
#         df_bts_nodes_neighbors = pd.DataFrame(data_bts_nodes.current_rows)
#         return df_bts_nodes_neighbors
