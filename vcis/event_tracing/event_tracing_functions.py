from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
import pandas

class EventTracingFunctions:
    def __init__(self,verbose=False):
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools()
        self.verbose = verbose

    def filter_common_df(self, full_common_df, df_history, distance):
        step_lat, step_lon = self.utils.get_step_size(distance)
        common_df_dict = {}
        for device_id in df_history['device_id'].unique():
            df_device_common = full_common_df[full_common_df['device_id'] == device_id]
            df_device_history = df_history[df_history['device_id'] == device_id]
            df_visit_history = self.utils.get_visit_df(df=df_device_history, step_lat=step_lat, step_lon=step_lon)
            mask = df_device_common['usage_timeframe'].apply(
                lambda x: any((df_visit_history['start_time'] <= x) & (x <= df_visit_history['end_time']))
            )
            df_device_common = df_device_common[mask]
            df_device_common = df_device_common.reset_index(drop=True)
            common_df_dict[device_id] = df_device_common
                
        return common_df_dict

