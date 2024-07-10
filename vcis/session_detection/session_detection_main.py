import pandas as pd
from datetime import datetime
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
import numpy as np
import fastdtw
import time
import warnings
import math
import seaborn as sns
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')
from geopy.distance import geodesic, great_circle
from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.plots.cotraveler_plots import CotravelerPlots
import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.session_detection.session_detection_functions import SessionDetectionFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.plots.cotraveler_plots import CotravelerPlots

##########################################################################################################

class SessionDetectionMain():
    def __init__(self, verbose: bool = False):
        self.session_detection_functions = SessionDetectionFunctions()
        self.utils = CDR_Utils(verbose)
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools(verbose)
        self.cassandra_spark_tools = CassandraSparkTools()
        self.cotraveler_plots = CotravelerPlots(verbose)
        
        self.verbose = verbose

    def generate_overshoot_flags(self,imsi, server, start_date,end_date, fetch_size, radius,sectors_threshold, time_difference_threshold, time_window,new_active_location_threshold):
        
        session = self.cassandra_tools.get_cassandra_connection(server=server)
        start_date = self.utils.convert_datetime_to_ms_str(start_date)
        end_date = self.utils.convert_datetime_to_ms_str(end_date)
        
        df = self.cassandra_tools.get_device_history_imsi(imsi,start_date=start_date, end_date=end_date,session=session,default_fetch_size=fetch_size)
        
        df = self.session_detection_functions.get_visits_df_from_CDR_Trace(df)
       

        df = self.session_detection_functions.flag_overshoots_based_on_previous_sectors(df,sector_threshold=sectors_threshold,time_threshold=time_difference_threshold,radius=radius)

        df = self.session_detection_functions.generate_visited_flag(df,time_window=time_window)
        df = self.session_detection_functions.generate_duration_at_bts_flag(df, threshold_minutes=new_active_location_threshold)

        df["Overshoot"]= "undetermined"
        
        # Default to 'undetermined'
        df.loc[df['potential_overshoot'] == False, 'Overshoot'] = False  
        # If 'a' is False, set 'new_column' to False
        df.loc[df['potential_overshoot'] & (df['potential_session_end'] | df['switching_overshoot'] | (df['potential_newly_active_location'] ==False)), 'Overshoot'] = True
        df.loc[df['potential_overshoot'] & (df['potential_newly_active_location']), 'Overshoot'] = False  # If 'a' is True and 'd' is False, set 'Overshoot' to False
        return df
       
      
        # intermediary = df.drop_duplicates(subset=['cgi_id','location_latitude','location_longitude','location_azimuth'])
        
        # self.session_detection_functions.calculate_sectors_within_radius(intermediary,radius)
        
        # joined_df = pd.merge(df, intermediary , on="cgi_id")
        
        # joined_df.drop([col for col in joined_df.columns if '_y' in col], axis=1, inplace=True)
        # # Rename columns to remove suffixes
        
        # joined_df.columns = joined_df.columns.str.replace('_x', '')
        
        # joined_df.sort_values("usage_timeframe")
        
        # joined_df['OvershootFlag'] = self.session_detection_functions.flag_overshoots_based_on_previous_sectors(joined_df, radius, sectors_threshold, time_difference_threshold,num_sectors_column="number_of_sectors")

        # # time_spent_threshold = 30 #minutes

        # # Call the function
        # joined_df= self.session_detection_functions.generate_time_spent_flag(joined_df, time_spent_threshold)
        # joined_df = self.session_detection_functions.generate_visited_flag(joined_df)
        # return joined_df
