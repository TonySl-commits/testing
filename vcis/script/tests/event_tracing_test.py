import pandas as pd
from vcis.event_tracing.event_tracing_main import EventTracingMain
from vcis.utils.utils import CDR_Properties,CDR_Utils
from vcis.databases.cassandra.cassandra_tools import CassandraTools
import warnings
import pickle 
warnings.filterwarnings("ignore")

event_tracing_main = EventTracingMain(verbose=True)
cassandra_tools = CassandraTools(verbose=True)
properties = CDR_Properties()
utils = CDR_Utils(verbose=True)

df_history = pd.read_csv(properties.passed_filepath_excel + 'df_history.csv')
distance = 100
server = '10.1.2.205'
region = '142'
sub_region = '145'
scenario_id = 182370
start_date = '2023-02-01'
end_date = '2024-02-29'

start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)

session = cassandra_tools.get_cassandra_connection(server)
# df_history = cassandra_tools.get_scenario_data(scenario_id=scenario_id,start_date=start_date,end_date=end_date,region=region,sub_region=sub_region,session=session)
# common_df_dict = event_tracing_main.event_tracing(df_history,distance,session,region,sub_region)

# print(common_df_dict)

# # save common df dictionary to pickle file
# with open('common_df_dict.pickle', 'wb') as file:
#     pickle.dump(common_df_dict, file)

# load pickle file
with open('common_df_dict.pickle', 'rb') as file:
    common_df_dict = pickle.load(file)

import plotly.express as px

# Filter df_history based on device_ids from common_df_dict and generate bar charts for distribution of hits per weekdays
device_ids = df_history['device_id'].unique()
fig_list = []

# Define the order of weekdays from Monday to Sunday
weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

for device_id in device_ids:
    if device_id in common_df_dict:
        df_device_history = df_history[df_history['device_id'] == device_id]
        df_device_common = common_df_dict[device_id]
        # Convert 'usage_timeframe' to datetime and extract the weekday for both history and common dataframes
        df_device_history['weekday'] = pd.to_datetime(df_device_history['usage_timeframe'], unit='ms').dt.day_name()
        df_device_common['weekday'] = pd.to_datetime(df_device_common['usage_timeframe'], unit='ms').dt.day_name()
        
        # Count weekdays for both history and common dataframes
        weekday_counts_history = df_device_history['weekday'].value_counts().reindex(weekdays_order).reset_index()
        weekday_counts_common = df_device_common['weekday'].value_counts().reindex(weekdays_order).reset_index()
        
        weekday_counts_history.columns = ['Weekday', 'Hits History']
        weekday_counts_common.columns = ['Weekday', 'Hits Common']
        
        # Merge both dataframes to have a single dataframe for plotting
        merged_weekday_counts = pd.merge(weekday_counts_history, weekday_counts_common, on='Weekday')
        
        # Use device_id as color for the bar plot
        fig = px.bar(merged_weekday_counts, x='Weekday', 
                     y=['Hits History', 'Hits Common'], barmode='group',
                     title=f'Hits per Weekday for Device ID {device_id}', 
                     labels={'value': 'Hits', 'variable': 'Data Type'})
        fig_list.append(fig)

# Display all the figures
for fig in fig_list:
    fig.show()

