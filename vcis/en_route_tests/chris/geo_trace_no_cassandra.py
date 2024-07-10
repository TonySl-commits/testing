import pandas as pd
import time

import warnings
warnings.filterwarnings('ignore')

from IPython.display import display

import datetime
# from cdr_trace.geo_trace.geo_edge_trace.geo_trace import GeoTrace
from cdr_trace.geo_trace.geo_trace import GeoTrace
# from cdr_trace.geo_trace.geo_edge_trace.trace_tools import TraceTools
from cdr_trace.geo_trace.trace_tools import TraceTools

print('INFO:       Geo Trace Engine Process Started !!!')

geo_tracer = GeoTrace()
trace_tools = TraceTools()

data_directory = "C:/Users/mpedro/Desktop/CDR_Trace/data/dataframes/device_history/"

# df_history_marcelinho = pd.read_csv(data_directory + "device_history_marcelinho.csv")
df_history_pedro = pd.read_csv(data_directory + "device_history_pedro.csv")
# df_history_nathalie = pd.read_csv(data_directory + "device_history_nathalie.csv")
# df_history_zaher = pd.read_csv(data_directory + "device_history_zaher.csv")
# geo_id_from_fadi_device1 = pd.read_csv(data_directory + "device_history_from_fadi_device1.csv")
# geo_id_from_fadi_device2 = pd.read_csv(data_directory + "device_history_from_fadi_device2.csv")

df_history_pedro['service_provider_id'] = 'batenjen'
df_history_pedro['location_name'] = 'kebbe'

# # Filter date on 2024-01-13 only (from Timestamp)
# df_history_pedro['Date'] = pd.to_datetime(df_history_pedro['usage_timeframe'], unit='ms').dt.date
# filter_date = datetime.date(2024, 4, 17)
# df_history_pedro = df_history_pedro[df_history_pedro['Date'] == filter_date]

# # Filter device
# # df_history_nathalie = df_history_nathalie[df_history_nathalie['device_id'] == '4e79560f-e59a-4d7b-8b91-6dddbd571c57']

# # # Filter time
# df_history_pedro['usage_timeframe'] = pd.to_datetime(df_history_pedro['usage_timeframe'], unit='ms')
# start_time = datetime.datetime(2024, 4, 17, 15, 47, 8)
# end_time = datetime.datetime(2024, 4, 17, 17, 12, 23)
# df_history_pedro = df_history_pedro[(df_history_pedro['usage_timeframe'] >= start_time) & (df_history_pedro['usage_timeframe'] <= end_time)]

#################################################################################################################

start_time = time.time()

data = geo_tracer.trace(df_history_pedro, simulation_id=1, demo=True)

end_time = time.time()

print(f"Execution time: {round(end_time - start_time, 3) / 60} minutes.")

print('INFO:       Geo Trace Engine Process Successfully Completed!!!')


#################################################################################################################