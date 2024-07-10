import pandas as pd
from vcis.utils.utils import CDR_Utils,CDR_Properties
from vcis.gpx.gpx_preproccess_monitor import GPXPreprocessMonitor

utils = CDR_Utils()
properties = CDR_Properties()
gpx_monitor_preprocess = GPXPreprocessMonitor(cassandra_write = True)

data1 = pd.read_csv('/u01/jupyter-scripts/Riad/CDR_trace/data/insert/new_cvs1.csv')
print(data1.head(5))
mapping_1 = {
    'Device_ID_geo': 'device_id',
    'Latitude':'location_latitude',
    'Longitude':'location_longitude',
    'ms':'usage_timeframe',
}

data1.rename(columns=mapping_1, inplace=True)
data1.drop(columns=['Device_Name'],inplace=True)

device_id1 = data1['device_id'].head(1).values[0]


data1 = gpx_monitor_preprocess.process_gpx_folder(df_gpx = data1,device_id=device_id1)


# gpx_monitor_preprocess.write_into_cassandra_geo(data1)
gpx_monitor_preprocess.write_into_cassandra_geo(data1,cassandra_host='10.10.10.101')
