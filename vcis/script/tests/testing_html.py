import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from vcis.event_tracing.common_devices_functions import CommonDevicesFunctions
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.utils.utils import CDR_Utils , CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.event_tracing.common_devices_functions import CommonDevicesFunctions
import datetime

utils = CDR_Utils()
properties = CDR_Properties()
cassandra_tools = CassandraTools()
common_devices_functions = CommonDevicesFunctions()
max_time_gap = 10000
start_date = '2022-01-01'
end_date= datetime.datetime.now().strftime("%Y-%m-%d")
start_date = str(start_date)
end_date = str(end_date)
local = True
distance = 100
server = "10.1.10.110"
region=142
sub_region=145
start_date = utils.convert_datetime_to_ms_str(start_date)
end_date = utils.convert_datetime_to_ms_str(end_date)
session = cassandra_tools.get_cassandra_connection(server)
step_lat,step_lon = utils.get_step_size(distance)

df_history = pd.read_csv('df_history_913.csv')
df_history = utils.add_reverseGeocode_columns(df_history)
df_common = pd.read_csv('df_common_913.csv')
df_common = utils.add_reverseGeocode_columns(df_common)
groups = pd.read_csv('groups_913.csv')
black_listed_devices = ['ea830a7f-4e1c-42db-8d38-c1404f1b7aff']
df_blacklist_devices = cassandra_tools.get_device_history_geo_chunks(black_listed_devices,session = session, start_date = start_date, end_date = end_date,region=region,sub_region=sub_region)
devices_threat_table = common_devices_functions.get_threat_table_devices(df_history,groups,df_blacklist_devices, max_time_gap)
df2_exploded = devices_threat_table.explode('Contaced Black Listed Device(s)')

print(df_blacklist_devices)
group_id = 3
try:
    groups = groups.drop(columns=['Unnamed: 0'])
except:
    pass
try:
    df_history = df_history.drop(columns=['Unnamed: 0'])
except:
    pass
try:
    df_common = df_common.drop(columns=['Unnamed: 0'])
except:
    pass


group = groups[groups['group_id'] == group_id]
groups['group_ids'] = groups['group_ids'].apply(eval)
group_id_list = groups['group_ids'].values[group_id]

group_df = df_history[df_history['device_id'].isin(group_id_list)]
group_common_df = df_common[df_common['device_id'].isin(group_id_list)]

group_df['usage_timeframe'] = pd.to_datetime(group_df['usage_timeframe'], unit='ms')
device_id_mapping = {old_id: f"Device_{str(i+1).zfill(3)}" for i, old_id in enumerate(group_df['device_id'].unique())}
group_df['device_id_s'] = group_df['device_id'].map(device_id_mapping)
group_common_df['device_id_s'] = group_common_df['device_id'].map(device_id_mapping)
devices_threat_table['device_id_s'] = devices_threat_table['device_id'].map(device_id_mapping)

num_unique_devices = group_df['device_id_s'].nunique()
filtered_df_common = df_common[df_common['group_id'] == group_id]
top_3_cities = filtered_df_common['city'].value_counts().head(3)
most_common_place = top_3_cities.index.tolist()
most_visited_place = group_df['usage_timeframe'].dt.hour.mode()[0]
number_of_visited_places = group_df['usage_timeframe'].dt.day_name().mode()[0]


# df1_exploded = group_df.explode('group_ids').rename(columns={'group_ids': 'device_id'})
# merged_df = df1_exploded.merge(df2_exploded, on='device_id', how='left')
# group_contact_blacklist = merged_df.groupby('group_id')['Contaced Black Listed Device(s)'].apply(lambda x: x.notna().any()).reset_index()
# group_contact_blacklist.rename(columns={'Contaced Black Listed Device(s)': 'contacted_blacklisted'}, inplace=True)
# group_contact_blacklist['contacted_blacklisted'] = group_contact_blacklist['contacted_blacklisted'].map({True: 'Yes', False: 'No'})

# print(group_contact_blacklist)

filtered_devices_threat_table = devices_threat_table[devices_threat_table['device_id'].isin(group_id_list)]

df_devices_common = {}
df_devices_history = {}

for id in group_df['device_id'].unique():
    df_device_common = df_common[df_common['device_id'] == id]
    df_device_history = df_history[df_history['device_id'] == id]
    if len(df_device_common) <= 1:
        print(id,"skipped since it has only one hit")
    else:
        df_devices_common[id] = df_device_common
        df_devices_history[id] = df_device_history


fig = common_devices_functions.hit_distribution(*df_devices_common.values())

bar_fig = common_devices_functions.hit_bar(*df_devices_history.values())

map_fig = px.scatter_mapbox(
    group_df,
    lat="location_latitude",
    lon="location_longitude",
    color="device_id_s",
    size_max=15,
    zoom=1,
    mapbox_style="open-street-map",
    title="Device Locations"
)

filtered_devices_threat_table = filtered_devices_threat_table.drop(columns=['Lifespan Info','device_id'])
filtered_devices_threat_table = filtered_devices_threat_table.rename(columns={'device_id_s': 'Device ID'})
filtered_devices_threat_table = filtered_devices_threat_table[['Device ID','Sus Areas Visited & Time','Contaced Black Listed Device(s)','First & Last Activity']]
filtered_devices_threat_table['Sus Areas Visited & Time'] = filtered_devices_threat_table['Sus Areas Visited & Time'].apply(lambda x: 'No Suspicious areas' if not x else x)
filtered_devices_threat_table['Contaced Black Listed Device(s)'] = filtered_devices_threat_table['Contaced Black Listed Device(s)'].fillna('No blacklisted devices in contact')

# Generate HTML table from filtered_devices_threat_table
table_html = filtered_devices_threat_table.to_html(index=False, classes='table table-striped', border=0)

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Interactive Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Nunito:ital,wght@0,200;0,300;0,400;0,600;0,700;0,800;0,900;1,200;1,300;1,400;1,600;1,700;1,800&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Nunito', sans-serif;
            color: #4267B2;
            background-color: #FFFFFF;
        }}
        .dashboard {{
            background-color: #FFFFFF;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }}
        .stat-box {{
            background-color: #F0F2F5;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 10px;
            color: #4267B2;
            border: 1px solid #4267B2;
        }}
        .table-container {{
            background-color: #F0F2F5;
            padding: 20px;
            border-radius: 10px;
            margin-top: 20px;
            color: #4267B2;
            border: 1px solid #4267B2;
            max-height: 400px;
            overflow-y: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #4267B2;
        }}
        th {{
            background-color: #F0F2F5 !important; 
            color: #4267B2;
            position: sticky;
            top: 0;
            z-index: 1;
        }}
        body::before {{
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: #FFFFFF;
            z-index: -1;
        }}
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div class="container">
        <div class="dashboard">
            <h1 class="display-4 text-center mb-4" style="color: #4267B2;">Case Insights for Group {group_id}</h1>
            <div class="row">
                <div class="col-md-3">
                    <div class="stat-box">
                        <h5>Number of Unique Devices</h5>
                        <p>{num_unique_devices}</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <h5>Most Common Place</h5>
                        <p>{most_common_place}</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <h5>Most Visited Place</h5>
                        <p>{most_visited_place}</p>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <h5>Number of Visited Places</h5>
                        <p>{number_of_visited_places}</p>
                    </div>
                </div>
            </div>
            <div class="row mt-4">
                <div class="col-md-12">
                    {fig.to_html(full_html=False, include_plotlyjs='cdn')}
                </div>
            </div>
            <div class="row mt-4">
                <div class="col-md-8">
                    {map_fig.to_html(full_html=False, include_plotlyjs='cdn')}
                </div>
                <div class="col-md-4">
                    {bar_fig.to_html(full_html=False, include_plotlyjs='cdn')}
                </div>
            </div>
            <div class="table-container">
                {table_html}
            </div>
        </div>
    </div>
</body>
</html>
"""

with open('interactive_dashboard.html', 'w') as file:
    file.write(html_content)
print("HTML content saved to interactive_dashboard.html")