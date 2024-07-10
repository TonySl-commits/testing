from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
import subprocess

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from time import time
import statistics

import warnings
warnings.filterwarnings('ignore')

# Read nodes from cassandra
##########################################################################################################

utils = CDR_Utils()
cassandra_tools = CassandraTools()
properties = CDR_Properties()

##########################################################################################################

server = '10.1.10.110'
bts_table = cassandra_tools.get_table(properties.bts_with_polygon,server=server)
bts_full_table = cassandra_tools.get_table(properties.bts_table_name,server=server)

bts_table = pd.merge(bts_table, bts_full_table, how='inner', on='cgi_id')
##########################################################################################################

time_estimate = []
max_retries = 4
retry_count = 0
x= 0
y =0
z =0
len_scanned = 0
df_list = []
session = cassandra_tools.get_cassandra_connection(server)
bts_data = bts_table.drop_duplicates(subset=['location_latitude', 'location_longitude', 'location_azimuth'])
bts_data = bts_data.tail(1001)
print(bts_data)
len_table = bts_data.shape[0]
remaining = len_table -10000
for _, row in bts_data.iterrows():
    t = time()
 
    if not row['polygon']:
        len_table -= 1
        x=x+1
        continue
    query = properties.polygon_activity_scan
    query = query.replace('table_name', str(properties.lebanon_nodes_table_name))
    query = query.replace('index', str(properties.lebanon_nodes_table_name + '_idx01'))
    query = query.replace('replace_latitude_longitude_list', str(", ".join(f"{x} {y}" for x, y in row['polygon'])))
    data = session.execute(query)
    nodes_scanned = pd.DataFrame(data.current_rows)
    if nodes_scanned.empty:
        len_table -= 1
        x=x+1
        continue
    n_len = len(nodes_scanned)
    nodes_scanned.rename(columns={'location_latitude': 'node_latitude', 'location_longitude': 'node_longitude'}, inplace=True)
    nodes_scanned['location_longitude'] = row['location_longitude']
    nodes_scanned['location_latitude'] = row['location_latitude']
    nodes_scanned['location_azimuth'] = row['location_azimuth']
    len_scanned = len_scanned + nodes_scanned.shape[0]
    bts_with_nodes = pd.merge(bts_table, nodes_scanned, how='inner', on=['location_longitude', 'location_latitude', 'location_azimuth'])    
    bts_with_nodes = bts_with_nodes[['cgi_id','osmid']]
    df_list.append(bts_with_nodes)

    if x == remaining and z==0:
        bts_with_nodes = pd.concat(df_list)
        bts_with_nodes.to_csv(properties.passed_filepath_bts + 'temp.csv', index=False)
        df_list = []
        columns = bts_with_nodes.columns
        
        query2 = """dsbulk load -url 'replace_path' -delim ',' -m "replace_columns1" -query "INSERT INTO datacrowd.table_name(replace_columns2) VALUES (replace_columns3)" """
        mapping1 = ", ".join([f"{i}={col}" for i, col in enumerate(columns)])
        mapping2 = ", ".join([f"{col}" for col in columns])
        mapping3 = ", ".join([f":{col}" for col in columns])    
        query_combo = query2.replace('table_name', properties.bts_nodes_table_name)
        query_combo = query_combo.replace('replace_path', properties.passed_filepath_bts + 'temp.csv')
        query_combo = query_combo.replace('replace_columns1', mapping1)
        query_combo= query_combo.replace('replace_columns2', mapping2)
        query_combo= query_combo.replace('replace_columns3', mapping3)
        print(f"★★★★★★★★★★★★★★★ ROWS TO INSERT: {len_scanned}★★★★★★★★★★★★★★★")
        len_scanned = 0
        subprocess.run(query_combo, shell=True)
        x=0
        z = 1

    elif x%1000 ==0:
        bts_with_nodes = pd.concat(df_list)
        bts_with_nodes.to_csv(properties.passed_filepath_bts + 'temp.csv', index=False)
        df_list = []
        columns = bts_with_nodes.columns
        
        query2 = """dsbulk load -url 'replace_path' -delim ',' -m "replace_columns1" -query "INSERT INTO datacrowd.table_name(replace_columns2) VALUES (replace_columns3)" """
        mapping1 = ", ".join([f"{i}={col}" for i, col in enumerate(columns)])
        mapping2 = ", ".join([f"{col}" for col in columns])
        mapping3 = ", ".join([f":{col}" for col in columns])    
        query_combo = query2.replace('table_name', properties.bts_nodes_table_name)
        query_combo = query_combo.replace('replace_path', properties.passed_filepath_bts + 'temp.csv')
        query_combo = query_combo.replace('replace_columns1', mapping1)
        query_combo= query_combo.replace('replace_columns2', mapping2)
        query_combo= query_combo.replace('replace_columns3', mapping3)
        print(f"★★★★★★★★★★★★★★★ ROWS TO INSERT: {len_scanned}★★★★★★★★★★★★★★★")
        len_scanned = 0
        subprocess.run(query_combo, shell=True)
    x=x+1
    process_time = time() - t
    
    time_estimate.append(process_time)
    
    avg_time = statistics.mean(time_estimate)
    
    time_estimated = round(((avg_time * len_table) / 60), 1)
    
    print("★★★★★★★★★★★★★★★ Remaining BTSs: {:>5} ★★★★★★★★★★★★★★★ Retrieved nodes: {:>4} ★★★★★★★★★★★★★★★ Estimated: {:>4} minutes ★★★★★★★★★★★★★★★".format(len_table,n_len,time_estimated))

    len_table -= 1
