from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

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
cassandra_spark_tools = CassandraSparkTools()
spark = cassandra_spark_tools.get_spark_connection() 
properties = CDR_Properties()

##########################################################################################################

server = '10.1.10.66'
bts_table = cassandra_spark_tools.get_spark_data(properties.cdr_bts_polygon_vertices,local=False,passed_connection_host=server)
bts_table = bts_table.toPandas()
bts_data = bts_table.drop_duplicates(subset=['location_latitude', 'location_longitude', 'location_azimuth'])

##########################################################################################################
x = bts_data.shape[0]
time_estimate = []
max_retries = 4
retry_count = 0

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

cluster = Cluster([server], auth_provider=auth_provider)
session = cluster.connect('datacrowd')
session.default_timeout = 10000
session.default_fetch_size = 150000

bts_data['vertex1'] = bts_data['vertex1'].apply(lambda x: x.replace('(','').replace(')','').split(','))
bts_data['vertex2'] = bts_data['vertex2'].apply(lambda x: x.replace('(','').replace(')','').split(','))
bts_data['vertex3'] = bts_data['vertex3'].apply(lambda x: x.replace('(','').replace(')','').split(','))
bts_data['vertex4'] = bts_data['vertex4'].apply(lambda x: x.replace('(','').replace(')','').split(','))

for _, row in bts_data.iterrows():
    t = time()
    if row['vertex1'] == ['nan'] or row['vertex2'] == ['nan'] or row['vertex3'] == ['nan'] or row['vertex4'] == ['nan']:
        continue
    query = properties.polygon_activity_scan
    query = query.replace('table_name', str('lebanon_nodes'))
    
    query = query.replace('lat1', str(row[properties.location_latitude]))
    query = query.replace('lon1', str(row[properties.location_longitude]))

    query = query.replace('lat2', str(row['vertex1'][0]))
    query = query.replace('lon2', str(row['vertex1'][1]))

    query = query.replace('lat3', str(row['vertex2'][0]))
    query = query.replace('lon3', str(row['vertex2'][1]))

    query = query.replace('lat4', str(row['vertex3'][0]))
    query = query.replace('lon4', str(row['vertex3'][1]))

    query = query.replace('lat5', str(row['vertex4'][0]))
    query = query.replace('lon5', str(row['vertex4'][1]))

    query = query.replace('lat6', str(row[properties.location_latitude]))
    query = query.replace('lon6', str( row[properties.location_longitude]))

    data = session.execute(query)
    nodes_scanned = pd.DataFrame(data.current_rows)
    if nodes_scanned.empty:
        continue
    n_len = len(nodes_scanned)
    nodes_scanned.rename(columns={'location_latitude': 'node_latitude', 'location_longitude': 'node_longitude'}, inplace=True)
    
    nodes_scanned['location_longitude'] = row['location_longitude']
    nodes_scanned['location_latitude'] = row['location_latitude']
    nodes_scanned['location_azimuth'] = row['location_azimuth']
    
    bts_with_nodes = pd.merge(bts_table, nodes_scanned, how='inner', on=['location_longitude', 'location_latitude', 'location_azimuth'])    
    bts_with_nodes = bts_with_nodes[['cgi_id','osmid']]
    bts_with_nodes = spark.createDataFrame(bts_with_nodes)

    cassandra_spark_tools.insert_to_cassandra_using_spark(df = bts_with_nodes, passed_table_name='bts_node_projection')
    
    process_time = time() - t
    
    time_estimate.append(process_time)
    
    avg_time = statistics.mean(time_estimate)
    
    time_estimated = round(((avg_time * x) / 60), 1)
    
    print("★★★★★★★★★★★★★★★ Remaining BTSs: {:>5} ★★★★★★★★★★★★★★★ Retrieved nodes: {:>4} ★★★★★★★★★★★★★★★ Estimated: {:>4} minutes ★★★★★★★★★★★★★★★".format(x,n_len,time_estimated))

    x -= 1
