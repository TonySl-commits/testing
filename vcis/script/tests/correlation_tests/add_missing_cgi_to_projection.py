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
print(bts_table.shape)
bts_full_table = cassandra_tools.get_table(properties.bts_table_name,server=server)
print(bts_full_table.shape)
bts_table = pd.merge(bts_table, bts_full_table, how='inner', on='cgi_id')

bts_table = bts_table[['cgi_id']]
non_missing = pd.read_csv(properties.passed_filepath_bts + 'non_missing_cgi_id.csv')
print(non_missing.columns)
print(bts_table)
non_missing = non_missing.rename(columns={0:'cgi_id'})
uncommon_rows = pd.concat([bts_table, non_missing]).drop_duplicates(keep=False)

print(uncommon_rows)