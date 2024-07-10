from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from pyspark.sql.functions import col
from vcis.utils.utils import CDR_Utils,CDR_Properties

import pandas as pd

cassandra_spark_tools = CassandraSparkTools()
properties = CDR_Properties()
utils = CDR_Utils()

table_name = 'loc_location_cdr_main_new'
server = '10.10.10.60'
df = cassandra_spark_tools.get_spark_data(passed_table_name=table_name,passed_connection_host=server,local=False)

df_filtered = df.filter(df.imsi_id == '155007038832890')
df_filtered = df_filtered.toPandas()

print(df_filtered)

# df = cassandra_spark_tools.get_spark_data('loc_location_cdr_main_new',passed_connection_host='10.1.10.66')
# # df.show(5)
# df_filtered = df.filter(df.imsi_id == '415634192479912')
# df_filtered.show(5)