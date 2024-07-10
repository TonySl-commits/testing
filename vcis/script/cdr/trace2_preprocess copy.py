from vcis.cdr.format2.preprocess import CDR_Preprocess
from vcis.bts.integration import CDR_Integration
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
import os
import pandas as pd


utils = CDR_Utils()
properties = CDR_Properties()
preprocess = CDR_Preprocess()
integration = CDR_Integration()
cassandra_tools = CassandraTools()
cassandra_spark_tools = CassandraSparkTools()

pd.set_option('display.max_columns', None)
server='10.1.10.110'

service_provider_id = 10
offset = False
local = False

passed_filepath_cdr_format2_device = properties.passed_filepath_cdr_format2 + '333'
all_files = []

try:
    files = os.listdir(passed_filepath_cdr_format2_device)
except:
    pass

try:
    for file in files:
        if file.endswith(".csv"):
            all_files.append(os.path.join(passed_filepath_cdr_format2_device, file))
except:
    pass


phone_number = '96103000000'
imsi = '415000000000000' 
imei = '00000000000000'

df = utils.read_cdr_records(all_files)
df = preprocess.select_columns(df)

if not df.dropna(subset=[properties.imsi]).empty:
    imsi = int(df.dropna(subset=[properties.imsi])[properties.imsi].value_counts().idxmax())
else:
    imsi = '415000000000000'
    print("*"*30,"IMSI MISSING! for " + file,"*"*30)

if not df.dropna(subset=[properties.imei]).empty:
    imei = int(df.dropna(subset=[properties.imei])[properties.imei].value_counts().idxmax())
    print(imei)
else:
    imei = '00000000000000'
    print("*"*30,"IMEI MISSING! for " + file,"*"*30)

if not df.dropna(subset=[properties.phone_number]).empty:
    phone_number = int(df.dropna(subset=[properties.phone_number])[properties.phone_number].value_counts().idxmax())
else:
    phone_number = '96103000000'
    print("*"*30,"PHONE NUMBER MISSING! for " + file,"*"*22)

df[properties.imsi] = imsi
df[properties.imei] = imei
df[properties.phone_number] = phone_number

df = df.sort_values(properties.start_timestamp)
df = preprocess.join_start_end(df)

# bts_path = properties.passed_filepath_excel + 'bts_data.csv'
# bts_table = pd.read_csv(bts_path)
bts_table = cassandra_tools.get_bts_table(server=server)

df = preprocess.add_location_column(df, bts_table)
df = utils.convert_datetime_to_ms(df=df,date_column_name= properties.usage_timeframe,offset=offset,format='%Y-%m-%d %H:%M:%S')
df = preprocess.add_millisecond_to_date(df)

print("IMSI ID:" , df[properties.imsi].unique())
print("IMEI ID:" , df[properties.imei].unique())
print("Phone Number:" , df[properties.phone_number].unique())

df[properties.service_provider_id] = service_provider_id
df.drop(properties.bts_cell_name, axis=1, inplace=True)
df.to_csv(properties.passed_filepath_excel + 'device_2.csv',index=False)

# cassandra_spark_tools.insert_to_cassandra_using_spark(df=df,passed_table_name=properties.cdr_table_name,passed_connection_host=server,local=local)
