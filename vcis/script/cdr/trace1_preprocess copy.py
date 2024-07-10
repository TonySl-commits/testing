from vcis.cdr.format1.preprocess import CDR_Preprocess
from vcis.bts.integration import CDR_Integration
from vcis.utils.utils import CDR_Utils, CDR_Properties
import pandas as pd
import os
from vcis.databases.cassandra.cassandra_tools import CassandraTools
# from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

utils = CDR_Utils()
properties = CDR_Properties()
preprocess = CDR_Preprocess()
integration = CDR_Integration()
cassandra_tools = CassandraTools()
# cassandra_spark_tools = CassandraSparkTools()
pd.set_option('display.max_columns', None)
server='10.1.10.110'
service_provider_id = 10
offset = False
local = False

files_list = ['96170242890.xlsx']

phone_number = '96103000000'
imsi = '415000000000000' 
imei = '00000000000000'

for file in files_list:
    path = properties.passed_filepath_cdr_format1 + file
    
    data1,data2,data3 = preprocess.read_excel_files(path)

    bts_path = properties.passed_filepath_excel + 'bts_data.xlsx'
    bts_table = pd.read_excel(bts_path)
    # bts_table = cassandra_tools.get_bts_table(server=server)

    print(data1[[properties.cell_id_start_d1,properties.start_date_d123]].sort_values(properties.start_date_d123).head(1))
    print(data2[[properties.cell_id_d2,properties.start_date_d123]].sort_values(properties.start_date_d123).head(1))
    print(data3[[properties.cell_id_start_d3,properties.start_date_d123]].sort_values(properties.start_date_d123).head(1))
    

    phone_number = int(data2.dropna(subset=['MSISDN_NUMBER'])['MSISDN_NUMBER'].head(1).values[0])
    
    # phone_number = None
    if phone_number is None:
        phone_number = '96103000000'
        print("*"*30,"PHONE NUMBER MISSING! for " + file,"*"*30)

    data1[properties.phone_number] = phone_number
    data2[properties.phone_number] = phone_number
    data3[properties.phone_number] = phone_number
    
    data1 = preprocess.select_columns(data1,properties.data1_columns)
    data2 = preprocess.select_columns(data2,properties.data2_columns)
    data3 = preprocess.select_columns(data3,properties.data3_columns)

    data1 = preprocess.fix_dtypes_d1(data1)
    data2 = preprocess.fix_dtypes_d2(data2)
    data3 = preprocess.fix_dtypes_d3(data3)

    data2 = preprocess.add_milliseconds_to_date(data2,properties.start_date_d123,properties.start_millisecond_d23,offset)
    data3 = preprocess.add_milliseconds_to_date(data3,properties.start_date_d123,properties.start_millisecond_d23,offset)
    data3 = preprocess.add_milliseconds_to_date(data3,properties.end_date_d13,properties.end_millisecond_d3,offset)

    data1 = preprocess.combine_start_end_d1(data=data1,offset = offset)
    data2 = preprocess.rename_columns_d2(data2)
    data3 = preprocess.combine_start_end_d3(data3)
    
    data1 = preprocess.add_location_column(data1, bts_table)
    data2 = preprocess.add_location_column(data2, bts_table)
    data3 = preprocess.add_location_column(data3, bts_table)

    data = pd.concat([data1,data2,data3])

    if not data.dropna(subset=[properties.imsi]).empty:
        imsi = int(data.dropna(subset=[properties.imsi])[properties.imsi].value_counts().idxmax())
    else:
        imsi = '415000000000000'
        print("*"*30,"IMSI MISSING! for " + file,"*"*30)

    if not data.dropna(subset=[properties.imei]).empty:
        imei = int(data.dropna(subset=[properties.imei])[properties.imei].value_counts().idxmax())
        print(imei)
    else:
        imei = '00000000000000'
        print("*"*30,"IMEI MISSING! for " + file,"*"*30)
    phone_number = int(data.dropna(subset=[properties.phone_number])[properties.phone_number].value_counts().idxmax())

    # if not data.dropna(subset=[properties.phone_number]).empty:
    #     phone_number = int(data.dropna(subset=[properties.phone_number])[properties.phone_number].head(1).values[0])
    # else:
    #     phone_number = '96103000000'
    #     print("*"*30,"PHONE NUMBER MISSING! for " + file,"*"*30)

    data[properties.phone_number] = phone_number
    data[properties.imsi] = imsi
    data[properties.imei] = imei

    data[properties.phone_number] = data[properties.phone_number].astype(str)
    
    data = preprocess.select_columns(data,properties.trace_columns)

    data = preprocess.fix_milliseconds(data)
    data[properties.service_provider_id] = service_provider_id

    data = utils.convert_ms_to_datetime(data,properties.usage_timeframe)

    print(data.sort_values('usage_timeframe')['usage_timeframe'].head(1))
    print(data.columns)


    data[properties.imsi] = imsi
    data[properties.imei] = imei
    data[properties.phone_number] = phone_number
    print("IMSI ID:" , data[properties.imsi].unique())
    print("IMEI ID:" , data[properties.imei].unique())
    print("Phone Number:" , data[properties.phone_number].unique())

    # cassandra_spark_tools.insert_to_cassandra_using_spark(df=data,passed_table_name=properties.cdr_table_name,passed_connection_host='10.1.10.110')
    data.to_csv(properties.passed_filepath_excel + 'device_1.csv',index=False)