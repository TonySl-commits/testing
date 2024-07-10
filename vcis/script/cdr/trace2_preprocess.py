import streamlit as st
import pandas as pd

from vcis.cdr.format2.preprocess import CDR_Preprocess
from vcis.bts.integration import CDR_Integration
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools
# from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

utils = CDR_Utils()
properties = CDR_Properties()
preprocess = CDR_Preprocess()
integration = CDR_Integration()
cassandra_tools = CassandraTools()
# cassandra_tools = CassandraSparkTools()

pd.set_option('display.max_columns', None)

service_provider_id = 10

local = False

phone_number = '96103000000'
imsi = '415000000000000' 
imei = '00000000000000'

def process_data(df,progress_bar,step_progress,offset,session):

    progress = 0
    if not df.dropna(subset=[properties.imsi]).empty:
        imsi = str(int(df.dropna(subset=[properties.imsi])[properties.imsi].value_counts().idxmax()))
    else:
        imsi = '415000000000000'
        print("*"*30,"IMSI MISSING! ","*"*30)

    if not df.dropna(subset=[properties.imei]).empty:
        imei = str(int(df.dropna(subset=[properties.imei])[properties.imei].value_counts().idxmax()))
        print(imei)
    else:
        imei = '00000000000000'
        print("*"*30,"IMEI MISSING! ","*"*30)

    if not df.dropna(subset=[properties.phone_number]).empty:
        phone_number = str(df.dropna(subset=[properties.phone_number])[properties.phone_number].value_counts().idxmax())
    else:
        phone_number = '96103000000'
        print("*"*30,"PHONE NUMBER MISSING! ","*"*22)

    progress = progress + step_progress
    progress_bar.progress(progress)
    if not phone_number.startswith('961'):
        phone_number = '961' + phone_number
    df[properties.imsi] = imsi
    df[properties.imei] = imei
    df[properties.phone_number] = phone_number
    df = df.sort_values(properties.start_timestamp)
    df = preprocess.join_start_end(df)
    progress = progress + step_progress
    progress_bar.progress(progress)
    try:
        bts_table = cassandra_tools.get_bts_table(session=session)
    except:
        bts_table = pd.read_csv(properties.passed_filepath_excel + "bts_table.csv")
        bts_table.columns = bts_table.columns.str.lower()
    progress = progress + step_progress

    progress_bar.progress(progress)

    df = preprocess.add_location_column(df, bts_table)
    df = utils.convert_datetime_to_ms(df=df,date_column_name= properties.usage_timeframe,offset=offset,format='%Y-%m-%d %H:%M:%S')
    df = preprocess.add_millisecond_to_date(df)
    df = preprocess.fix_datatype_column(df)
    progress = progress + step_progress
    progress_bar.progress(progress)

    st.write(df[properties.data_type].unique())
    print("IMSI ID:" , df[properties.imsi].unique())
    print("IMEI ID:" , df[properties.imei].unique())
    print("Phone Number:" , df[properties.phone_number].unique())
    df[properties.service_provider_id] = service_provider_id
    df.drop(properties.bts_cell_name, axis=1, inplace=True)
    return df

def insert_data(df,session):
    df['type_id'] = 1
    service_provider_id = 10
    df['location_latitude'] = df['location_latitude'].astype(float)
    df['location_longitude'] = df['location_longitude'].astype(float)
    df['usage_timeframe'] = df['usage_timeframe'].astype('int64')
    df['location_azimuth'] = df['location_azimuth'].astype(str)
    df = df.sort_values('usage_timeframe')

    for idx, row in df.iterrows():
        session.execute(f"""INSERT INTO {properties.cassandra_key_space}.{properties.cdr_main_table} \
                        (service_provider_id, cgi_id, usage_timeframe, imei_id, location_azimuth, location_latitude, location_longitude, phone_number, imsi_id, data_type, type_id) \
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                           (
                            service_provider_id,
                            row['cgi_id'],
                            row['usage_timeframe'],
                            row['imei_id'],
                            row['location_azimuth'],
                            row['location_latitude'],
                            row['location_longitude'],
                            row['phone_number'],
                            row['imsi_id'],
                            row['data_type'],
                            row['type_id']
                            ))
