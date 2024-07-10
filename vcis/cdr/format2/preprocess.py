from vcis.utils.utils import CDR_Properties , CDR_Utils
import pandas as pd
import streamlit as st

class CDR_Preprocess():

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

    def select_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.rename(columns={self.properties.phone_number_before_rename: self.properties.phone_number,
                                self.properties.imsi_bf : self.properties.imsi,
                                self.properties.imei_bf: self.properties.imei})

        df = df.rename(columns={self.properties.data_type_bf : self.properties.data_type})
        df = df.dropna(subset=[self.properties.data_type])

        return df[self.properties.records_col]
    
    def join_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        start = df[self.properties.records_start_col]
        end = df[self.properties.records_end_col]
        start = start.rename(columns=self.properties.start_records_column_mapping)
        end = end.rename(columns=self.properties.end_records_column_mapping)
        df = pd.concat([start,end]) 
        df =df.sort_values(by=[self.properties.usage_timeframe])  
        nona= df.shape[0]
        df = df.dropna(subset=[self.properties.bts_site_name])
        na = df.shape[0]
        print(f"Null values dropped : {nona-na}")
        return df
    
    def add_location_column(self, df, bts_table):
        df = df.sort_values(self.properties.usage_timeframe).reset_index(drop=True)
        joined = pd.merge(df, bts_table, on= self.properties.bts_site_name).sort_values(self.properties.usage_timeframe).reset_index(drop=True)
        return joined[self.properties.records_col_updated]
    
    def add_millisecond_to_date(self, df):
        df[self.properties.usage_timeframe_ms] = df[self.properties.usage_timeframe_ms].astype(int)
        df[self.properties.usage_timeframe] = df[self.properties.usage_timeframe] + df[self.properties.usage_timeframe_ms]
        df = df.drop(columns=[self.properties.usage_timeframe_ms])
        df = df.dropna(subset=[self.properties.cgi_id])
        return df
    
    def fix_azimuth(self, df):
        df[self.properties.location_azimuth] = df[self.properties.location_azimuth].replace('Indoor', 360)
    
    def generate_random_imsi_imei(self, df):
        df[self.properties.imsi] = self.utils.generate_random_imei()
        df[self.properties.imei] = self.utils.generate_random_imsi()
        return df
    
    def fix_datatype_column(self, df):
        df[self.properties.data_type] = df[self.properties.data_type].apply(lambda x: x.replace('CDR CP', 'CDRS') if x == 'CDR CP' else x)
        df[self.properties.data_type] = df[self.properties.data_type].apply(lambda x: x.replace('SDR', 'SDR') if x == 'SDR' else x)
        df[self.properties.data_type] = df[self.properties.data_type].apply(lambda x: 'CDRS S1' if x.startswith('(S1)') else x)
        return df
###################################################################################################
    def insert_to_cassandra(self, df):
        spark = self.utils.get_spark_connection()
        df = df.drop(columns=[self.properties.bts_site_name])
        df[self.properties.service_provider_id] = self.properties.alfa_id
        df[self.properties.data_source_id] = self.properties.data_source_id_number
        if(type(df) == pd.DataFrame):
            df = spark.createDataFrame(df)
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=self.properties.cdr_table_name, keyspace="datacrowd") \
            .mode("append") \
            .save()
        return df
###################################################################################################    
    def merge_bts_tables(self, bts_table,new_bts_table):
        new_bts_table['location_azimuth'] = new_bts_table['location_azimuth'].astype(str)
        new_bts_table['location_latitude'] = new_bts_table['location_latitude'].astype(float)
        new_bts_table['location_longitude'] = new_bts_table['location_longitude'].astype(float)
        
        bts_table['location_azimuth'] = bts_table['location_azimuth'].astype(str)
        bts_table['location_latitude'] = bts_table['location_latitude'].astype(float)
        bts_table['location_longitude'] = bts_table['location_longitude'].astype(float)
        merged_bts_table = pd.merge(bts_table, new_bts_table, on=['location_latitude','location_longitude','location_azimuth'],how='left')
        return merged_bts_table
    