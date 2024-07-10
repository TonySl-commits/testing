from vcis.utils.utils import CDR_Properties , CDR_Utils
import pandas as pd
import findspark

findspark.init()

class CDR_Preprocess():

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

    def add_tech_column(self,twog,threeg,LTE):
        twog[self.properties.network_type_code] = 1
        threeg[self.properties.network_type_code] = 2
        LTE[self.properties.network_type_code] = 3
        return twog,threeg,LTE
    
    def rename_columns(self,twog,threeg,LTE):
        threeg = threeg.rename(columns=self.properties.column_mapping_3g)
        LTE = LTE.rename(columns=self.properties.column_mapping_LTE)
        twog = twog.rename(columns = self.properties.column_mapping)
        threeg = threeg.rename(columns = self.properties.column_mapping)
        LTE = LTE.rename(columns = self.properties.column_mapping)
        return twog, threeg, LTE
    
    def add_default_columns(self,df,network_type):
        df[self.properties.bts_cell_id] = 0
        df[self.properties.status_code] = 2
        df[self.properties.status_bdate] = '1-1-2023'
        df[self.properties.location_altitude] = 0
        df[self.properties.creation_date] = '1-1-2023'
        df[self.properties.created_by] = 0
        df[self.properties.update_date] = '1-1-2023'
        df[self.properties.updated_by] = 0
        df[self.properties.equipment_id] = 1
        df[self.properties.bts_scrambling_code] = 0
        df[self.properties.routing_area_code] = 0
        df[self.properties.is_splitter_used] = 0

        if network_type == 'alfa':

            df[self.properties.service_provider_id] = 10
        else:
            df[self.properties.service_provider_id] = 15
        return df
    

    def add_rnc_column(self,df):
        df[self.properties.bts_rnc_id] = pd.to_numeric(df[self.properties.rnc_code].str.extract("RNC(\\d+)", expand=False), errors='coerce').fillna(0).astype(int)
        return df
    
    def add_lcid_column(self,df):
        df[self.properties.logical_cell_code] = df.apply(
            lambda row: row[self.properties.bts_cell_internal_code]
            if (row[self.properties.network_type_code] == 1)
            else (row[self.properties.bts_cell_internal_code] + row[self.properties.bts_rnc_id] * 65536) 
                if (row[self.properties.network_type_code] == 2) 
                else ((row[self.properties.evolved_node_b] *256) + row[self.properties.sector])/1, axis=1)
        return df
    
    def add_frequency_column(self,df):
        df[self.properties.device_frequency] = df.apply(
            lambda row: 900 
            if ((1 <= row[self.properties.bcch_code] <= 124) & (row[self.properties.network_type_code] == 1))
            else (1800 
                if ((row[self.properties.bcch_code] > 500) & (row[self.properties.network_type_code] == 1)) 
                else (1800 
                    if ((1649 <= row[self.properties.earfcn_code] <= 6376) & (row[self.properties.network_type_code] == 3)) 
                    else (900 
                        if ((len(str(row[self.properties.uafrcn_code])) == 4) & (row[self.properties.network_type_code] == 2)) 
                        else (2100 
                            if ((len(str(row[self.properties.uafrcn_code])) == 5) & (row[self.properties.network_type_code] == 2)) 
                            else None)))), axis=1)
        return df
    
    def round_coordinates(self,df):
        df['location_longitude'] = df['location_longitude'].astype(float)
        df['location_latitude'] = df['location_latitude'].astype(float)
        df[self.properties.location_longitude] = df[self.properties.location_longitude].round(4)
        df[self.properties.location_latitude] = df[self.properties.location_latitude].round(4)
        return df

    def fix_coordinates(self,df):
        df[self.properties.location_longitude] = df[self.properties.location_longitude].astype(float)
        df[self.properties.location_latitude] =  df[self.properties.location_latitude].astype(float)

        df[self.properties.location_long] = df[self.properties.location_longitude].apply(self.utils.truncate_and_divide)
        df[self.properties.location_lat] = df[self.properties.location_latitude].apply(self.utils.truncate_and_divide)

        df[self.properties.location_longitude] = df.groupby([self.properties.location_long, self.properties.location_lat])[self.properties.location_longitude].transform('first')
        df[self.properties.location_latitude] = df.groupby([self.properties.location_long, self.properties.location_lat])[self.properties.location_latitude].transform('first')
        return df
    
    def select_columns(self,df):
        df = df[[
            self.properties.bts_cell_id,
            self.properties.bts_cell_internal_code,
            self.properties.bts_cell_name,
            self.properties.network_type_code,
            self.properties.status_code,
            self.properties.status_bdate,
            self.properties.location_area_code,
            self.properties.tracking_area_code,
            self.properties.site_name,
            self.properties.cgi_id,
            self.properties.location_longitude,
            self.properties.location_latitude,
            self.properties.location_altitude,
            self.properties.location_azimuth,
            self.properties.bcch_code,
            self.properties.uafrcn_code,
            self.properties.bsi_code,
            self.properties.bsc_code,
            self.properties.rnc_code,
            self.properties.mme_code,
            self.properties.evolved_node_b,
            self.properties.service_provider_id,
            self.properties.creation_date,
            self.properties.created_by,
            self.properties.update_date,
            self.properties.updated_by,
            self.properties.equipment_id,
            self.properties.country_division_name,
            self.properties.is_splitter_used,
            self.properties.bts_scrambling_code,
            self.properties.routing_area_code,
            self.properties.logical_cell_code,
            self.properties.device_frequency
    ]]
        return df
    def fix_azimuth(self, df):
        df[self.properties.location_azimuth] = df[self.properties.location_azimuth].replace('Indoor', 360)
        
    def fix_columns_type(self,df):
        for col in self.properties.int_col:
            df[col] = df[col].fillna(0).astype(int)

        for col in self.properties.date_col:
            df[col] = pd.to_datetime(df[col])

        for col in self.properties.str_col:
            df[col] = df[col].astype(str)

        return df

    def merge_coordinates_touch(self,data,old_data):
        data = pd.merge(data, old_data, on=[self.properties.location_latitude, self.properties.location_longitude], how='inner')
    
