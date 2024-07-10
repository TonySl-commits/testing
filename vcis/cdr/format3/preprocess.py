import warnings
warnings.filterwarnings('ignore')
from pyspark.sql.functions import lit
import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties
from vcis.databases.cassandra.cassandra_tools import CassandraTools

#############################################################################################################################

######################################################### Functions #########################################################
#Date Functions
class CDR_Preprocess():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.cassandra_tools = CassandraTools()
        
    def process_data(self,path,progress_bar,step_progress,offset:bool=False,format_type:str=''):
        progress = 0
        columns_to_select_NB = ['first_nbi_ECGI_SAI', 'imei', 'imsi', 'last_nbi_ECGI_SAI', 'msisdn', 'start_utc_c', 'end_utc_c','data_type_Enrich']        
        columns_to_select_BTS = ['nbi_ECGI_SAI', 'nbi_CELL_NAME', 'Long', 'Lat', 'CGI', 'Azimuth']
        pd.set_option('display.float_format', '{:.0f}'.format)

        df = pd.read_csv(path,delimiter = ';',low_memory=False, dtype={'last_nbi_ECGI_SAI': str,'first_nbi_ECGI_SAI':str},usecols=columns_to_select_NB)
        cells = pd.read_csv(self.properties.passed_filepath_cdr_format3 + "REF_NBI_Cells.csv",dtype={'ECGI/SAI': str}, usecols=columns_to_select_BTS,low_memory=False)
        print(df['data_type_Enrich'].unique())
        df = df[df['data_type_Enrich'] != 'CDR_CORE_CS']
        print(df['data_type_Enrich'].unique())
        if format_type == 'CDR':
            df['data_type_Enrich'].apply(lambda x: x.replace('CDR_CP', 'CDRS') if x == 'CDR_CP' else x)
            df['data_type_Enrich'].apply(lambda x: x.replace('CDR_CIGALE', 'CDRS S1') if x == 'CDR_CIGALE' else x)
        elif format_type == 'SDR':
            df['data_type_Enrich'] = 'SDR'
        print(df['data_type_Enrich'].unique())
        progress = progress + step_progress
        progress_bar.progress(progress)
        
        df = df[columns_to_select_NB]
        cells = cells[columns_to_select_BTS]
        
        print(df['msisdn'].unique())
        df['msisdn'] = pd.to_numeric(df['msisdn'], errors='coerce').dropna().astype('int64').iloc[0]
        column_to_check1 = ['first_nbi_ECGI_SAI','last_nbi_ECGI_SAI']
        column_to_check2 = ['CGI','nbi_ECGI_SAI']

        df_clean = df.dropna(subset=column_to_check1,how='any')
        cells_clean = cells.dropna(subset=column_to_check2,how='any')

        cells_clean['nbi_ECGI_SAI'] = cells_clean['nbi_ECGI_SAI'].astype('int64')
        df_clean['first_nbi_ECGI_SAI'] = df_clean['first_nbi_ECGI_SAI'].astype('int64')
        df_clean['last_nbi_ECGI_SAI'] = df_clean['last_nbi_ECGI_SAI'].astype('int64')

        progress = progress + step_progress
        progress_bar.progress(progress)

        merged_df1 = pd.merge(df_clean, cells_clean, left_on='first_nbi_ECGI_SAI', right_on='nbi_ECGI_SAI')
        merged_df2 = pd.merge(df_clean, cells_clean, left_on='last_nbi_ECGI_SAI', right_on='nbi_ECGI_SAI', how='inner')

        progress = progress + step_progress
        progress_bar.progress(progress)

        start_df = merged_df1[[ 'first_nbi_ECGI_SAI','start_utc_c', 'imei','imsi','msisdn','CGI','Azimuth','Lat','Long','data_type_Enrich']]
        start_df.columns = ['cell_id','usage_timeframe','imei_id','imsi_id','phone_number','cgi_id','location_azimuth','location_latitude','location_longitude','data_type']
        end_df = merged_df2[['last_nbi_ECGI_SAI','end_utc_c', 'imei','imsi','msisdn','CGI','Azimuth','Lat','Long','data_type_Enrich']]
        end_df.columns = ['cell_id','usage_timeframe','imei_id','imsi_id','phone_number','cgi_id','location_azimuth','location_latitude','location_longitude','data_type']

        progress = progress + step_progress
        progress_bar.progress(progress)

        df = pd.concat([start_df, end_df])

        df = df.astype(str)
        progress = progress + step_progress
        progress_bar.progress(progress)

        if not pd.to_numeric(df['imsi_id'], errors='coerce').dropna().empty:
            df['imsi_id'] = pd.to_numeric(df['imsi_id'], errors='coerce').dropna().astype('int64').iloc[0]

        if not pd.to_numeric(df['imei_id'], errors='coerce').dropna().empty:
            df['imei_id'] =  pd.to_numeric(df['imei_id'], errors='coerce').dropna().astype('int64').iloc[0]

        if not pd.to_numeric(df['phone_number'], errors='coerce').dropna().empty:
            df['phone_number'] = pd.to_numeric(df['phone_number'], errors='coerce').dropna().astype('int64').iloc[0]

        df = self.utils.convert_datetime_to_ms(df,offset=offset)

        return df

    def insert_data(self,df,session):
        df['type_id'] = 2
        service_provider_id = 10
        df['location_latitude'] = df['location_latitude'].astype(float)
        df['location_longitude'] = df['location_longitude'].astype(float)
        df['usage_timeframe'] = df['usage_timeframe'].astype('int64')
        df['location_azimuth'] = df['location_azimuth'].astype(str)
        df['phone_number'] = df['phone_number'].astype(str)
        df = df.sort_values('usage_timeframe')

        for idx, row in df.iterrows():
            session.execute(f"""INSERT INTO {self.properties.cassandra_key_space}.{self.properties.cdr_main_table} \
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
