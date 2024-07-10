import warnings
warnings.filterwarnings('ignore')
from pyspark.sql.functions import lit
import findspark
import pandas as pd
from vcis.utils.utils import CDR_Utils, CDR_Properties

findspark.init()

#############################################################################################################################

######################################################### Functions #########################################################
#Date Functions
class CDR_Preprocess():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()


################################################## IMSI FUNCTIONS #####################################################

    def read_excel_files(self, file_path: str):
        xlsx = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
        try:
            data1, data2, data3 = list(xlsx.values())
        except:
            data1, _,data2, data3 = list(xlsx.values())
        data1[self.properties.data_type] = 'CDRS'
        data2[self.properties.data_type] = 'SDR'
        data3[self.properties.data_type] = 'CDRS S1'
        return data1, data2, data3
    
    def select_columns(self, data, columns):
        return data[columns]
    
    def fix_dtypes_d1(self, data1):
        data1[self.properties.cell_id_start_d1] = data1[self.properties.cell_id_start_d1].astype(str)
        data1[self.properties.cell_id_end_d1] = data1[self.properties.cell_id_end_d1].astype(str)
        return data1
    
    def fix_dtypes_d2(self, data2):
        data2[self.properties.cell_id_d2] = data2[self.properties.cell_id_d2].dropna().astype(int).astype(str)
        
        return data2
    
    def fix_dtypes_d3(self, data3):
        data3[self.properties.cell_id_start_d3] = data3[self.properties.cell_id_start_d3].astype(str)
        data3[self.properties.cell_id_end_d3] = data3[self.properties.cell_id_end_d3].astype(str)
        return data3

    def add_milliseconds_to_date(self, data, date_column, milliseconds_column,offset=False):
        # if type(date_column) is not int:
        #     self.utils.convert_datetime_to_ms(df = data, date_column_name = date_column,offset = offset)
        data[date_column] = data[date_column] + data[milliseconds_column]
        print(data[date_column])
        return data
    
    def combine_start_end_d1(self, data,offset=False):
        data_start = data[[self.properties.cell_id_start_d1,self.properties.start_date_d123,self.properties.phone_number,self.properties.imsi_bf,self.properties.imei_bf,self.properties.data_type]]
        data_end = data[[self.properties.cell_id_end_d1,self.properties.end_date_d13,self.properties.data_type]]
        data_start = data_start.rename(columns = {self.properties.cell_id_start_d1:self.properties.cgi_id,
                                              self.properties.start_date_d123:self.properties.usage_timeframe,
                                              self.properties.imsi_bf:self.properties.imsi,
                                              self.properties.imei_bf:self.properties.imei})
        
        data_end = data_end.rename(columns = {self.properties.cell_id_end_d1:self.properties.cgi_id,
                                              self.properties.end_date_d13:self.properties.usage_timeframe,
                                              self.properties.imsi_bf:self.properties.imsi,
                                              self.properties.imei_bf:self.properties.imei})
        data = pd.concat([data_start,data_end])

        print("d1",data[self.properties.usage_timeframe])
        data = self.utils.convert_datetime_to_ms(df = data,date_column_name= self.properties.usage_timeframe,offset=offset)
        print("d1 after",data[self.properties.usage_timeframe])
        return data
    
    def combine_start_end_d3(self, data):
        data_start = data[[self.properties.cell_id_start_d3,self.properties.start_date_d123,self.properties.phone_number,self.properties.imsi_bf,self.properties.imei_bf,self.properties.data_type]]
        data_end = data[[self.properties.cell_id_end_d3,self.properties.end_date_d13,self.properties.data_type]]
        data_start = data_start.rename(columns = {self.properties.cell_id_start_d3:self.properties.cgi_id,
                                              self.properties.start_date_d123:self.properties.usage_timeframe,
                                              self.properties.imsi_bf:self.properties.imsi,
                                              self.properties.imei_bf:self.properties.imei})
        
        data_end = data_end.rename(columns = {self.properties.cell_id_end_d3:self.properties.cgi_id,
                                              self.properties.end_date_d13:self.properties.usage_timeframe,
                                              self.properties.imsi_bf:self.properties.imsi,
                                              self.properties.imei_bf:self.properties.imei})
        data = pd.concat([data_start,data_end])
        return data

    def rename_columns_d2(self, data):
        data = data.rename(columns = {self.properties.cell_id_d2:self.properties.cgi_id,
                                      self.properties.start_date_d123:self.properties.usage_timeframe,
                                              self.properties.imsi_bf:self.properties.imsi,
                                              self.properties.imei_bf:self.properties.imei})
        return data
    
    def add_location_column(self, data, bts_table):
        # bts_table = bts_table.toPandas()
        data = data.sort_values(self.properties.usage_timeframe).reset_index(drop=True)
        joined = pd.merge(data, bts_table, on= self.properties.cgi_id).sort_values(self.properties.usage_timeframe).reset_index(drop=True)
        return joined[self.properties.trace1_columns]
    
    def generate_random_imsi_imei(self, data):
        data[self.properties.imsi] = self.utils.generate_random_imsi()
        data[self.properties.imei] = self.utils.generate_random_imei()
        return data
    
    def add_fixed_columns(self,data):
        data[self.properties.service_provider_id] = self.properties.alfa_id
        data[self.properties.data_source_id] = self.properties.data_source_id_number
        data[self.properties.type_id] = self.properties.type_id
        return data
    
    def fix_milliseconds(self,data):
        x=0
        while x<5:
            data[self.properties.usage_timeframe] = data.groupby(self.properties.usage_timeframe).cumcount() + data[self.properties.usage_timeframe]
            x+=1
        data = data.drop_duplicates()        
        return data