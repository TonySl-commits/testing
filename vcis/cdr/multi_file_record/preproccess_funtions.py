from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils

from pyspark.sql.functions import expr, lit, split, col, regexp_replace, rand, round, unix_timestamp, concat
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import udf
from pyspark.sql.functions import coalesce, current_timestamp, unix_timestamp, when, col, split
from pyspark.sql.functions import pandas_udf, PandasUDFType
import findspark
findspark.init()

class CDR_Preproccess_fucntions():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.country_codes_list = self.properties.country_codes_list
    #### Phone numbers functions
    # def leb_number_transformation(passed_phone_no, is_called ):
    #     """
    #     Transforms Lebanese phone numbers to a standardized format.

    #     Args:
    #         passed_phone_no (str): The phone number to be transformed.
    #         list_codes (list): List of codes to match against.

    #     Returns:
    #         tuple: A tuple containing the matched code and the transformed phone number.
    #     """
            
    #     if passed_phone_no.startswith('96103') and len(passed_phone_no) == 11:
    #         passed_phone_no = '961' + passed_phone_no[4:]  
    #         return '961', passed_phone_no
        
    #     if passed_phone_no.startswith('0096103') and len(passed_phone_no) == 13:
    #         passed_phone_no = '961' + passed_phone_no[6:]
    #         return '961', passed_phone_no
        
    #     if passed_phone_no.startswith('00961') and len(passed_phone_no) == 12 and passed_phone_no[5] == '3':
    #         passed_phone_no = passed_phone_no[2:]  
    #         return '961', passed_phone_no

    #     if passed_phone_no.startswith('961') and len(passed_phone_no) == 10 and passed_phone_no[3] == '3':
    #         return '961', passed_phone_no

    #     if len(passed_phone_no) == 8 and passed_phone_no[:2] in ['70', '71', '76', '78', '79', '81', '09', '06']:
    #         return '961', '961' + passed_phone_no

    #     if len(passed_phone_no) == 8 and passed_phone_no.startswith('03'):
    #         return '961', '961' + passed_phone_no[1:]

    #     if len(passed_phone_no) == 7 and passed_phone_no.startswith('3'):
    #         return '961', '9610' + passed_phone_no
        
    #     if passed_phone_no.startswith('961') and len(passed_phone_no) == 11:
    #         return '961', passed_phone_no

    #     if passed_phone_no.startswith('00961') and len(passed_phone_no) == 13:
    #         passed_phone_no = passed_phone_no[2:]  
    #         return '961', passed_phone_no
        
    #     if passed_phone_no.startswith(tuple(passed_country_codes_list)):
    #         matched_code = next((code for code in passed_country_codes_list if passed_phone_no.startswith(code)), None)
    #         if matched_code:
    #             return matched_code, passed_phone_no

    #     return None, passed_phone_no
    #### Date functions
    def convert_date_to_mill(self, table, passed_date_column_name: str):
        """
        Converts a date column in a table to milliseconds.

        Args:
            table (DataFrame): The table containing the date column.
            passed_date_column_name (str): The name of the date column.
            passed_date_format (str): The format of the date column.
        Returns:
            DataFrame: The table with the converted date column.
        """
        try:
            table = table.withColumn(passed_date_column_name, F.to_timestamp(col(passed_date_column_name), 'yyyyMMddHHmmss'))

        except:
            print("date is not in this format yyyyMMddHHmmss")
        
        try:
            table = table.withColumn(passed_date_column_name, F.to_timestamp(col(passed_date_column_name), 'dd/MM/yyyy hh:mm:ss a'))

        except:
            print("date is not in this format dd/MM/yyyy hh:mm:ss a")
        try:
            table = table.withColumn(passed_date_column_name, F.to_timestamp(col(passed_date_column_name), 'yyyy-MM-dd HH:mm:ss'))

        except:
            print("date is not in this format yyyy-MM-dd HH:mm:ss")
            print("please check the date format")

        table = table.withColumn(passed_date_column_name, col(passed_date_column_name).cast('long') * 1000)
        return table


    def concatenate_date_columns_1(self, table):
        """
        Concatenates 'call_bdate' and 'call_btime' columns in a table and converts the result to milliseconds.

        Args:
            table (DataFrame): The table containing the 'call_bdate' and 'call_btime' columns.

        Returns:
            DataFrame: The table with the concatenated and converted date column.
        """
        table = table.withColumn('call_datetime', concat(col('call_bdate'), lit(' '), col('call_btime')))
        table = table.withColumn('call_datetime', unix_timestamp('call_datetime', 'dd/MM/yy HH:mm:ss') * 1000)
        table = table.drop('call_bdate', 'call_btime')
        table = table.withColumn('process_flag', lit(1))
        return table

    def concatenate_date_columns_2(self, table):
        """
        Concatenates 'zdate' and 'ztime' columns in a table and converts the result to milliseconds.

        Args:
            table (DataFrame): The table containing the 'zdate' and 'ztime' columns.

        Returns:
            DataFrame: The table with the concatenated and converted date column.
        """
        table = table.withColumn('z_datetime', concat(col('zdate'), lit(' '), col('ztime')))
        table = table.withColumn('z_datetime', unix_timestamp('z_datetime', 'dd-MMM-yy HH:mm:ss') * 1000)
        table = table.drop('zdate', 'ztime')
        return table

    #### BTS Functions
    def bts_called_transformation(self):
        """
        Transformation function for calledBTS.

        Returns:
            None
        """
        def transformation(called_start_cgi_id, called_end_cgi_id):
            if called_start_cgi_id is not None and called_start_cgi_id.startswith('41501') and len(called_start_cgi_id) == 13:
                # Transformation logic goes here
                pass
                called_start_cgi_id = called_start_cgi_id[5:] 
            
            if called_start_cgi_id is not None and called_end_cgi_id.startswith('41501') and len(called_end_cgi_id) == 13:
                called_end_cgi_id = called_end_cgi_id[5:] 

            return called_start_cgi_id, called_end_cgi_id
        
        return udf(transformation, StructType([
            StructField('called_start_cgi_id', StringType(), True),
            StructField('called_end_cgi_id', StringType(), True)
        ]))
    
    def bts_calling_transformation(self):
        """
        Transformation function for callingBTS.

        Returns:
            None
        """
        def transformation(calling_start_cgi_id, calling_end_cgi_id):
            if calling_start_cgi_id is not None and calling_start_cgi_id.startswith('41501') and len(calling_start_cgi_id) == 13:
                calling_start_cgi_id = calling_start_cgi_id[5:]
            
            if calling_end_cgi_id is not None and calling_end_cgi_id.startswith('41501') and len(calling_end_cgi_id) == 13:
                calling_end_cgi_id = calling_end_cgi_id[5:]

            return calling_start_cgi_id, calling_end_cgi_id
        
        return udf(transformation, StructType([
            StructField('calling_start_cgi_id', StringType(), True), 
            StructField('calling_end_cgi_id', StringType(), True)    
        ]) )

    #### Add Location Cloumns
    def add_location_columns(self, passed_dataframe, passed_bts_table):
        """
        Adds location columns to a DataFrame by joining it with a BTS table.

        Args:
            passed_dataframe (DataFrame): The DataFrame to add location columns to.
            passed_bts_table (DataFrame): The BTS table to join with.

        Returns:
            DataFrame: The DataFrame with the added location columns.
        """
        joined_df_1 = passed_dataframe.join(passed_bts_table, 
                                    passed_dataframe["cgi_id"] == passed_bts_table["bts_cgi"],
                                    "left")
        selected_columns = [
            passed_dataframe["*"],
            joined_df_1["bts_lat"].alias("location_latitude"),  # Replace "column1" with the actual column names
            joined_df_1["bts_long"].alias("location_longitude"),
            joined_df_1["bts_azimuth"].alias("location_azimuth")
        ]
        joined_df_1 = joined_df_1.select(*selected_columns)
        return joined_df_1
