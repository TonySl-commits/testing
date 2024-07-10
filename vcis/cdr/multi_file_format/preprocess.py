from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.cdr.multi_file_format.preproccess_funtions import CDR_Preproccess_fucntions
from vcis.cdr.multi_file_format.loading_CDR_data_txt import Loading_CDR_data
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from vcis.cdr.multi_file_format.loading_CDR_data_txt import Loading_CDR_data
from pyspark.sql.functions import lit, col , current_timestamp , unix_timestamp , when , concat , udf , explode , broadcast
from pyspark.sql.types import StringType, StructType, StructField

class CDR_Preproccess:
    def __init__(self, 
                 verbose: bool = False,
                 passed_connection_host: str = "10.10.10.101"):
        self.verbose = verbose
        self.passed_connection_host = passed_connection_host
        
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.cassandra_spark_tools = CassandraSparkTools()
        self.preproccess_functions = CDR_Preproccess_fucntions()
        self.loading_txt = Loading_CDR_data()
        
        self.loc_location_cdr_in_calls = self.loading_txt.load_loc_location_cdr_in_calls()
        self.loc_location_cdr_out_calls = self.loading_txt.load_loc_location_cdr_out_calls()
        self.loc_location_cdr_in_sms = self.loading_txt.load_loc_location_cdr_in_sms()
        self.loc_location_cdr_out_sms = self.loading_txt.load_loc_location_cdr_out_sms()
        self.loc_location_cdr_voltein = self.loading_txt.load_loc_location_cdr_voltein()
        self.loc_location_cdr_volteout = self.loading_txt.load_loc_location_cdr_volteout()
        self.loc_location_cdr_data_session = self.loading_txt.load_loc_location_cdr_data_session()
        self.loc_location_cdr_data_session_roaming = self.loading_txt.load_loc_location_cdr_data_session_roaming()
        self.loc_location_cdr_data_session_s = self.loading_txt.load_loc_location_cdr_data_session_s()
        
        self.bts_table = self.cassandra_spark_tools.get_spark_data(passed_table_name = self.properties.bts_table_name,passed_connection_host = self.passed_connection_host)
        self.transform_udf_called = self.preproccess_functions.bts_called_transformation()
        self.transform_udf_calling = self.preproccess_functions.bts_calling_transformation()
        self.list_country_codes = self.properties.country_codes_list
        
        # self.leb_number_transformation = self.preproccess_functions.leb_number_transformation()
        # self.return_type = StructType([StructField('country_code', StringType(), True),StructField('phone_no', StringType(), True)])
        # self.udf_apply_transformations = udf(f = self.leb_number_transformation, returnType=StructType([StructField(name = 'country_code', dataType= StringType(), nullable= True),StructField(name = 'phone_no', dataType= StringType(), nullable= True)]))  
        # useArrow=True
    ###############################################################################################################################
    
    def process_calls(self):
        loc_location_cdr_in_calls = self.loc_location_cdr_in_calls
        udf_apply_transformations =  udf(f = self.preproccess_functions.leb_number_transformation, returnType=StructType([StructField(name = 'country_code', dataType= StringType(), nullable= True),StructField(name = 'phone_no', dataType= StringType(), nullable= True)]) )  

        # Phone numbers
        if 'called_no' in loc_location_cdr_in_calls.columns:
            loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
                .withColumn('called_country_code', col('transformed_called.country_code')) \
                .withColumn('called_no', col('transformed_called.phone_no')) \
                .drop('transformed_called')
        if 'calling_no' in loc_location_cdr_in_calls.columns:
            loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(True)
            )) \
                .withColumn('calling_country_code', col('transformed_calling.country_code')) \
                .withColumn('calling_no', col('transformed_calling.phone_no')) \
                .drop('transformed_calling')

        # Date columns
        loc_location_cdr_in_calls = self.preproccess_functions.concatenate_date_columns_1(loc_location_cdr_in_calls)

        # BTS
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn(
            "called_start_cgi_id",
            self.transform_udf_called("called_start_cgi_id", "called_end_cgi_id").getItem("called_start_cgi_id")
        ).withColumn(
            "called_end_cgi_id",
            self.transform_udf_called("called_start_cgi_id", "called_end_cgi_id").getItem("called_end_cgi_id")
        )

        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn(
        "call_datetime",
        when(
            (col("call_datetime").isNull()) | (col("call_datetime") == "N/A"),
            unix_timestamp(current_timestamp()) * 1000  
        ).otherwise(col("call_datetime")))

        loc_location_cdr_out_calls = self.loc_location_cdr_out_calls

        #Phone numbers
        if 'called_no' in loc_location_cdr_out_calls.columns:
            loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_out_calls.columns:
            loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')
            
        #Date columns
        loc_location_cdr_out_calls = self.preproccess_functions.concatenate_date_columns_1(loc_location_cdr_out_calls)

        #BTS
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn(
            "calling_start_cgi_id",
            self.transform_udf_calling("calling_start_cgi_id", "calling_end_cgi_id").getItem("calling_start_cgi_id")
        ).withColumn(
            "calling_end_cgi_id",
            self.transform_udf_calling("calling_start_cgi_id", "calling_end_cgi_id").getItem("calling_end_cgi_id")
        )
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("call_datetime",
                                    when(
                                        (col("call_datetime").isNull()) | (col("call_datetime") == "N/A"),
                                        unix_timestamp(current_timestamp()) * 1000  
                                    ).otherwise(col("call_datetime")))

        return loc_location_cdr_in_calls, loc_location_cdr_out_calls
    
    ###############################################################################################################################

    def process_sms(self):
        loc_location_cdr_in_sms = self.loc_location_cdr_in_sms
        udf_apply_transformations =  udf(f = self.preproccess_functions.leb_number_transformation, returnType=StructType([StructField(name = 'country_code', dataType= StringType(), nullable= True),StructField(name = 'phone_no', dataType= StringType(), nullable= True)]))  


        #Phone numbers
        if 'called_no' in loc_location_cdr_in_sms.columns:
            loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_in_sms.columns:
            loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')

        #Date columns
        loc_location_cdr_in_sms = self.preproccess_functions.concatenate_date_columns_1(loc_location_cdr_in_sms)

        #BTS
        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn(
            "called_start_cgi_id",
            self.transform_udf_called("called_start_cgi_id", "called_end_cgi_id").getItem("called_start_cgi_id")
        ).withColumn(
            "called_end_cgi_id",
            self.transform_udf_called("called_start_cgi_id", "called_end_cgi_id").getItem("called_end_cgi_id")
        )

        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn(
            "call_datetime",
            when(
                (col("call_datetime").isNull()) | (col("call_datetime") == "N/A"),
                unix_timestamp(current_timestamp()) * 1000  
            ).otherwise(col("call_datetime")))
    

        loc_location_cdr_out_sms = self.loc_location_cdr_out_sms
        #Phone numbers
        if 'called_no' in loc_location_cdr_out_sms.columns:
            loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_out_sms.columns:
            loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')

        #Date columns
        loc_location_cdr_out_sms = self.preproccess_functions.concatenate_date_columns_1(loc_location_cdr_out_sms)

        #BTS
        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn(
            "calling_start_cgi_id",
            self.transform_udf_calling("calling_start_cgi_id", "calling_end_cgi_id").getItem("calling_start_cgi_id")
        ).withColumn(
            "calling_end_cgi_id",
            self.transform_udf_calling("calling_start_cgi_id", "calling_end_cgi_id").getItem("calling_end_cgi_id")
        )

        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn("call_datetime",
                                    when(
                                        (col("call_datetime").isNull()) | (col("call_datetime") == "N/A"),
                                        unix_timestamp(current_timestamp()) * 1000  
                                    ).otherwise(col("call_datetime"))) 
        
        return loc_location_cdr_in_sms, loc_location_cdr_out_sms
    
    ###############################################################################################################################

    def process_volte(self):
        loc_location_cdr_voltein = self.loc_location_cdr_voltein

        udf_apply_transformations =  udf(f = self.preproccess_functions.leb_number_transformation, returnType=StructType([StructField(name = 'country_code', dataType= StringType(), nullable= True),StructField(name = 'phone_no', dataType= StringType(), nullable= True)]))  

        #Phone numbers
        if 'called_no' in loc_location_cdr_voltein.columns:
            loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_voltein.columns:
            loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')

            loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn(
                                        "called_bdate",
                                        when(
                                            (col("called_bdate").isNull()) | (col("called_bdate") == "N/A"),
                                            unix_timestamp(current_timestamp()) * 1000  
                                        ).otherwise(col("called_bdate"))) 
                                        

        #Date columns
        loc_location_cdr_voltein = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_voltein, 'called_bdate')
        
        loc_location_cdr_volteout = self.loc_location_cdr_volteout
    
        #Phone numbers
        if 'called_no' in loc_location_cdr_volteout.columns:
            loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_volteout.columns:
            loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')


        #Date columns
        loc_location_cdr_volteout = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_volteout, 'calling_bdate')

        loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn(
                                    "calling_bdate",
                                    when(
                                        (col("calling_bdate").isNull()) | (col("calling_bdate") == "N/A"),
                                        unix_timestamp(current_timestamp()) * 1000  
                                    ).otherwise(col("calling_bdate")))

        return loc_location_cdr_voltein, loc_location_cdr_volteout
    
    ###############################################################################################################################

    def process_data_session(self):
        udf_apply_transformations = udf(self.preproccess_functions.leb_number_transformation, StructType([
        StructField('country_code', StringType(), True),
        StructField('phone_no', StringType(), False)
        ]))
        loc_location_cdr_data_session_roaming = self.loc_location_cdr_data_session_roaming
        #Phone numbers
        if 'called_no' in loc_location_cdr_data_session_roaming.columns:
            loc_location_cdr_data_session_roaming = loc_location_cdr_data_session_roaming.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_data_session_roaming.columns:
            loc_location_cdr_data_session_roaming = loc_location_cdr_data_session_roaming.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')

        #Date columns
        loc_location_cdr_data_session_roaming = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_data_session_roaming, 'a')
        loc_location_cdr_data_session_roaming = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_data_session_roaming, 'call_bdate')

        loc_location_cdr_data_session_roaming = loc_location_cdr_data_session_roaming.withColumn(
                                            "call_bdate",
                                            when(
                                                (col("call_bdate").isNull()) | (col("call_bdate") == "N/A"),
                                                unix_timestamp(current_timestamp()) * 1000  
                                            ).otherwise(col("call_bdate")))
        
    
        loc_location_cdr_data_session_s = self.loc_location_cdr_data_session_s
        #Phone numbers
        if 'called_no' in loc_location_cdr_data_session_s.columns:
            loc_location_cdr_data_session_s = loc_location_cdr_data_session_s.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_data_session_s.columns:
            loc_location_cdr_data_session_s = loc_location_cdr_data_session_s.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')
            
        #Date columns
        loc_location_cdr_data_session_s = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_data_session_s, 'a')
        loc_location_cdr_data_session_s = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_data_session_s, 'call_bdate')
        loc_location_cdr_data_session_s = loc_location_cdr_data_session_s.withColumn(
                                        "call_bdate",
                                        when(
                                            (col("call_bdate").isNull()) | (col("call_bdate") == "N/A"),
                                            unix_timestamp(current_timestamp()) * 1000  
                                        ).otherwise(col("call_bdate")))

        loc_location_cdr_data_session = self.loc_location_cdr_data_session

        #Phone numbers
        if 'called_no' in loc_location_cdr_data_session.columns:
            loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn('transformed_called', udf_apply_transformations(
                col('called_no'), lit(True)
            )) \
            .withColumn('called_country_code', col('transformed_called.country_code')) \
            .withColumn('called_no', col('transformed_called.phone_no')) \
            .drop('transformed_called')

        if 'calling_no' in loc_location_cdr_data_session.columns:
            loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn('transformed_calling', udf_apply_transformations(
                col('calling_no'), lit(False)
            )) \
            .withColumn('calling_country_code', col('transformed_calling.country_code')) \
            .withColumn('calling_no', col('transformed_calling.phone_no')) \
            .drop('transformed_calling')
            
        #Date columns
        loc_location_cdr_data_session = self.preproccess_functions.convert_date_to_mill(loc_location_cdr_data_session, 'call_bdate')
        loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn(
                                        "call_bdate",
                                        when(
                                            (col("call_bdate").isNull()) | (col("call_bdate") == "N/A"),
                                            unix_timestamp(current_timestamp()) * 1000  
                                        ).otherwise(col("call_bdate")))
        
        return loc_location_cdr_data_session, loc_location_cdr_data_session_roaming, loc_location_cdr_data_session_s
    
    ###############################################################################################################################