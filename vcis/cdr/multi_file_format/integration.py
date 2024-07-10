from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from vcis.cdr.multi_file_format.preproccess_funtions import CDR_Preproccess_fucntions
from vcis.cdr.multi_file_format.preprocess import CDR_Preproccess
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools
from pyspark.sql.functions import expr, lit, split, col, regexp_replace, rand, round, unix_timestamp, concat
from cassandra.cluster import Cluster
class CDR_Integration():
    def __init__(self, 
                 verbose: bool = False,
                 passed_connection_host: str = '10.10.10.101'):
        self.verbose = verbose
        self.passed_connection_host = passed_connection_host
        
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.preproccess_fucntions = CDR_Preproccess_fucntions()
        self.preproccess = CDR_Preproccess(passed_connection_host = self.passed_connection_host)
        self.cassandra_spark_tools = CassandraSparkTools()
        
        self.bts_table = self.cassandra_spark_tools.get_spark_data(self.properties.bts_table_name, passed_connection_host = self.passed_connection_host)
        self.loc_location_cdr_in_calls , self.loc_location_cdr_out_calls = self.preproccess.process_calls()
        self.loc_location_cdr_in_sms , self.loc_location_cdr_out_sms = self.preproccess.process_sms()
        self.loc_location_cdr_voltein , self.loc_location_cdr_volteout = self.preproccess.process_volte()
        
        self.loc_location_cdr_data_session , self.loc_location_cdr_data_session_roaming , self.loc_location_cdr_data_session_s = self.preproccess.process_data_session()
        
        self.cdr_columns_names = self.properties.cdr_columns_names
        self.cdr_main_table_name = self.properties.cdr_main_table_name
        
############################################################################################################################################################################        

    ########### Calls ###########
    
    def intergrate_call (self):
        loc_location_cdr_in_calls = self.loc_location_cdr_in_calls
        loc_location_cdr_out_calls = self.loc_location_cdr_out_calls

        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumnRenamed("call_datetime", "call_bdate")
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("call_duration_ms", expr("call_duration * 1000"))
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("call_edate", expr("call_bdate + call_duration_ms"))
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("call_edate", expr("call_bdate + call_duration"))


        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("data_source", lit('2'))
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("type", lit('1'))
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.withColumn("service_provider_id", lit('10'))
        columns_to_select = [
            'called_end_cgi_id',
            'called_imei_id',
            'called_imsi_id',
            'called_no',
            'called_start_cgi_id',
            'called_end_cgi_id',
            'service_provider_id',
            'call_edate',
            'call_bdate',
            'type',
            'data_source'
        ]
        loc_location_cdr_in_calls = loc_location_cdr_in_calls.select(columns_to_select)

        df_start = loc_location_cdr_in_calls.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                                                        "called_start_cgi_id as called_cgi_id", "call_bdate as call_date", "type", 
                                                        "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_in_calls.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                                                    "called_end_cgi_id as called_cgi_id",
                                                    "call_edate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_in_calls = df_start.union(df_end)
        existing_columns = result_in_calls.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}
        # Apply column renaming
        formated_in_calls = result_in_calls.select([col(c).alias(column_mapping[c]) for c in result_in_calls.columns])
        formated_in_calls= self.preproccess_fucntions.add_location_columns(formated_in_calls, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_in_calls = formated_in_calls.withColumn(column_name, formated_in_calls[column_name].cast(column_type))
        cdr_main_table_name = self.cdr_main_table_name
        def insert_into_cassandra(partition):
        
            cluster = Cluster(['10.1.10.66'])
            session = cluster.connect('datacrowd')

            for row in partition:
                insert_statement = """
                    INSERT INTO {}
                    (imsi_id, usage_timeframe, cgi_id, data_source_id, imei_id, 
                    location_azimuth, location_latitude, location_longitude, 
                    phone_number, service_provider_id, type_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(cdr_main_table_name)
                session.execute(insert_statement, (
                    row["imsi_id"], row["usage_timeframe"], row["cgi_id"], 
                    row["data_source_id"], row["imei_id"], row["location_azimuth"], 
                    row["location_latitude"], row["location_longitude"], 
                    row["phone_number"], row["service_provider_id"], row["type_id"]
                ))
        formated_in_calls = formated_in_calls.na.drop(subset=["usage_timeframe"])
        # Insert into cassandra
        formated_in_calls.foreachPartition(insert_into_cassandra)


        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumnRenamed("call_datetime", "call_bdate")
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("call_duration_ms", expr("call_duration * 1000"))
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("call_edate", expr("call_bdate + call_duration_ms"))
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("call_edate", expr("call_bdate + call_duration"))


        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("data_source", lit('2'))
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("type", lit('1'))
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.withColumn("service_provider_id", lit('10'))
        columns_to_select = [
            'calling_end_cgi_id',
            'calling_imei_id',
            'calling_imsi_id',
            'calling_no',
            'calling_start_cgi_id',
            'calling_end_cgi_id',
            'service_provider_id',
            'call_edate',
            'call_bdate',
            'type',
            'data_source'
        ]
        loc_location_cdr_out_calls = loc_location_cdr_out_calls.select(columns_to_select)

        df_start = loc_location_cdr_out_calls.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_start_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_out_calls.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_end_cgi_id", 
                            "call_edate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_out_calls = df_start.union(df_end)
        existing_columns = result_out_calls.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_out_calls = result_out_calls.select([col(c).alias(column_mapping[c]) for c in result_out_calls.columns])
        formated_out_calls= self.preproccess_fucntions.add_location_columns(formated_out_calls, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_out_calls = formated_out_calls.withColumn(column_name, formated_out_calls[column_name].cast(column_type))
        
        formated_out_calls = formated_out_calls.na.drop(subset=["usage_timeframe"])

        # Insert into cassandra
        formated_out_calls.foreachPartition(insert_into_cassandra)

        return formated_in_calls, formated_out_calls
############################################################################################################################################################################        
    
    ########### SMS ###########

    def intergrate_sms (self):
        loc_location_cdr_in_sms = self.loc_location_cdr_in_sms
        loc_location_cdr_out_sms = self.loc_location_cdr_out_sms


        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn("service_provider_id", lit('10'))
        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn("type", lit("2"))
        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumn("data_source", lit('2'))

        loc_location_cdr_in_sms = loc_location_cdr_in_sms.withColumnRenamed("call_datetime", "call_bdate")
        columns_to_select = [
            'called_imei_id',
            'called_imsi_id',
            'called_no',
            'called_start_cgi_id',
            'called_end_cgi_id',
            'service_provider_id',
            'call_bdate',
            'type',
            'data_source'
        ]
        loc_location_cdr_in_sms = loc_location_cdr_in_sms.select(columns_to_select)

        df_start = loc_location_cdr_in_sms.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_start_cgi_id as called_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_in_sms.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_end_cgi_id as called_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_in_sms = df_start.union(df_end)
        existing_columns = result_in_sms.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_in_sms = result_in_sms.select([col(c).alias(column_mapping[c]) for c in result_in_sms.columns])
        formated_in_sms= self.preproccess_fucntions.add_location_columns(formated_in_sms, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_in_sms = formated_in_sms.withColumn(column_name, formated_in_sms[column_name].cast(column_type))
        cdr_main_table_name = self.cdr_main_table_name
        def insert_into_cassandra(partition):
        
            cluster = Cluster(['10.1.10.66'])
            session = cluster.connect('datacrowd')

            for row in partition:
                insert_statement = """
                    INSERT INTO {}
                    (imsi_id, usage_timeframe, cgi_id, data_source_id, imei_id, 
                    location_azimuth, location_latitude, location_longitude, 
                    phone_number, service_provider_id, type_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(cdr_main_table_name)
                session.execute(insert_statement, (
                    row["imsi_id"], row["usage_timeframe"], row["cgi_id"], 
                    row["data_source_id"], row["imei_id"], row["location_azimuth"], 
                    row["location_latitude"], row["location_longitude"], 
                    row["phone_number"], row["service_provider_id"], row["type_id"]
                ))
        
        formated_in_sms = formated_in_sms.na.drop(subset=["usage_timeframe"])

        # Insert into cassandra
        formated_in_sms.foreachPartition(insert_into_cassandra)


        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn("service_provider_id", lit('10'))
        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn("type", lit("2"))
        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumn("data_source", lit('2'))

        loc_location_cdr_out_sms = loc_location_cdr_out_sms.withColumnRenamed("call_datetime", "call_bdate")
        columns_to_select = [
            'calling_imei_id',
            'calling_imsi_id',
            'calling_no',
            'calling_start_cgi_id',
            'calling_end_cgi_id',
            'service_provider_id',
            'call_bdate',
            'type',
            'data_source'
        ]
        loc_location_cdr_out_sms = loc_location_cdr_out_sms.select(columns_to_select)

        df_start = loc_location_cdr_out_sms.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_start_cgi_id as calling_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_out_sms.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_end_cgi_id as calling_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_out_sms = df_start.union(df_end)
        existing_columns = result_out_sms.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_out_sms = result_out_sms.select([col(c).alias(column_mapping[c]) for c in result_out_sms.columns])
        formated_out_sms= self.preproccess_fucntions.add_location_columns(formated_out_sms, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_out_sms = formated_out_sms.withColumn(column_name, formated_out_sms[column_name].cast(column_type))
        
        formated_out_sms = formated_out_sms.na.drop(subset=["usage_timeframe"])

        # Insert into cassandra
        formated_out_sms.foreachPartition(insert_into_cassandra)

        return formated_in_sms, formated_out_sms
############################################################################################################################################################################        
    
    ########### Volte ###########

    def intergrate_volte (self):

        loc_location_cdr_voltein = self.loc_location_cdr_voltein
        loc_location_cdr_volteout = self.loc_location_cdr_volteout

        loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn("service_provider_id", lit('10'))
        loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn("type", lit("4"))
        loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn("data_source", lit('2'))

        loc_location_cdr_voltein= loc_location_cdr_voltein.withColumn("call_duration_ms", expr("call_duration * 1000"))
        loc_location_cdr_voltein = loc_location_cdr_voltein.withColumn("call_edate", expr("called_bdate + call_duration_ms"))

        columns_to_select = [
            'called_imei_id',
            'called_imsi_id',
            'called_no',
            'called_end_lac_cell',
            'called_start_lac_cell',
            'service_provider_id',
            'called_bdate',
            'call_edate',
            'type',
            'data_source'
        ]
        #loc_location_cdr_voltein = loc_location_cdr_voltein.select(columns_to_select)
        df_start = loc_location_cdr_voltein.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_start_lac_cell as called_cgi_id", 
                            "called_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_voltein.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_end_lac_cell as called_cgi_id", 
                            "call_edate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_voltein = df_start.union(df_end)
        existing_columns = result_voltein.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_voltein = result_voltein.select([col(c).alias(column_mapping[c]) for c in result_voltein.columns])
        formated_voltein= self.preproccess_fucntions.add_location_columns(formated_voltein, self.bts_table)
        
        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_voltein = formated_voltein.withColumn(column_name, formated_voltein[column_name].cast(column_type))
        cdr_main_table_name = self.cdr_main_table_name
        def insert_into_cassandra(partition):
        
            cluster = Cluster(['10.1.10.66'])
            session = cluster.connect('datacrowd')

            for row in partition:
                insert_statement = """
                    INSERT INTO {}
                    (imsi_id, usage_timeframe, cgi_id, data_source_id, imei_id, 
                    location_azimuth, location_latitude, location_longitude, 
                    phone_number, service_provider_id, type_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(cdr_main_table_name)
                session.execute(insert_statement, (
                    row["imsi_id"], row["usage_timeframe"], row["cgi_id"], 
                    row["data_source_id"], row["imei_id"], row["location_azimuth"], 
                    row["location_latitude"], row["location_longitude"], 
                    row["phone_number"], row["service_provider_id"], row["type_id"]
                ))
        formated_voltein = formated_voltein.na.drop(subset=["usage_timeframe"])
        # Insert into cassandra
        formated_voltein.foreachPartition(insert_into_cassandra)

        loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn("service_provider_id", lit('10'))
        loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn("type", lit("4"))
        loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn("data_source", lit('2'))

        loc_location_cdr_volteout= loc_location_cdr_volteout.withColumn("call_duration_ms", expr("calling_duration * 1000"))
        loc_location_cdr_volteout = loc_location_cdr_volteout.withColumn("call_edate", expr("calling_bdate + call_duration_ms"))
        columns_to_select = [
            'calling_imei_id',
            'calling_imsi_id',
            'calling_no',
            'calling_start_lte_cgi_id',
            'calling_end_lte_cgi_id',
            'service_provider_id',
            'calling_bdate',
            'call_edate',
            'type',
            'data_source'
        ]
        loc_location_cdr_volteout = loc_location_cdr_volteout.select(columns_to_select)

        df_start = loc_location_cdr_volteout.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_start_lte_cgi_id as calling_cgi_id", 
                            "calling_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_volteout.selectExpr("calling_imei_id", "calling_imsi_id", "calling_no", 
                            "calling_end_lte_cgi_id as calling_cgi_id", 
                            "call_edate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_volteout = df_start.union(df_end)
        existing_columns = result_volteout.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_volteout = result_volteout.select([col(c).alias(column_mapping[c]) for c in result_volteout.columns])
        formated_volteout= self.preproccess_fucntions.add_location_columns(formated_volteout, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_volteout = formated_volteout.withColumn(column_name, formated_volteout[column_name].cast(column_type))

        formated_volteout = formated_volteout.na.drop(subset=["usage_timeframe"])

        # Insert into cassandra
        formated_volteout.foreachPartition(insert_into_cassandra)

        return formated_voltein, formated_volteout

############################################################################################################################################################################

    ########### Data Session ###########

    def intergrate_data_session (self):
        loc_location_cdr_data_session = self.loc_location_cdr_data_session
        loc_location_cdr_data_session_roaming = self.loc_location_cdr_data_session_roaming
        loc_location_cdr_data_session_s = self.loc_location_cdr_data_session_s

        loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn("service_provider_id", lit('10'))
        loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn("type", lit("3"))
        loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn("data_source", lit('2'))

        loc_location_cdr_data_session= loc_location_cdr_data_session.withColumn("call_duration_ms", expr("call_duration * 1000"))
        loc_location_cdr_data_session = loc_location_cdr_data_session.withColumn("call_edate", expr("call_bdate + call_duration_ms"))

        columns_to_select = [
            'called_imei_id',
            'called_imsi_id',
            'called_no',
            'called_start_cgi_id',
            'called_end_cgi_id',
            'service_provider_id',
            'call_bdate',
            'call_edate',
            'type',
            'data_source'
        ]
        loc_location_cdr_data_session = loc_location_cdr_data_session.select(columns_to_select)
        df_start = loc_location_cdr_data_session.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_start_cgi_id as called_cgi_id", 
                            "call_bdate as call_date", "type", "data_source",'service_provider_id')

        # Select the columns for the second set of rows
        df_end = loc_location_cdr_data_session.selectExpr("called_imei_id", "called_imsi_id", "called_no", 
                            "called_end_cgi_id as called_cgi_id", 
                            "call_edate as call_date", "type", "data_source",'service_provider_id')

        # Combine the two DataFrames into one
        result_data_session = df_start.union(df_end)
        existing_columns = result_data_session.columns
        column_mapping = {existing_columns[i]: self.cdr_columns_names[i] for i in range(len(existing_columns))}

        # Apply column renaming
        formated_data_session = result_data_session.select([col(c).alias(column_mapping[c]) for c in result_data_session.columns])
        formated_data_session= self.preproccess_fucntions.add_location_columns(formated_data_session, self.bts_table)

        # Apply schema mapping to DataFrame
        for column_name, column_type in self.properties.cassandra_schema_mapping.items():
            formated_data_session = formated_data_session.withColumn(column_name, formated_data_session[column_name].cast(column_type))
        cdr_main_table_name = self.cdr_main_table_name
        def insert_into_cassandra(partition):
        
            cluster = Cluster(['10.1.10.66'])
            session = cluster.connect('datacrowd')

            for row in partition:
                insert_statement = """
                    INSERT INTO {}
                    (imsi_id, usage_timeframe, cgi_id, data_source_id, imei_id, 
                    location_azimuth, location_latitude, location_longitude, 
                    phone_number, service_provider_id, type_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(cdr_main_table_name)
                session.execute(insert_statement, (
                    row["imsi_id"], row["usage_timeframe"], row["cgi_id"], 
                    row["data_source_id"], row["imei_id"], row["location_azimuth"], 
                    row["location_latitude"], row["location_longitude"], 
                    row["phone_number"], row["service_provider_id"], row["type_id"]
                ))
        formated_data_session = formated_data_session.na.drop(subset=["usage_timeframe"])

        # Insert into cassandra
        formated_data_session.foreachPartition(insert_into_cassandra)

        return formated_data_session