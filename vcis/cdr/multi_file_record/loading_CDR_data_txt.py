from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils

from pyspark.sql.functions import expr ,lit,split, col,regexp_replace,rand,round,unix_timestamp,concat

class Loading_CDR_data():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.properties = CDR_Properties()
        self.utils = CDR_Utils()
        self.spark = self.utils.get_spark_cennection()
        
    ##############################################################################################################################
    
    def load_loc_location_cdr_in_calls(self):
        return self.spark.read.text(self.properties.cdr_text_calls_in_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("calling_no"),
                split(col("value"), "\t").getItem(1).alias("called_no"),
                split(col("value"), "\t").getItem(2).alias("call_bdate"),
                split(col("value"), "\t").getItem(3).alias("call_btime"),
                split(col("value"), "\t").getItem(4).alias("call_duration"),
                split(col("value"), "\t").getItem(5).alias("called_start_cgi_id"),
                split(col("value"), "\t").getItem(6).alias("called_imei_id"),
                split(col("value"), "\t").getItem(7).alias("called_end_cgi_id"),
                split(col("value"), "\t").getItem(8).alias("called_imsi_id"),
                lit(10).alias("service_provider_id")
            )\
            .na.fill("N/A")

    def load_loc_location_cdr_out_calls(self):
        return self.spark.read.text(self.properties.cdr_test_calls_out_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("calling_no"),
                split(col("value"), "\t").getItem(1).alias("calling_imsi_id"),
                split(col("value"), "\t").getItem(2).alias("called_no"),
                split(col("value"), "\t").getItem(3).alias("call_bdate"),
                split(col("value"), "\t").getItem(4).alias("call_btime"),
                split(col("value"), "\t").getItem(5).alias("call_duration"),
                split(col("value"), "\t").getItem(6).alias("calling_start_cgi_id"),
                split(col("value"), "\t").getItem(7).alias("calling_imei_id"),
                split(col("value"), "\t").getItem(8).alias("calling_end_cgi_id"),
                lit(10).alias("service_provider_id")
            )\
            .na.fill("N/A")

    def load_loc_location_cdr_in_sms(self):
        return self.spark.read.text(self.properties.cdr_text_sms_in_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("called_no"),
                split(col("value"), "\t").getItem(1).alias("called_imsi_id"),
                split(col("value"), "\t").getItem(2).alias("call_bdate"),
                split(col("value"), "\t").getItem(3).alias("call_btime"),
                split(col("value"), "\t").getItem(4).alias("called_smssc_id"),
                split(col("value"), "\t").getItem(5).alias("calling_no"),
                split(col("value"), "\t").getItem(6).alias("called_imei_id"),
                split(col("value"), "\t").getItem(7).alias("called_start_cgi_id"),
                split(col("value"), "\t").getItem(8).alias("called_end_cgi_id"),
                split(col("value"), "\t").getItem(9).alias("bts_no")
            )\
            .withColumn("service_provider_id", lit(10))\
            .na.fill("N/A")

    def load_loc_location_cdr_out_sms(self):
        return self.spark.read.text(self.properties.cdr_text_sms_out_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("calling_no"),
                split(col("value"), "\t").getItem(1).alias("calling_imsi_id"),
                split(col("value"), "\t").getItem(2).alias("call_bdate"),
                split(col("value"), "\t").getItem(3).alias("call_btime"),
                split(col("value"), "\t").getItem(4).alias("calling_smssc_id"),
                split(col("value"), "\t").getItem(5).alias("called_no"),
                split(col("value"), "\t").getItem(6).alias("calling_imei_id"),
                split(col("value"), "\t").getItem(7).alias("calling_start_cgi_id"),
                split(col("value"), "\t").getItem(8).alias("calling_end_cgi_id"),
                lit(10).alias("service_provider_id")
            )\
            .na.fill("N/A")

    def load_loc_location_cdr_voltein(self):
        return self.spark.read.text(self.properties.cdr_text_volte_in_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("calling_no"),
                split(col("value"), "\t").getItem(1).alias("called_no"),
                split(col("value"), "\t").getItem(2).alias("called_bdate"),
                split(col("value"), "\t").getItem(3).alias("call_duration"),
                split(col("value"), "\t").getItem(4).alias("called_start_lac_cell"),
                split(col("value"), "\t").getItem(5).alias("called_end_lac_cell"),
                split(col("value"), "\t").getItem(6).alias("called_imsi_id"),
                split(col("value"), "\t").getItem(7).alias("called_imei_id")
            )\
            .withColumn("service_provider_id", lit(10))\
            .na.fill("N/A")

    def load_loc_location_cdr_volteout(self):
        return self.spark.read.text(self.properties.cdr_text_volte_out_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("calling_no"),
                split(col("value"), "\t").getItem(1).alias("calling_imsi_id"),
                split(col("value"), "\t").getItem(2).alias("called_no"),
                split(col("value"), "\t").getItem(3).alias("calling_bdate"),
                split(col("value"), "\t").getItem(4).alias("calling_duration"),
                split(col("value"), "\t").getItem(5).alias("calling_start_lte_cgi_id"),
                split(col("value"), "\t").getItem(6).alias("calling_imei_id"),
                split(col("value"), "\t").getItem(7).alias("calling_end_lte_cgi_id")
            )\
            .withColumn("service_provider_id", lit(10))\
            .na.fill("N/A")

    def load_loc_location_cdr_data_session_roaming(self):
        return self.spark.read.text(self.properties.cdr_text_data_session_roam_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("called_no"),
                split(col("value"), "\t").getItem(1).alias("called_public_ip_number_1"),
                split(col("value"), "\t").getItem(2).alias("called_public_ip_number_2"),
                split(col("value"), "\t").getItem(3).alias("called_private_ip_number"),
                split(col("value"), "\t").getItem(4).alias("called_imsi_id"),
                split(col("value"), "\t").getItem(5).alias("called_imei_id"),
                split(col("value"), "\t").getItem(6).alias("call_bdate"),
                split(col("value"), "\t").getItem(7).alias("call_duration"),
                split(col("value"), "\t").getItem(8).alias("a"),
                split(col("value"), "\t").getItem(9).alias("b"),
                split(col("value"), "\t").getItem(10).alias("c"),
                split(col("value"), "\t").getItem(11).alias("called_start_lac"),
                split(col("value"), "\t").getItem(12).alias("called_end_lac"),
                split(col("value"), "\t").getItem(13).alias("called_start_cell"),
                split(col("value"), "\t").getItem(14).alias("called_end_cell")
            )\
            .withColumn("service_provider_id", lit(10))\
            .na.fill("N/A")

    def load_loc_location_cdr_data_session(self):
        return self.spark.read.text(self.properties.cdr_text_data_session_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("called_no"),
                split(col("value"), "\t").getItem(1).alias("called_public_ip_number_1"),
                split(col("value"), "\t").getItem(2).alias("called_public_ip_number_2"),
                split(col("value"), "\t").getItem(3).alias("called_private_ip_number"),
                split(col("value"), "\t").getItem(4).alias("called_imsi_id"),
                split(col("value"), "\t").getItem(5).alias("called_imei_id"),
                split(col("value"), "\t").getItem(6).alias("call_bdate"),
                split(col("value"), "\t").getItem(7).alias("call_duration"),
                split(col("value"), "\t").getItem(8).alias("called_start_cgi_id"),
                split(col("value"), "\t").getItem(9).alias("called_end_cgi_id"),
                split(col("value"), "\t").getItem(10).alias("lac1"),
                split(col("value"), "\t").getItem(11).alias("lac2"),
                split(col("value"), "\t").getItem(12).alias("lac3"),
                split(col("value"), "\t").getItem(13).alias("lac4"),
                split(col("value"), "\t").getItem(14).alias("lac5"),
                split(col("value"), "\t").getItem(15).alias("lac6"),
                lit(10).alias("service_provider_id")
            )\
            .na.fill("N/A")

    def load_loc_location_cdr_data_session_s(self):
        return self.spark.read.text(self.properties.cdr_text_data_session_s_path)\
            .select(
                split(col("value"), "\t").getItem(0).alias("called_no"),
                split(col("value"), "\t").getItem(1).alias("called_public_ip_number_1"),
                split(col("value"), "\t").getItem(2).alias("called_public_ip_number_2"),
                split(col("value"), "\t").getItem(3).alias("called_private_ip_number"),
                split(col("value"), "\t").getItem(4).alias("called_imsi_id"),
                split(col("value"), "\t").getItem(5).alias("called_imei_id"),
                split(col("value"), "\t").getItem(6).alias("call_bdate"),
                split(col("value"), "\t").getItem(7).alias("call_duration"),
                split(col("value"), "\t").getItem(8).alias("a"),
                split(col("value"), "\t").getItem(9).alias("b"),
                split(col("value"), "\t").getItem(10).alias("c"),
                split(col("value"), "\t").getItem(11).alias("called_start_lac"),
                split(col("value"), "\t").getItem(12).alias("called_end_lac"),
                split(col("value"), "\t").getItem(13).alias("called_start_cell"),
                split(col("value"), "\t").getItem(14).alias("called_end_cell")
            )\
            .withColumn("service_provider_id", lit(10))\
            .na.fill("N/A")
