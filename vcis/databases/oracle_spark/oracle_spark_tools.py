from vcis.utils.utils import CDR_Properties , CDR_Utils
import pandas as pd
from pyspark.sql.types import BinaryType
from pyspark.sql import functions as F
from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

class OracleSparkTools():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.cassandra_spark_tools = CassandraSparkTools()
    
    # def create_temp_table(self, df, table_name, table_id):
    #     pd.DataFrame.iteritems = pd.DataFrame.items
    #     spark = self.sparkConnection.get_spark()
    #     df = spark.createDataFrame(df)
    #     df.write.format('jdbc').options(
    #         url=f'jdbc:oracle:thin:@{self.properties.host_name}:{self.properties.port}/{self.properties.sid}',
    #         driver='oracle.jdbc.driver.OracleDriver',
    #         dbtable=f'SSDX_TMP.{table_name}_{table_id}',
    #         user=f'{self.properties.username}',
    #         password=f'{self.properties.password}').mode('overwrite').save()
    #     spark.stop()
    #     print ("Data inserted successfully")
    def create_temp_table(self, df, table_name, table_id ,clob_columns_name,server:str ="10.1.10.66" ):
        pd.DataFrame.iteritems = pd.DataFrame.items
        spark = self.cassandra_spark_tools.get_spark_connection(passed_connection_host =server)
        df = spark.createDataFrame(df)
        # print(df.select(clob_columns_name).take(1)[0])
        try:
            df = df.withColumn(clob_columns_name, F.col(clob_columns_name).cast("string"))
            # print(df.select(clob_columns_name).take(1)[0])
            df = df.withColumn(clob_columns_name, F.col(clob_columns_name).cast(BinaryType()))
            # print(df.select(clob_columns_name).take(1)[0])
        except Exception as e:
            print('CDR_TRACE ERROR :',e)
            pass
        df.write.format('jdbc').options(
            url=f'jdbc:oracle:thin:@{self.properties.oracle_host_name}:{self.properties.oracle_port}/{self.properties.oracle_key_space}',
            driver='oracle.jdbc.driver.OracleDriver',
            dbtable=f'SSDX_TMP.{table_name}_{table_id}',
            user=f'{self.properties.oracle_username}',
            password=f'{self.properties.oracle_password}').mode('overwrite').save()
        spark.stop() 
        print("Data inserted successfully")
  

    def save_aoi_confidence_analysis_results(self, df, table_name, table_id):
        spark = self.cassandra_spark_tools.get_spark_connection()
        df = spark.createDataFrame(df)

        df.write.format('jdbc').options(
            url=f'jdbc:oracle:thin:@{self.properties.oracle_host_name}:{self.properties.oracle_port}/{self.properties.oracle_key_space}',
            driver='oracle.jdbc.driver.OracleDriver',
            dbtable=f'SSDX_TMP.{table_name}_{table_id}',
            user=f'{self.properties.oracle_username}',
            password=f'{self.properties.oracle_password}').mode('overwrite').save()
        spark.stop() 
        print("INFO:   AOI Confidence Analysis Results inserted successfully")

    def insert_into_oracle(self,df,spark,table_name,table_id):
        pd.DataFrame.iteritems = pd.DataFrame.items
        spark = self.cassandra_spark_tools.get_spark_connection()
        try:
            df = spark.createDataFrame(df)
            df.write.jdbc(
                url=f'jdbc:oracle:thin:@{self.properties.oracle_host_name}:{self.properties.oracle_port}/{self.properties.oracle_key_space}',
                table=f"SSDX_TMP.{table_name}_{table_id}",
                mode='append',
                properties={'user': f'{self.properties.oracle_username}',
                            'password': f'{self.properties.oracle_password}',
                            'driver': 'oracle.jdbc.OracleDriver'}
            )
            print ("Data inserted successfully")
        except:
            print("No data to insert")
            pass
        spark.stop()    
    # def insert_to_oracle(self,df,spark,unique_number):
    #     pd.DataFrame.iteritems = pd.DataFrame.items
    #     spark = self.cassandra_spark_tools.get_spark_connection()
    #     df['bts_cell_id'] = range(unique_number, unique_number +len(df))
    #     df = spark.createDataFrame(df)
    #     df.write.jdbc(
    #         url='jdbc:oracle:thin:@10.1.10.64:1522/vdcdb',
    #         table='SSDX_TMP.TECH_CO_TRAVELER_TABLE_723',
    #         mode='append',
    #         properties={'user': 'ssdx_eng',
    #                     'password': 'ssdx_eng',
    #                     'driver': 'oracle.jdbc.OracleDriver'}
    #     )
    #     print ("Data inserted successfully")