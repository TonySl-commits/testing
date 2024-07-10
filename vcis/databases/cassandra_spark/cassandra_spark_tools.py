##############################################################################################################################
    # Imports
##############################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import glob
import os
from vcis.utils.properties import CDR_Properties
import pickle
class CassandraSparkTools:
    def __init__(self, verbose=False):
        self.properties = CDR_Properties()
        self.verbose = verbose
        self.spark_session_configs = []
##############################################################################################################################
    # Spark Connection
##############################################################################################################################
    def get_spark_connection(self,passed_app_name:str = 'cotraveler',passed_connection_host: str = "10.1.10.66" , passed_instances: int = 2 , passed_executor_memory: int = 10 , passed_driver_memory: int = 10,local =True):
        if local:
            spark = SparkSession.builder.master('local[*]')\
                .appName(passed_app_name) \
                .config("spark.cassandra.connection.host", passed_connection_host )\
                .config("spark.executor.instances",f'{passed_instances}')\
                .config("spark.executor.memory",f'{passed_executor_memory}G')\
                .config("spark.driver.memory",f'{passed_driver_memory}G')\
                .getOrCreate()
            spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        
        elif local==None:
            jar_dir = "/u01/cassandra/spark-3.3.0/jars"
            
            # Get a list of all JAR files in the directory
            jar_files = glob.glob(os.path.join(jar_dir, "*.jar"))
            
            # Join the list into a single string with commas separating each path
            jar_paths = ",".join(jar_files)
            print(jar_paths)
            spark = SparkSession.builder.master('spark://10.1.10.110:7077')\
            .config("spark.cassandra.connection.host", passed_connection_host )\
            .config("spark.executor.instances",f'{passed_instances}')\
            .config("spark.executor.memory",f'{passed_executor_memory}G')\
            .config("spark.driver.memory",f'{passed_driver_memory}G')\
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
            .config("spark.jars", jar_paths)\
            .appName('CDR_Trace')\
            .getOrCreate()

        else:
            spark = SparkSession.builder.master('spark://10.10.10.70:7077')\
                .appName(passed_app_name) \
                .config("spark.cassandra.connection.host", passed_connection_host )\
                .config("spark.executor.instances",f'{passed_instances}')\
                .config("spark.executor.memory",f'{passed_executor_memory}G')\
                .config("spark.driver.memory",f'{passed_driver_memory}G')\
                .getOrCreate()
                
        self.spark_session_configs.append((spark.sparkContext.applicationId, {'appName': 'cotraveler', 'master': 'local'}))
        print(self.spark_session_configs)
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")    
        return spark
           
##############################################################################################################################
     # Fucntion to load spark table
##############################################################################################################################
    def get_spark_data(self, passed_table_name:str = None, passed_keyspace:str = 'datacrowd',passed_connection_host: str = '10.10.10.101',local = True):
        spark = self.get_spark_connection(passed_connection_host = passed_connection_host,local = local)
        table = spark.read.format("org.apache.spark.sql.cassandra").options(table=passed_table_name, keyspace= passed_keyspace).load()
        return table ,spark
    
##############################################################################################################################
        # Fucntion to get imsi history
##############################################################################################################################
    def get_device_history_imsi_spark(self,
                             imsi_id:str,
                             start_date:str ,
                             end_date:str ,
                             server:str = '10.1.10.66',
                             local=True
                             ):
        df ,spark= self.get_spark_data(passed_connection_host = server, passed_table_name = 'loc_location_cdr_main_new',local=local)
        
        df_filtered = df.filter((df.imsi_id == imsi_id) & (col('usage_timeframe').between(start_date, end_date)))
        df_filtered_panads = df_filtered.toPandas()
        spark.sparkContext.stop()
        spark.stop()
        return df_filtered_panads
    
##############################################################################################################################
        # Fucntion to get imsi history
##############################################################################################################################
    def insert_to_cassandra_using_spark(self, df, passed_table_name,passed_connection_host: str = '10.1.10.66',local=True):
        spark = self.get_spark_connection(passed_connection_host=passed_connection_host,local=local)
        if(type(df) == pd.DataFrame):
            df = spark.createDataFrame(df)
            
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=passed_table_name, keyspace="datacrowd") \
            .mode("append") \
            .save()
        return df