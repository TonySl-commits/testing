# import findspark
# findspark.init()

# from pyspark.sql import SparkSession  
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# from cassandra.util import uuid_from_time
# from cassandra.cluster import Cluster
# from cassandra.concurrent import execute_concurrent

# class sparkConnection:
#     def __init__(self):
#         self.spark = self.connect_spark()

#     def connect_spark(self):
#         self.spark = SparkSession.builder.master('local')\
#                         .config("spark.executor.instances","3")\
#                         .config("spark.executor.memory","11G")\
#                         .config("spark.driver.memory","11G")\
#                         .getOrCreate()
        
#         self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#         return self.spark
    
#     def connect_spark_cassandra(self):
#         spark = SparkSession.builder.master('local[*]')\
#                     .config("spark.cassandra.connection.host", "10.1.10.66")\
#                     .config("spark.executor.instances","3")\
#                     .config("spark.executor.memory","11G")\
#                     .config("spark.driver.memory","11G")\
#                     .getOrCreate()

#         spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#         return spark
    
#     def get_spark(self):
#         return self.spark

#     def load_spark_data(self, table):
#         spark = self.connect_spark_cassandra()
#         table = self.spark.read.format("org.apache.spark.sql.cassandra")\
#                 .options(table=table, keyspace='datacrowd')\
#                 .load()
#         return table, spark
