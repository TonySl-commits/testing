# from vcis.databases.cassandra_spark.cassandra_spark_tools import CassandraSparkTools

# cassandra_spark_tools = CassandraSparkTools()

# cassandra_spark_tools.get_spark_connection(passed_connection_host = '10.1.10.110',local=False)
from pyspark.sql import SparkSession

# Define your configuration parameters
spark_master_url = "spark://10.10.10.70:7077"
AppName = "cotraveler"
mem_size = "20G"
partitions_nb = "200"
parallelism_nb = "10"

cassandra_hosts = "10.10.10.70"
cassandra_username = "cassandra"
cassandra_password = "cassandra"

msg = f"Cassandra target hosts: {cassandra_hosts}"
print(msg)
# logger.info(msg)

# Add files to the Spark classpath using --files during spark-submit
#os_command = f"spark-submit --master {spark_master_url} --files {submit_files_path} csvprq_to_cassandra_v3.ipynb"
#os.system(os_command)

# Create Spark session without configuring files in SparkSession
spark = SparkSession.builder.master(spark_master_url) \
    .config("spark.cassandra.connection.host", cassandra_hosts) \
    .config("spark.driver.memory", mem_size) \
    .config("spark.executor.memory", mem_size) \
    .config("spark.sql.shuffle.partitions", partitions_nb) \
    .config("spark.default.parallelism", parallelism_nb) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.catalog.casscatalog", "com.datastax.spark.catalog.CatalogConnector") \
    .config("spark.cassandra.auth.username", cassandra_username) \
    .config("spark.cassandra.auth.password", cassandra_password) \
    .appName(AppName) \
    .getOrCreate()

msg = f"Spark started for [{AppName}]"
print(msg)
spark.stop()
# logger.info(msg)