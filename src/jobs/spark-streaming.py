import pyspark
from pyspark.sql import SparkSession

local_host = "127.0.0.1"
# to read data from socket
def start_streaming(spark_conn):
    stream_df = spark_conn.readStream.format("socket")\
                 .option("host", local_host)\
                 .option("port", 9999)\
                 .load()
    
    query = stream_df.writeStream.outputMode("append").format("console").start() # display streams on console
    query.awaitTermination()

if __name__ == '__main__':
    spark_conn = SparkSession.builder.appName('SocketStreamingConsumer').getOrCreate()
    start_streaming(spark_conn)