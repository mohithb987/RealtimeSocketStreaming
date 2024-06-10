import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col

local_host = "127.0.0.1"

# to read data from socket
def start_streaming(spark_conn):
    try:
        stream_df = spark_conn.readStream.format("socket")\
                    .option("host", local_host)\
                    .option("port", 9999)\
                    .load()
        
        #schema to recieve from socket and produce to Kafka cluster broker
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType()),
        ])

        # extract each col from the batch, convert to JSON, and select every record from the col
        stream_df = stream_df.select( from_json(col('value'), schema).alias("data") ).select(("data.*")) 
        query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start() # display streams on console
        
        query.awaitTermination()

    except Exception as e:
        print(e)

if __name__ == '__main__':
    spark_conn = SparkSession.builder.appName('SocketStreamingConsumer').getOrCreate()
    start_streaming(spark_conn)