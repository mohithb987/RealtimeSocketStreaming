import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, udf, when
from config.config import config
from time import sleep
from openai import OpenAI
from groq import Groq

local_host = "127.0.0.1"

def sentiment_analysis(comment) -> str:
    if comment:
        # client = OpenAI(api_key=config['openai']['api_key'])
        client = Groq(api_key=config['groq']['api_key'])

        chat_completion = client.chat.completions.create(
            # model='gpt-3.5-turbo',
            model="llama3-8b-8192",
            messages = [
                {
                    "role": "system",
                    "content": """
                                You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRL.
                                You are to respond with one word from the option specified above, do not add any more text.

                                Here is the comment:

                                {comment}
                                """.format(comment=comment)
                }
            ]
        )

        print(chat_completion.choices[0].message.content)

        return chat_completion.choices[0].message.content
    
    return "Empty"

# to read data from socket
def start_streaming(spark):
    topic = 'customers_review'
    while True:
        try:
            print("Attempting to connect to socket...")
            stream_df = (spark.readStream.format("socket")
                         .option("host", "0.0.0.0")
                         .option("port", 9999)
                         .load()
                         )
            print("Connected to socket successfully.")
            #schema to recieve from socket and produce to Kafka cluster broker
            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            # extract each col from the batch, convert to JSON, and select every record from the col
            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))
            # query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start() # display streams on console
            
            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn('feedback', 
                        when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                        .otherwise(None)
                        )

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            print("Starting to write to Kafka...")

            query = (kafka_df.writeStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                   .option("kafka.security.protocol", config['kafka']['security.protocol'])
                   .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                   .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                    .option('kafka.ssl.endpoint.identification.algorithm', 'https')
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('topic', topic)
                   .start()
                   .awaitTermination()
                )
            print("Finished writing to Kafka.")


        except Exception as e:
            print(e)
            print('Retrying in 10 seconds... ')
            sleep(10)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName('SocketStreamingConsumer').getOrCreate()
    start_streaming(spark_conn)