from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import  pyspark.sql.types as T

my_conf=SparkConf()
my_conf.set("spark.master","local[*]")
my_conf.set("spark.app.name","kafkaStreaming")

spark=SparkSession.builder\
    .config(conf=my_conf)\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3")\
    .getOrCreate()



#giving schema for future input json string that we will get as an input via interactions
schema_def= T.StructType([T.StructField("user_id", T.IntegerType()),
                          T.StructField("property_id", T.IntegerType()),
                          T.StructField("date", T.TimestampType()),
                          T.StructField("event", T.StringType())])

#reading Kafka Topic
kafka_df=spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","Events")\
    .option("startingOffsets","earliest")\
    .load()


#giving structure to JSON string
value_df=kafka_df.select(F.from_json(F.col("value").cast("string"), schema_def).alias("value"))
exploded_df=value_df.select(F.col("value.*")) #to extract all columns at root level



#logic to aggregate all user events which are interactions
user_analysis_df=exploded_df.selectExpr("user_id")\
    .where(F.expr("event='Interaction'"))\
    .groupBy("user_id")\
    .agg(F.count("user_id").alias("interactions_count"))

# ===================================================PAID USER CODE=============================================#
#paid_user_push_notification_logic
paid_user_df=user_analysis_df.select(F.col("user_id"),F.col("interactions_count"))\
                            .where(F.expr("interactions_count>=9"))



#we will pass the JSON string to Kafka as key value pair. Below logic ensures the same
paid_user_target_df=paid_user_df.selectExpr("cast(user_id as string) as key",
                            """to_json(named_struct('user',user_id,'interactions_count',interactions_count)) as value
                            """)
# kafka_target_df.printSchema()


#writing paid_user IDs to Kafka topic for notifications and actions
paid_user_writer_query=paid_user_target_df\
    .writeStream\
    .queryName("paid-user-writer")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","paid_user")\
    .outputMode("update")\
    .option("checkpointLocation","chk_pnt_dir1")\
    .start()
#================================================PAID USER CODE ENDS=======================================#

#================================================POSSIBLE BROKER=========================================#

possible_broker_df=exploded_df\
    .groupBy(F.window(F.col("date"),"1 minute"),F.col("user_id"))\
    .agg(F.count("user_id").alias("threat_user_interactions"))\
    .where(F.expr("threat_user_interactions>10"))

#we will pass the JSON string to Kafka as key value pair. Below logic ensures the same
possible_broker_target_df=possible_broker_df.selectExpr("cast(window.start as string) as key",\
                            """to_json(named_struct('window.start',window.start,'user_id',user_id,\
                            'threat_user_interactions',threat_user_interactions)) as value""")

possible_broker_writer_query=possible_broker_target_df\
    .writeStream\
    .queryName("possible-broker-writer")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","possible_broker")\
    .outputMode("update")\
    .option("checkpointLocation","chk_pnt_dir2")\
    .start()
#================================================POSSIBLE_BROKER_CODE_ENDS================================#

#=================================================PROPERTY_CODE============================================#
property_analysis_df=exploded_df.selectExpr("property_id")\
    .where(F.expr("event='Interaction'"))\
    .groupBy("property_id")\
    .agg(F.count("property_id").alias("property_interactions_count"))



property_status_df=property_analysis_df.select(F.col("property_id"),F.col("property_interactions_count"))\
    .where(F.expr("property_interactions_count>5"))

#we will pass the JSON string to Kafka as key value pair. Below logic ensures the same
property_status_target_df=property_status_df.selectExpr("cast(property_id as string) as key",
                            """to_json(named_struct('property',property_id,'property_interactions_count',property_interactions_count)) as value\
                            """)



property_status_writer_query=property_status_target_df\
    .writeStream\
    .queryName("property-status-writer")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","property_status")\
    .outputMode("update")\
    .option("checkpointLocation","chk_pnt_dir3")\
    .start()
#========================================================POSSIBLE BROKER CODE ENDS==================================================#
#multiple streams are used
spark.streams.awaitAnyTermination()