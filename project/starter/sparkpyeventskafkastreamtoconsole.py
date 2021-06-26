from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName("spark-app").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
kafkaStediEventsRawDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()

kafkaStediEventsDF = kafkaStediEventsRawDF\
    .selectExpr(
        "CAST(key AS STRING) key",
        "CAST(value AS STRING) value"
    )

customerRiskSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType())
    ]
)
kafkaStediEventsDF\
    .withColumn("stediEvents", from_json("value", customerRiskSchema))\
    .select(col("stediEvents.*"))\
    .createOrReplaceTempView("CustomerRisk")

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")
customerRiskStreamingDF\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .start()\
    .awaitTermination()
