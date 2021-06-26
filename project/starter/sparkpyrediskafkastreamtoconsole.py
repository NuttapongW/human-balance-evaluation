from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType


redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", StringType())
                    ]
                )
            )
        )
    ]
)

customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

customerRiskSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder.appName("spark-app").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafkaRedisRawDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

kafkaRedisDF = kafkaRedisRawDF\
    .selectExpr(
        "CAST(key AS STRING) key",
        "CAST(value AS STRING) value"
    )

kafkaRedisDF\
    .withColumn("redisSortedSet", from_json("value", redisMessageSchema))\
    .select(col("redisSortedSet.*"))\
    .createOrReplaceTempView("RedisSortedSet")
redisDataDF = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")
redisDataDF\
    .withColumn("unbase", unbase64(redisDataDF.encodedCustomer).cast("string"))\
    .withColumn("customer", from_json("unbase", customerSchema))\
    .select(col("customer.*"))\
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark\
    .sql("select email, birthDay from CustomerRecords where email is not null AND birthDay is not null")

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear")
)
emailAndBirthYearStreamingDF\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .start()\
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct
