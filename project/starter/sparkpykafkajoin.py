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

spark = SparkSession\
    .builder\
    .appName("spark-app")\
    .getOrCreate()

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

kafkaStediEventsDF\
    .withColumn("stediEvents", from_json("value", customerRiskSchema))\
    .select(col("stediEvents.*"))\
    .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

emailAndBirthYearStreamingDF.join(
    customerRiskStreamingDF,
    expr("""
    email = customer
    """)
)\
    .selectExpr(
    "CAST(email as string) key",
    "CAST(to_json(struct(*)) as string) value"
) \
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("topic", "kafka.risk")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
