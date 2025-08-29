from pyspark.sql import SparkSession, functions as F, types as T
from validators import parse_dt, valid_iin, normalize_city, normalize_address, valid_vat

spark = (
    SparkSession.builder.appName("fiscal-stream")
    .getOrCreate()
    )

spark.sparkContext.setLogLevel("WARN")

# VALIDATORS
udf_parse_dt = F.udf(lambda s: parse_dt(s), T.TimestampType())
udf_valid_iin = F.udf(lambda s: bool(valid_iin(s)), T.BooleanType())
udf_city = F.udf(lambda s: normalize_city(s), T.StringType())
udf_address = F.udf(lambda s: normalize_address(s), T.StringType())
udf_valid_vat = F.udf(lambda s: valid_vat(s), T.StringType())

# read Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "fiscal_row")
    .option("startingOffsets", "latest")
    .load()
)

# value[b'{json}'] >> value['{json}'] >> json['{json}']
json_str = raw.select(
    F.col('value').cast('string').alias('json')
)

schema = T.StructType([
    T.StructField("CompanyName", T.StringType()),
    T.StructField("TaxPayerIIN", T.StringType()),
    T.StructField("KKTFactorNumber (ЗНМ)", T.StringType()),
    T.StructField("KKTRegistrationNumber (РНМ)", T.StringType()),
    T.StructField("ReceiptNumber", T.LongType()),
    T.StructField("DateTime", T.StringType()),
    T.StructField("Items", T.ArrayType(
        T.StructType([
            T.StructField("Category", T.StringType()),
            T.StructField("Name", T.StringType()),
            T.StructField("UnitPrice", T.DoubleType()),
            T.StructField("Quantity", T.IntegerType()),
            T.StructField("Total", T.DoubleType()),
            T.StructField("VAT", T.StructType([
                T.StructField("Rate", T.StringType()),
                T.StructField("Sum", T.DoubleType()),
            ])),
        ])
    )),
    T.StructField("TotalSum", T.DoubleType()),
    T.StructField("Payment", T.StringType()),
    T.StructField("FiscalSign (ФП)", T.StringType()),
    T.StructField("OFD", T.StructType([
        T.StructField("Rate", T.StringType()),
        T.StructField("Site", T.StringType()),
    ])),
    T.StructField("Address", T.StringType()),
    T.StructField("QRCode", T.StringType()),
])

df = (
    json_str
    .select(F.from_json("json", schema).alias("d"))
    .select("d.*")
    .withColumn("DateTime_ts", udf_parse_dt("DateTime"))
    )

# RAW data to MinIO [parquet]
df_with_month = df.withColumn(
    "DateTime_ts",
    F.coalesce(
        F.to_timestamp("DateTime", "yyyy-MM-dd, HH:mm:ss"),
        F.to_timestamp("DateTime", "dd.MM.yy HH:mm"),
        F.to_timestamp("DateTime", "dd/MM/yyyy HH:mm:ss")
    )
).withColumn("year_month", F.date_format("DateTime_ts", "yyyy-MM"))


raw_sink = (
    df_with_month
    .withColumn("ingest_time", F.current_timestamp())
    .writeStream
    .format("parquet")
    .option("checkpointLocation", "s3a://raw/_cnk/")
    .option("path", "s3a://raw/fiscal/")
    .partitionBy("year_month")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()