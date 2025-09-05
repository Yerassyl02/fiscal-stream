import os
import re
from pyspark.sql import SparkSession, functions as F, types as T
from validators import parse_dt, valid_iin, normalize_city, normalize_address, valid_vat

spark = (
    SparkSession.builder.appName("fiscal-stream")
    .getOrCreate()
    )

spark.sparkContext.setLogLevel("WARN")

# VALIDATORS [IF YOU WANNA WORK WITH UDF]
# udf_parse_dt = F.udf(lambda s: parse_dt(s), T.TimestampType())
# udf_valid_iin = F.udf(lambda s: bool(valid_iin(s)), T.BooleanType())
# udf_city = F.udf(lambda s: normalize_city(s), T.StringType())
# udf_address = F.udf(lambda s: normalize_address(s), T.StringType())
# udf_valid_vat = F.udf(lambda s: valid_vat(s), T.StringType())

# ============================================================

event_time = F.coalesce(
    F.to_timestamp("DateTime", "yyyy-MM-dd, HH:mm:ss"),
    F.to_timestamp("DateTime", "dd.MM.yy HH:mm"),
    F.to_timestamp("DateTime", "dd/MM/yyyy HH:mm:ss")
)

iin_valid = F.col("TaxPayerIIN").rlike(r"^\d{12}$")

unvalid_endigs = ['!!!', '***', '??', ' ']

product_pattern = r"(?:{}+$)".format("|".join(re.escape(e) for e in unvalid_endigs))

city = F.when(
    (F.col("Address").isNotNull()) & (F.col("Address") != "") & (F.col("Address") != "unknown"),
    F.split(F.col("Address"), ",")[0]
).otherwise("")

address_correct = F.when(
    (F.col("Address").isNotNull()) &
    (F.col("Address") != "") &
    (F.col("Address") != "unknown"),
    F.col("Address")
)

vat_valid = F.regexp_extract(F.col("main_vat_rate"), r"(\d+)", 1).cast("int")
vat_valid = F.when((vat_valid.isNotNull()), 
                   F.concat(vat_valid.cast("string"), F.lit("%")))

qr_code_valid = F.when(
    (F.col("QRCode").isNotNull()) & 
    (F.col("QRCode") != "") & 
    (F.col("QRCode") != "битый_qr_code"),
    F.col("QRCode")
)

# ////============================================================

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
            T.StructField("Total", T.DoubleType())
        ])
    )),
    T.StructField("TotalSum", T.DoubleType()),
    T.StructField("VatRate", T.StringType()),
    T.StructField("VatSum", T.StringType()),
    T.StructField("Payment", T.StringType()),
    T.StructField("FiscalSign (ФП)", T.StringType()),
    T.StructField("OFD", T.StructType([
        T.StructField("Name", T.StringType()),
        T.StructField("Site", T.StringType()),
    ])),
    T.StructField("Address", T.StringType()),
    T.StructField("QRCode", T.StringType()),
])

# UDF DATETIME VALIDATOR
# df = (
#     json_str
#     .select(F.from_json("json", schema).alias("d"))
#     .select("d.*")
#     .withColumn("DateTime_ts", udf_parse_dt("DateTime"))
#     )

# ============================================================

df = (
    json_str
    .select(F.from_json("json", schema).alias("d"))
    .select("d.*")
    .withColumn("DateTime_ts", event_time)
    )

# ////============================================================



# RAW data to MinIO [parquet] 
# UDF DATETIME
# df_with_month = df.withColumn(
#     "DateTime_ts",
#     F.coalesce(
#         F.to_timestamp("DateTime", "yyyy-MM-dd, HH:mm:ss"),
#         F.to_timestamp("DateTime", "dd.MM.yy HH:mm"),
#         F.to_timestamp("DateTime", "dd/MM/yyyy HH:mm:ss")
#     )
# ).withColumn("year_month", F.date_format("DateTime_ts", "yyyy-MM"))

# ============================================================

df_with_month = df.withColumn(
    "DateTime_ts",
    event_time
).withColumn("year_month", F.date_format("DateTime_ts", "yyyy-MM"))

# ////============================================================


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

# VALIDATITION WITH UDF
# enriched = (
#     df
#     .withColumn("event_time", udf_parse_dt("DateTime"))
#     .withColumn("iin_valid", udf_valid_iin("TaxPayerIIN"))
#     .withColumn("address_correct", udf_address("Address"))
#     .withColumn("main_vat_rate",
#                 F.when(F.size("Items") > 0, F.col("Items")[0]["VAT"]["Rate"]).otherwise(F.lit("")))
#     .withColumn("vat_valid", udf_valid_vat("main_vat_rate"))
#     .withColumn("is_clean", 
#                 F.col("event_time").isNotNull() &
#                 F.col("iin_valid") &
#                 F.col("vat_valid").isNotNull() &
#                 (F.col("TotalSum") > 0)
#                 )
# )

# ============================================================

enriched = (
    df
    .withColumn("event_time", event_time)
    .withColumn("iin_valid", iin_valid)
    .withColumn("address_correct", address_correct)

    .withColumn("main_vat_rate",
                F.when(F.size("Items") > 0, F.col("VatRate")).otherwise(F.lit("")))
    .withColumn("vat_valid", vat_valid)

    .withColumn("qr_code_valid", qr_code_valid)
    .withColumn("is_clean", 
                F.col("event_time").isNotNull() &
                F.col("iin_valid") &
                F.col("address_correct").isNotNull() &
                F.col("vat_valid").isNotNull() &
                F.col("qr_code_valid").isNotNull() &
                (F.col("TotalSum") > 0)
                )
)

# ////============================================================

clean = (
    enriched
    .filter("is_clean")
    .select(
        "event_time",
        F.col("CompanyName").alias("company_name"),
        F.col("TaxPayerIIN").alias("tax_payer_iin"),
        F.col("KKTFactorNumber (ЗНМ)").alias("kkt_factor_number"),
        F.col("KKTRegistrationNumber (РНМ)").alias("kkt_registration_number"),
        F.col("ReceiptNumber").alias("receipt_number"),
        F.col("DateTime").alias("date_time"),
        F.col("Items").alias("items"),
        F.col("TotalSum").alias("total_sum"),
        F.col("VatRate").alias("vat_rate"),
        F.col("VatSum").alias("vat_sum"),
        F.col("Payment").alias("payment"),
        F.col("FiscalSign (ФП)").alias("fiscal_sign"),
        F.col("OFD").alias("ofd"),
        F.col("address_correct").alias("address"),
        F.col("QRCode").alias("qrcode")
    )
)

clean = clean.withColumn("upload_time", F.current_timestamp())

clean_exploded = (
    clean
    .withColumn("item", F.explode("items"))
    .select(
        "upload_time",
        "company_name",
        "tax_payer_iin",
        "kkt_factor_number",
        F.col("kkt_registration_number").cast("long"),
        F.col("receipt_number").cast("long"),
        F.col("event_time").alias("date_time"),

        # поля из Items
        F.col("item.Category").alias("category"),
        F.regexp_replace(F.col("item.Name"), product_pattern, "").alias("product_name"),
        F.col("item.UnitPrice").cast("decimal(18,2)").alias("unit_price"),
        F.col("item.Quantity").cast("long").alias("quantity"),
        F.col("item.Total").cast("decimal(18,2)").alias("total_amount_by_position"),

        F.col("total_sum").cast("decimal(18,2)").alias("total_sum"),
        F.col("vat_rate").alias("vat_rate"),
        F.col("vat_sum").cast("decimal(18,2)").alias("vat_sum"),
        "payment",
        F.col("fiscal_sign").cast("long").alias("fiscal_sign"),
        F.coalesce(F.col("OFD.Name"), F.lit("unknown")).alias("ofd_name"),
        F.coalesce(F.col("OFD.Site"), F.lit("unknown")).alias("ofd_site"),
        "address",
        F.col("qrcode").alias("qr_code")
    )
)

errors = (
    enriched
    .filter(~F.col("is_clean"))
    .withColumn("reason",
                F.concat_ws("; ",
                            F.when(F.col("event_time").isNull(), F.lit("bad_datetime")).otherwise(F.lit(None)),
                            F.when(~F.col("iin_valid"), F.lit("bad_iin")).otherwise(F.lit(None)),
                            F.when(~F.col("vat_valid").isNull(), F.lit("bad_vat")).otherwise(F.lit(None)),
                            F.when(F.col("TotalSum") <= 0, F.lit("total <= 0")).otherwise(F.lit(None))
                            )
                )
    .select(
        F.current_timestamp().alias("ts"),
        "reason",
        F.to_json(F.struct(df.columns)).alias("payload")
    )
)

# CLEANED/ERRORS DATA TO DATABASES [CLICKHOUSE, POSTGRESQL]
# CLEAN => CLICKHOUSE
clean_writes = (
    clean_exploded.writeStream
    .outputMode("append")
    .foreachBatch(lambda batch_df, epoch_id: (
        batch_df.write
        .mode("append")
        .format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/analytics")
        .option("dbtable", "fiscal_clean")
        .option("user", "chuser")
        .option("password", "chpass")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .save()
    ))
    .start()
)

spark.streams.awaitAnyTermination()