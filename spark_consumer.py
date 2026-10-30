import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pyspark

load_dotenv()

spark = (
    SparkSession.builder.appName("usdc_consumer")
    .config(
        "spark.jars.packages",
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
schema = StructType([
StructField("tx_hash", StringType()),
StructField("log_index", StringType()),
StructField("block_number", StringType()),
StructField("block_hash", StringType()),
StructField("from_wallet", StringType()),
StructField("to_wallet", StringType()),
StructField("amount_usdc", StringType()),
])

raw = (
spark.readStream.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "usdc_transfers")
.option("startingOffsets", "earliest")
.load()
)

parsed = raw.select(
F.col("value").cast("string").alias("raw_value"),
F.from_json(F.col("value").cast("string"), schema).alias("t"),
F.col("partition").alias("kafka_partition"),
F.col("offset").alias("kafka_offset"),
).select("raw_value", "t.*", "kafka_partition", "kafka_offset")

cleaned = (
parsed
.withColumn("log_index", F.col("log_index").cast("int"))
.withColumn("block_number", F.col("block_number").cast("long"))
.withColumn("amount_usdc", F.col("amount_usdc").cast("decimal(38,6)"))
.withColumn("from_wallet", F.lower("from_wallet"))
.withColumn("to_wallet", F.lower("to_wallet"))
)

validated = cleaned.withColumn(
"reason",
F.when(~F.col("tx_hash").rlike("^0x[0-9a-fA-F]{64}$"), "bad tx hash")
.when(F.col("block_number").isNull(), "bad block number")
.when(~F.col("from_wallet").rlike("^0x[0-9a-f]{40}$"), "bad from wallet")
.when(~F.col("to_wallet").rlike("^0x[0-9a-f]{40}$"), "bad to wallet")
.when(F.col("amount_usdc").isNull() | (F.col("amount_usdc") < 0), "bad amount"),
)


def get_connection():
    return snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key_file=os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="RAW",
    role=os.getenv("SNOWFLAKE_ROLE"),
    )


def write_batch(batch_df, batch_id):
    batch_df = batch_df.withColumn("loaded_at", F.current_timestamp())

    clean = batch_df.filter(F.col("reason").isNull()).select(
    "tx_hash", "log_index", "block_number", "block_hash",
    "from_wallet", "to_wallet", "amount_usdc",
    "kafka_partition", "kafka_offset", "loaded_at",
    ).toPandas()

    rejected = batch_df.filter(F.col("reason").isNotNull()).select(
    "raw_value", "reason", "kafka_partition", "kafka_offset", "loaded_at",
    ).toPandas()

    conn = get_connection()
    if len(clean) > 0:
        clean.columns = [c.upper() for c in clean.columns]
    write_pandas(conn, clean, "USDC_TRANSFERS_CLEAN", use_logical_type=True)
    if len(rejected) > 0:
     rejected.columns = [c.upper() for c in rejected.columns]
     write_pandas(conn, rejected, "USDC_TRANSFERS_REJECTED", use_logical_type=True)
    conn.close()

    print(f"Batch {batch_id}: {len(clean)} clean, {len(rejected)} rejected")


query = (
validated.writeStream
.foreachBatch(write_batch)
.option("checkpointLocation", "./checkpoints/usdc")
.trigger(processingTime="10 seconds")
.start()
)

query.awaitTermination()