from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from pyspark.sql.functions import exists
from pyspark.sql.functions import col
import os
from pyspark.sql.functions import sha2, concat, lit
from pyspark.sql.functions import date_format
from pyspark.sql.functions import trim

spark = SparkSession.builder.appName("silver").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


schema = StructType(
    [
        StructField("did", StringType()),
        StructField("time_us", LongType()),
        StructField("kind", StringType()),
        StructField(
            "commit",
            StructType(
                [
                    StructField("operation", StringType()),
                    StructField("rkey", StringType()),
                    StructField(
                        "record",
                        StructType(
                            [
                                StructField("text", StringType()),
                                StructField("createdAt", StringType()),
                                StructField("langs", ArrayType(StringType())),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

df = spark.read.schema(schema).json("/data/bronze/posts-2026-11-07-15.jsonl.gz")

posts = df.filter((col("kind") == "commit") & (col("commit.operation") == "create"))
print("posts:", posts.count())


deletes = df.filter((col("kind") == "commit") & (col("commit.operation") == "delete"))
print("deletes:", deletes.count())


salt = os.environ["HASH_SALT"]

posts = posts.withColumn("author_hash", sha2(concat(lit(salt), col("did")), 256))
deletes = deletes.withColumn("author_hash", sha2(concat(lit(salt), col("did")), 256))

posts.select("author_hash").show(3, truncate=False)

posts.select("commit.record.langs").show(10, truncate=False)


english = posts.filter(exists("commit.record.langs", lambda x: x.startswith("en")))
print("english:", english.count())


with_text = english.filter(trim(col("commit.record.text")) != "")
print("with text:", with_text.count())


d = deletes.select("author_hash", col("commit.rkey").alias("rkey"))
p = with_text.withColumn("rkey", col("commit.rkey"))

kept = p.join(d, on=["author_hash", "rkey"], how="left_anti")
print("after deletes:", kept.count())


silver = kept.select(
    "author_hash",
    "rkey",
    col("commit.record.text").alias("text"),
    col("commit.record.createdAt").alias("created_at"),
    col("commit.record.langs").alias("langs"),
    (col("time_us") / 1000000).cast("timestamp").alias("event_time"),
)
silver.printSchema()


out = silver.withColumn("date", date_format("event_time", "yyyy-MM-dd")).withColumn(
    "hour", date_format("event_time", "HH")
)
out.write.mode("overwrite").partitionBy("date", "hour").parquet("/silver/posts")

removed = (
    deletes.select(
        "author_hash",
        col("commit.rkey").alias("rkey"),
        (col("time_us") / 1000000).cast("timestamp").alias("deleted_at"),
    )
    .withColumn("date", date_format("deleted_at", "yyyy-MM-dd"))
    .withColumn("hour", date_format("deleted_at", "HH"))
)
removed.write.mode("overwrite").partitionBy("date", "hour").parquet("/silver/removed")
print("removed list rows:", removed.count())
