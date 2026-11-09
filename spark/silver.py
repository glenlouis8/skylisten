from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

from pyspark.sql.functions import col

import os
from pyspark.sql.functions import sha2, concat, lit

spark = SparkSession.builder.appName("silver").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


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
