"""
Silver job: bronze (raw hourly files) -> silver (clean Parquet).

Run for one hour of the day (UTC), for example:
    spark-submit silver.py 2026-11-07-14

What it does, in order:
  1. read the raw file for that hour, plus the next hour's file (late arrivals)
  2. split into new posts and delete notes, drop everything else
  3. hash the author id (privacy)
  4. keep English posts that have text
  5. remove posts that were deleted
  6. keep only posts that happened in the requested hour
  7. write posts + the list of removed posts as Parquet
"""
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, date_format, exists, lit, sha2, trim
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

# ---------------------------------------------------------------- setup
spark = SparkSession.builder.appName("silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # hide the INFO noise

# Cut hours in UTC, same as the bronze file names.
spark.conf.set("spark.sql.session.timeZone", "UTC")
# When writing, only replace the hour folders we touch, not the whole table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------------------------------------------------------- input
# The hour to process, given on the command line, like 2026-11-07-14.
hour = datetime.strptime(sys.argv[1], "%Y-%m-%d-%H")
nxt = hour + timedelta(hours=1)
# Posts from hour H can arrive late and sit in the file of hour H+1.
paths = [f"/data/bronze/posts-{h:%Y-%m-%d-%H}.jsonl.gz" for h in (hour, nxt)]

# We only list the fields we need. Spark ignores the rest of the JSON.
schema = StructType(
    [
        StructField("did", StringType()),  # who (user id)
        StructField("time_us", LongType()),  # when it happened, in microseconds
        StructField("kind", StringType()),  # commit / account / identity
        StructField(
            "commit",
            StructType(
                [
                    StructField("operation", StringType()),  # create / delete
                    StructField("rkey", StringType()),  # post id (needs did too)
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

df = spark.read.schema(schema).json(paths)

# ---------------------------------------------------------------- split
# Pile 1: new posts. Pile 2: delete notes. Account/identity events are dropped.
posts = df.filter((col("kind") == "commit") & (col("commit.operation") == "create"))
deletes = df.filter((col("kind") == "commit") & (col("commit.operation") == "delete"))
print("posts:", posts.count())
print("deletes:", deletes.count())

# ---------------------------------------------------------------- privacy
# Hash the user id with a secret salt. One-way: nobody can get the real id back,
# but the same user always gets the same hash, so posts and deletes still match.
salt = os.environ["HASH_SALT"]
posts = posts.withColumn("author_hash", sha2(concat(lit(salt), col("did")), 256))
deletes = deletes.withColumn("author_hash", sha2(concat(lit(salt), col("did")), 256))

# ---------------------------------------------------------------- clean
# Keep a post if any of its language tags starts with "en" (en, en-GB, en-US).
# Posts with no language tag are dropped.
english = posts.filter(exists("commit.record.langs", lambda x: x.startswith("en")))
print("english:", english.count())

# Drop posts with empty text (for example image-only posts).
with_text = english.filter(trim(col("commit.record.text")) != "")
print("with text:", with_text.count())

# ---------------------------------------------------------------- apply deletes
# left_anti join = keep posts that have NO matching delete note.
# A post is identified by author_hash + rkey together.
d = deletes.select("author_hash", col("commit.rkey").alias("rkey"))
p = with_text.withColumn("rkey", col("commit.rkey"))
kept = p.join(d, on=["author_hash", "rkey"], how="left_anti")
print("after deletes:", kept.count())

# ---------------------------------------------------------------- shape
# Flat, clean columns. The real user id is gone, only the hash stays.
silver = kept.select(
    "author_hash",
    "rkey",
    col("commit.record.text").alias("text"),
    col("commit.record.createdAt").alias("created_at"),
    col("commit.record.langs").alias("langs"),
    (col("time_us") / 1000000).cast("timestamp").alias("event_time"),
)

# Folders are named by the post's own time (event time), not by the file name.
out = silver.withColumn("date", date_format("event_time", "yyyy-MM-dd")).withColumn(
    "hour", date_format("event_time", "HH")
)
# This job owns ONE hour folder. Keep only that hour so jobs never overwrite
# each other's folders.
out = out.filter((col("date") == f"{hour:%Y-%m-%d}") & (col("hour") == f"{hour:%H}"))
out.write.mode("overwrite").partitionBy("date", "hour").parquet("/silver/posts")

# ---------------------------------------------------------------- removed list
# Every delete note is saved as author_hash + rkey + when. Later, a cleanup step
# uses this list to remove posts from older silver folders.
removed = (
    deletes.select(
        "author_hash",
        col("commit.rkey").alias("rkey"),
        (col("time_us") / 1000000).cast("timestamp").alias("deleted_at"),
    )
    .withColumn("date", date_format("deleted_at", "yyyy-MM-dd"))
    .withColumn("hour", date_format("deleted_at", "HH"))
)
# Same rule: this job owns only the delete notes that happened in its hour.
removed = removed.filter(
    (col("date") == f"{hour:%Y-%m-%d}") & (col("hour") == f"{hour:%H}")
)
removed.write.mode("overwrite").partitionBy("date", "hour").parquet("/silver/removed")
print("removed list rows:", removed.count())
