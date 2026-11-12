"""
Cleanup job: remove posts from silver that their authors deleted later.

silver.py only sees the delete notes inside its own two files. A delete can
arrive hours later, after an old hour was already written. This job checks the
whole removed list against all of silver and rewrites only the folders that
contain a deleted post.

Run (no arguments):
    spark-submit apply_deletes.py
"""
import shutil

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("apply_deletes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Keep folder-name columns as text. Otherwise hour=09 is read as the number 9,
# and the folder swap below looks for hour=9, which does not exist.
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

POSTS = "/silver/posts"
FIXED = "/silver/posts_fixed"  # side folder for the cleaned copy

posts = spark.read.parquet(POSTS)
removed = spark.read.parquet("/silver/removed").select("author_hash", "rkey")

# ---------------------------------------------------------------- find bad posts
# left_semi = keep posts that DO have a matching delete note.
matches = posts.join(removed, on=["author_hash", "rkey"], how="left_semi")
print("posts in silver:", posts.count())
print("should be removed:", matches.count())

# Which date/hour folders contain at least one deleted post?
# collect() brings the small list to Python now, before we touch any files.
bad_folders = matches.select("date", "hour").distinct().collect()
print("folders to rewrite:", len(bad_folders))

if bad_folders:
    # ------------------------------------------------------------ write clean copy
    # Take only the affected folders, drop the deleted posts, write to a side
    # folder. We cannot overwrite a folder we are reading from.
    affected = posts.join(
        matches.select("date", "hour").distinct(), on=["date", "hour"], how="left_semi"
    )
    cleaned = affected.join(removed, on=["author_hash", "rkey"], how="left_anti")
    cleaned.write.mode("overwrite").partitionBy("date", "hour").parquet(FIXED)

    # ------------------------------------------------------------ swap folders
    for r in bad_folders:
        old = f"{POSTS}/date={r.date}/hour={r.hour}"
        new = f"{FIXED}/date={r.date}/hour={r.hour}"
        shutil.rmtree(old)
        shutil.move(new, old)
    shutil.rmtree(FIXED)  # remove the empty side folder

# ---------------------------------------------------------------- check again
spark.catalog.refreshByPath(POSTS)
after = spark.read.parquet(POSTS)
left = after.join(removed, on=["author_hash", "rkey"], how="left_semi").count()
print("posts in silver after:", after.count())
print("still should be removed:", left)
