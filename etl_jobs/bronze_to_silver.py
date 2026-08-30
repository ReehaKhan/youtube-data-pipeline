import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


# ============================================================
# 1. GET GLUE JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_DATABASE",
        "BRONZE_TABLE",
        "SILVER_OUTPUT_PATH"
    ]
)


# ============================================================
# 2. STORE PARAMETERS
# ============================================================

job_name = args["JOB_NAME"]
bronze_database = args["BRONZE_DATABASE"]
bronze_table = args["BRONZE_TABLE"]
silver_output_path = args["SILVER_OUTPUT_PATH"]


# ============================================================
# 3. INITIALIZE SPARK / GLUE
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(job_name, args)


# ============================================================
# 4. ENABLE DYNAMIC PARTITION OVERWRITE
# ============================================================

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


# ============================================================
# 5. READ BRONZE DATA FROM GLUE DATA CATALOG
# ============================================================

print("================================================")
print("READING BRONZE DATA")
print("================================================")

bronze_dynamic_frame = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=bronze_database,
        table_name=bronze_table
    )
)
bronze_df = bronze_dynamic_frame.toDF()

print("Bronze schema:")
bronze_df.printSchema()


# ============================================================
# 6. EXPLODE THE VIDEOS ARRAY
# ============================================================
#
# Bronze structure:
#
# videos
#   ├── video 1
#   ├── video 2
#   ├── ...
#   └── video 50
#
# explode() converts this into individual rows.
#
# ============================================================

print("================================================")
print("EXPLODING VIDEOS ARRAY")
print("================================================")

silver_df = bronze_df.select(
    F.explode("videos").alias("video"),
    F.col("country").alias("country_code"),
    F.col("date").alias("ingestion_date")
)


# ============================================================
# 7. EXTRACT VIDEO FIELDS
# ============================================================

print("================================================")
print("EXTRACTING VIDEO FIELDS")
print("================================================")

silver_df = silver_df.select(

    # --------------------------------------------------------
    # Video identifiers
    # --------------------------------------------------------

    F.col("video.video_id").alias("video_id"),
    F.col("video.channel_id").alias("channel_id"),


    # --------------------------------------------------------
    # Channel information
    # --------------------------------------------------------

    F.col("video.channel_name").alias("channel_name"),


    # --------------------------------------------------------
    # Video information
    # --------------------------------------------------------

    F.col("video.title").alias("title"),
    F.col("video.description").alias("description"),


    # --------------------------------------------------------
    # Publishing information
    # --------------------------------------------------------

    F.col("video.published_at").alias("published_at"),


    # --------------------------------------------------------
    # Category
    # --------------------------------------------------------

    F.col("video.category_id").alias("category_id"),
    F.col("video.category_name").alias("category_name"),


    # --------------------------------------------------------
    # Content information
    # --------------------------------------------------------

    F.col("video.duration").alias("duration"),
    F.col("video.definition").alias("definition"),
    F.col("video.caption").alias("caption"),


    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    F.col("video.view_count").alias("view_count"),
    F.col("video.like_count").alias("like_count"),
    F.col("video.comment_count").alias("comment_count"),


    # --------------------------------------------------------
    # Country
    # --------------------------------------------------------

    F.col("country_code"),
    F.col("video.country_name").alias("country_name"),


    # --------------------------------------------------------
    # Ranking
    # --------------------------------------------------------

    F.col("video.rank").alias("rank"),


    # --------------------------------------------------------
    # Pipeline metadata
    # --------------------------------------------------------

    F.col("video.ingestion_timestamp").alias("ingestion_timestamp"),
    F.col("ingestion_date")
)


# ============================================================
# 8. CONVERT DATA TYPES
# ============================================================

print("================================================")
print("CONVERTING DATA TYPES")
print("================================================")

silver_df = (

    silver_df

    # --------------------------------------------------------
    # Statistics → BIGINT
    # --------------------------------------------------------

    .withColumn("view_count", F.col("view_count").cast(LongType()))
    .withColumn("like_count", F.col("like_count").cast(LongType()))
    .withColumn("comment_count", F.col("comment_count").cast(LongType()))


    # --------------------------------------------------------
    # Rank → INTEGER
    # --------------------------------------------------------

    .withColumn("rank", F.col("rank").cast(IntegerType()))


    # --------------------------------------------------------
    # timestamp
    # --------------------------------------------------------

    .withColumn("published_at", F.to_timestamp("published_at"))
    .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))


    # --------------------------------------------------------
    # date
    # --------------------------------------------------------

    .withColumn("ingestion_date", F.to_date("ingestion_date"))
)


# ============================================================
# 9. CONVERT YOUTUBE DURATION TO SECONDS
# ============================================================

print("================================================")
print("CREATING DURATION_SECONDS")
print("================================================")


hours = F.regexp_extract(F.col("duration"), r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", 1)
minutes = F.regexp_extract(F.col("duration"), r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", 2)
seconds = F.regexp_extract(F.col("duration"), r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", 3)


silver_df = silver_df.withColumn(
    "duration_seconds",
    (
        F.coalesce(hours.cast("long"), F.lit(0)) * 3600
        +
        F.coalesce(minutes.cast("long"), F.lit(0)) * 60
        +
        F.coalesce(seconds.cast("long"), F.lit(0))
    )
)


# ============================================================
# 10. REMOVE DUPLICATE OBSERVATIONS
# ============================================================


print("================================================")
print("REMOVING DUPLICATES")
print("================================================")

silver_df = silver_df.dropDuplicates([
    "video_id",
    "country_code",
    "ingestion_date"
])


# ============================================================
# 11. REORDER AND DROP COLUMNS 
# ============================================================
# Drop the following col that is redundant:
#       - duration
# ============================================================

silver_df = silver_df.select(
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "description",
    "published_at",
    "category_id",
    "category_name",
    "duration_seconds",
    "definition",
    "caption",
    "view_count",
    "like_count",
    "comment_count",
    "country_code",
    "country_name",
    "rank",
    "ingestion_timestamp",
    "ingestion_date"
)


# ============================================================
# 12. SHOW SILVER SCHEMA
# ============================================================

print("================================================")
print("SILVER SCHEMA")
print("================================================")

silver_df.printSchema()


# ============================================================
# 13. COUNT RECORDS
# ============================================================

record_count = silver_df.count()

print(
    f"Silver records created: {record_count}"
)


# ============================================================
# 14. SHOW SAMPLE DATA
# ============================================================

print("================================================")
print("SAMPLE SILVER DATA")
print("================================================")

silver_df.show(
    10,
    truncate=True
)


# ============================================================
# 15. WRITE SILVER DATA AS PARQUET
# ============================================================


print("================================================")
print("WRITING SILVER DATA")
print("================================================")

(
    silver_df.write.mode("overwrite")
            .partitionBy("country_code", "ingestion_date")
            .parquet(silver_output_path)
)


print("================================================")
print("SILVER DATA SUCCESSFULLY WRITTEN")
print("================================================")

print(f"Output path: {silver_output_path}")

print(f"Records written: {record_count}")


# ============================================================
# 16. COMMIT GLUE JOB
# ============================================================

job.commit()


print("================================================")
print("GLUE JOB COMPLETED SUCCESSFULLY")
print("================================================")