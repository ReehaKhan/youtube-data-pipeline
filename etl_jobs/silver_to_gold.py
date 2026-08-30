import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


# ============================================================
# 1. GET GLUE JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DATABASE",
        "SILVER_TABLE",
        "GOLD_BASE_PATH"
    ]
)

JOB_NAME = args["JOB_NAME"]
SILVER_DATABASE = args["SILVER_DATABASE"]
SILVER_TABLE = args["SILVER_TABLE"]
GOLD_BASE_PATH = args["GOLD_BASE_PATH"]


# ============================================================
# 2. INITIALIZE SPARK / GLUE
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(JOB_NAME, args)


# ============================================================
# 3. READ SILVER DATA
# ============================================================

print("==========================================")
print("READING SILVER DATA")
print("==========================================")

silver_df = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=SILVER_DATABASE,
        table_name=SILVER_TABLE
    )
    .toDF()
)


print("Silver schema:")
silver_df.printSchema()

silver_count = silver_df.count()

print(
    f"Silver records: {silver_count}"
)


# ============================================================
# 4. VALIDATE REQUIRED COLUMNS
# ============================================================

print("==========================================")
print("VALIDATING SILVER SCHEMA")
print("==========================================")


required_columns = [
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
]


missing_columns = [column
                for column in required_columns
                if column not in silver_df.columns]


if missing_columns:

    raise ValueError(
        "Silver table is missing required columns: "
        + ", ".join(missing_columns) + ". "
    )


print("Silver schema validation passed.")


# ============================================================
# 5. DETERMINE PROCESSING DATES
# ============================================================

print("==========================================")
print("DETERMINING PROCESSING DATES")
print("==========================================")


processing_dates = (
    silver_df
    .select("ingestion_date")
    .distinct()
    .filter(
        F.col("ingestion_date").isNotNull()
    )
)


processing_dates_list = [row["ingestion_date"]
                        for row in processing_dates.collect()]


print("Processing dates:", processing_dates_list)


if not processing_dates_list:

    raise ValueError(
        "No valid ingestion dates found in Silver."
    )


# ============================================================
# 6. GOLD PATHS
# ============================================================

dim_video_path = (f"{GOLD_BASE_PATH}/dim_video/")
dim_channel_path = (f"{GOLD_BASE_PATH}/dim_channel/")
dim_category_path = (f"{GOLD_BASE_PATH}/dim_category/")
dim_country_path = (f"{GOLD_BASE_PATH}/dim_country/")
dim_date_path = (f"{GOLD_BASE_PATH}/dim_date/")

fact_path = (f"{GOLD_BASE_PATH}/fact_video_trending/")


# ============================================================
# 7. HELPER FUNCTION
# ============================================================

def path_exists(path):

    try:
        spark.read.parquet(path).limit(1).count()
        return True

    except Exception:
        return False


# ============================================================
# 8. CREATE DIMENSION SOURCE DATASETS
# ============================================================

print("==========================================")
print("CREATING DIMENSION SOURCE DATA")
print("==========================================")


# ------------------------------------------------------------
# DIM VIDEO SOURCE
# ------------------------------------------------------------

dim_video_source = (
    silver_df
    .select(
        "video_id",
        "title",
        "description",
        "published_at",
        "duration_seconds",
        "definition",
        "caption"
    )
    .filter(F.col("video_id").isNotNull())
    .dropDuplicates(["video_id"])
)


# ------------------------------------------------------------
# DIM CHANNEL SOURCE
# ------------------------------------------------------------

dim_channel_source = (
    silver_df
    .select(
        "channel_id",
        "channel_name"
    )
    .filter(F.col("channel_id").isNotNull())
    .dropDuplicates(["channel_id", "channel_name"])
)


# ------------------------------------------------------------
# DIM CATEGORY SOURCE
# ------------------------------------------------------------

dim_category_source = (
    silver_df
    .select(
        "category_id",
        "category_name"
    )
    .filter(F.col("category_id").isNotNull())
    .dropDuplicates(["category_id"])
)


# ------------------------------------------------------------
# DIM COUNTRY SOURCE
# ------------------------------------------------------------

dim_country_source = (
    silver_df
    .select(
        "country_code",
        "country_name"
    )
    .filter(F.col("country_code").isNotNull())
    .dropDuplicates(["country_code"])
    .withColumn("region", F.lit("MENA"))
)


# ------------------------------------------------------------
# DIM DATE SOURCE
# ------------------------------------------------------------

dim_date_source = (
    silver_df
    .select(
        F.col("ingestion_date").alias("full_date")
    )
    .filter(F.col("full_date").isNotNull())
    .dropDuplicates()
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("week", F.weekofyear("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("year", F.year("full_date"))
)


# ============================================================
# 9. DIM VIDEO
# ============================================================

print("==========================================")
print("PROCESSING DIM_VIDEO")
print("==========================================")


if path_exists(dim_video_path):

    print("Existing dim_video found.")

    existing_dim_video = (spark.read.parquet(dim_video_path))

    max_video_key = (existing_dim_video.agg(F.max("video_key"))
                                        .collect()[0][0])

    if max_video_key is None:
        max_video_key = 0


    # Find videos not already in dimension

    new_videos = (
        dim_video_source.alias("new")
        .join(
            existing_dim_video.alias("old"),
            F.col("new.video_id") == F.col("old.video_id"),
            "left_anti"
        )
    )


    # Generate surrogate keys

    window = Window.orderBy("video_id")

    new_videos = (
        new_videos
        .withColumn("video_key", F.row_number().over(window) + F.lit(max_video_key))
        .select(
            "video_key",
            "video_id",
            "title",
            "description",
            "published_at",
            "duration_seconds",
            "definition",
            "caption"
        )
    )


    updated_dim_video = (
        existing_dim_video
        .unionByName(new_videos)
    )


else:

    print("dim_video does not exist. \nCreating it.")


    updated_dim_video = (
        dim_video_source
        .withColumn("video_key", F.row_number().over(Window.orderBy("video_id")))
        .select(
            "video_key",
            "video_id",
            "title",
            "description",
            "published_at",
            "duration_seconds",
            "definition",
            "caption"
        )
    )


# ------------------------------------------------------------
# MATERIALIZE BEFORE OVERWRITE
# ------------------------------------------------------------

updated_dim_video = (
    updated_dim_video
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Materializing dim_video...")

dim_video_count = (updated_dim_video.count())

print(f"dim_video records: {dim_video_count}")


# ------------------------------------------------------------
# WRITE DIM VIDEO
# ------------------------------------------------------------

(
    updated_dim_video
    .write
    .mode("overwrite")
    .parquet(
        dim_video_path
    )
)

updated_dim_video.unpersist()


# ============================================================
# 10. DIM CHANNEL — SCD TYPE 2
# ============================================================

print("==========================================")
print("PROCESSING DIM_CHANNEL")
print("SCD TYPE 2")
print("==========================================")


# ------------------------------------------------------------
# FIRST RUN
# ------------------------------------------------------------

if not path_exists(dim_channel_path):

    print("dim_channel does not exist. \nCreating it.")

    window = Window.orderBy("channel_id")

    updated_dim_channel = (
        dim_channel_source
        .withColumn("channel_key", F.row_number().over(window))
        .withColumn("valid_from", F.lit(min(processing_dates_list)).cast("date"))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .select(
            "channel_key",
            "channel_id",
            "channel_name",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )


# ------------------------------------------------------------
# SUBSEQUENT RUNS
# ------------------------------------------------------------

else:

    print("Existing dim_channel found.")

    existing_dim_channel = (spark.read.parquet(dim_channel_path))


    # Current versions only

    current_channels = (
        existing_dim_channel
        .filter(F.col("is_current") == True)
    )


    # --------------------------------------------------------
    # Completely new channels
    # --------------------------------------------------------

    new_channels = (
        dim_channel_source.alias("new")
        .join(
            current_channels.alias("old"),
            F.col("new.channel_id") == F.col("old.channel_id"),
            "left_anti"
        )
    )


    # --------------------------------------------------------
    # Detect changed channel names
    # --------------------------------------------------------

    changed_channels = (
        dim_channel_source.alias("new")
        .join(
            current_channels.alias("old"),
            F.col("new.channel_id") == F.col("old.channel_id"),
            "inner"
        )
        .filter(
            F.coalesce(F.col("new.channel_name"), F.lit(""))
            !=
            F.coalesce(F.col("old.channel_name"), F.lit(""))
        )
        .select(
            F.col("new.channel_id"),
            F.col("new.channel_name")
        )
        .dropDuplicates(["channel_id"])
    )


    changed_ids = (
        changed_channels
        .select("channel_id")
        .distinct()
    )


    # --------------------------------------------------------
    # Existing records that have NOT changed
    # --------------------------------------------------------

    unchanged_records = (
        existing_dim_channel
        .join(
            changed_ids,
            "channel_id",
            "left_anti"
        )
    )


    # --------------------------------------------------------
    # Old versions for changed channels
    # --------------------------------------------------------

    old_changed_records = (
        existing_dim_channel
        .join(
            changed_ids,
            "channel_id",
            "inner"
        )
        .withColumn("is_current", F.lit(False))
        .withColumn("valid_to", F.date_sub(F.current_date(), 1))
    )


    # --------------------------------------------------------
    # Determine effective date for changed channels
    # --------------------------------------------------------

    changed_effective_dates = (
        silver_df
        .join(
            changed_channels,
            "channel_id",
            "inner"
        )
        .select(
            "channel_id",
            "ingestion_date"
        )
        .groupBy("channel_id")
        .agg(F.min("ingestion_date").alias("effective_date"))
    )


    # --------------------------------------------------------
    # Highest existing channel key
    # --------------------------------------------------------

    max_channel_key = (
        existing_dim_channel
        .agg(F.max("channel_key"))
        .collect()[0][0]
    )


    if max_channel_key is None:
        max_channel_key = 0


    changed_count = (changed_channels.count())


    # --------------------------------------------------------
    # Create new versions for changed channels
    # --------------------------------------------------------

    changed_window = Window.orderBy("channel_id")


    new_changed_versions = (
        changed_channels
        .join(
            changed_effective_dates,
            "channel_id",
            "left"
        )
        .withColumn("channel_key", F.row_number().over(changed_window) + F.lit(max_channel_key))
        .withColumn("valid_from", F.col("effective_date"))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .select(
            "channel_key",
            "channel_id",
            "channel_name",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )


    # --------------------------------------------------------
    # Create completely new channels
    # --------------------------------------------------------

    new_window = Window.orderBy("channel_id")


    new_channels = (
        new_channels
        .withColumn("channel_key", F.row_number().over(new_window) + F.lit(max_channel_key + changed_count))
        .withColumn("valid_from", F.lit(min(processing_dates_list)).cast("date"))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .select(
            "channel_key",
            "channel_id",
            "channel_name",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )


    # --------------------------------------------------------
    # Combine everything
    # --------------------------------------------------------

    updated_dim_channel = (
        unchanged_records
        .unionByName(old_changed_records)
        .unionByName(new_changed_versions)
        .unionByName(new_channels)
    )


# ------------------------------------------------------------
# MATERIALIZE BEFORE OVERWRITE
# ------------------------------------------------------------

updated_dim_channel = (
    updated_dim_channel
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Materializing dim_channel...")

dim_channel_count = (updated_dim_channel.count())

print(f"dim_channel records: {dim_channel_count}")


# ------------------------------------------------------------
# WRITE
# ------------------------------------------------------------

(
    updated_dim_channel
    .write
    .mode("overwrite")
    .parquet(
        dim_channel_path
    )
)

updated_dim_channel.unpersist()


# ============================================================
# 11. DIM CATEGORY
# ============================================================

print("==========================================")
print("PROCESSING DIM_CATEGORY")
print("==========================================")


if path_exists(dim_category_path):

    print("Existing dim_category found.")


    existing_dim_category = (spark.read.parquet(dim_category_path))

    max_category_key = (
        existing_dim_category
        .agg(F.max("category_key"))
        .collect()[0][0]
    )


    if max_category_key is None:
        max_category_key = 0


    new_categories = (
        dim_category_source.alias("new")
        .join(
            existing_dim_category.alias("old"),
            F.col("new.category_id") == F.col("old.category_id"),
            "left_anti"
        )
    )


    window = Window.orderBy("category_id")

    new_categories = (
        new_categories
        .withColumn("category_key", F.row_number().over(window) + F.lit(max_category_key))
        .select(
            "category_key",
            "category_id",
            "category_name"
        )
    )


    updated_dim_category = (
        existing_dim_category
        .unionByName(new_categories)
    )


else:

    print("dim_category does not exist. \nCreating it.")

    window = Window.orderBy("category_id")

    updated_dim_category = (
        dim_category_source
        .withColumn("category_key", F.row_number().over(window))
        .select(
            "category_key",
            "category_id",
            "category_name"
        )
    )


# ------------------------------------------------------------
# MATERIALIZE
# ------------------------------------------------------------

updated_dim_category = (
    updated_dim_category
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Materializing dim_category...")

dim_category_count = (updated_dim_category.count())

print(f"dim_category records: {dim_category_count}")


# ------------------------------------------------------------
# WRITE
# ------------------------------------------------------------

(
    updated_dim_category
    .write
    .mode("overwrite")
    .parquet(dim_category_path)
)

updated_dim_category.unpersist()


# ============================================================
# 12. DIM COUNTRY
# ============================================================

print("==========================================")
print("PROCESSING DIM_COUNTRY")
print("==========================================")


if path_exists(dim_country_path):

    print("Existing dim_country found.")

    existing_dim_country = (spark.read.parquet(dim_country_path))

    max_country_key = (
        existing_dim_country
        .agg(F.max("country_key"))
        .collect()[0][0]
    )


    if max_country_key is None:
        max_country_key = 0


    new_countries = (
        dim_country_source.alias("new")
        .join(
            existing_dim_country.alias("old"),
            F.col("new.country_code") == F.col("old.country_code"),
            "left_anti"
        )
    )

    window = Window.orderBy("country_code")

    new_countries = (
        new_countries
        .withColumn("country_key", F.row_number().over(window) + F.lit(max_country_key))
        .select(
            "country_key",
            "country_code",
            "country_name",
            "region"
        )
    )


    updated_dim_country = (
        existing_dim_country
        .unionByName(new_countries)
    )


else:

    print("dim_country does not exist. \nCreating it.")

    window = Window.orderBy("country_code")

    updated_dim_country = (
        dim_country_source
        .withColumn("country_key", F.row_number().over(window))
        .select(
            "country_key",
            "country_code",
            "country_name",
            "region"
        )
    )


# ------------------------------------------------------------
# MATERIALIZE
# ------------------------------------------------------------

updated_dim_country = (
    updated_dim_country
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Materializing dim_country...")

dim_country_count = (updated_dim_country.count())

print(f"dim_country records: {dim_country_count}")


# ------------------------------------------------------------
# WRITE
# ------------------------------------------------------------

(
    updated_dim_country
    .write
    .mode("overwrite")
    .parquet(
        dim_country_path
    )
)

updated_dim_country.unpersist()


# ============================================================
# 13. DIM DATE
# ============================================================

print("==========================================")
print("PROCESSING DIM_DATE")
print("==========================================")


if path_exists(dim_date_path):

    print("Existing dim_date found.")

    existing_dim_date = (spark.read.parquet(dim_date_path))

    new_dates = (
        dim_date_source.alias("new")
        .join(
            existing_dim_date.alias("old"),
            F.col("new.full_date") == F.col("old.full_date"),
            "left_anti"
        )
    )


    updated_dim_date = (
        existing_dim_date
        .unionByName(new_dates)
    )


else:

    print("dim_date does not exist. \nCreating it.")

    updated_dim_date = (dim_date_source)


# ------------------------------------------------------------
# MATERIALIZE
# ------------------------------------------------------------

updated_dim_date = (
    updated_dim_date
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Materializing dim_date...")

dim_date_count = (updated_dim_date.count())

print(f"dim_date records: {dim_date_count}")


# ------------------------------------------------------------
# WRITE
# ------------------------------------------------------------

(
    updated_dim_date
    .write
    .mode("overwrite")
    .parquet(dim_date_path)
)

updated_dim_date.unpersist()


# ============================================================
# 14. CREATE FACT SOURCE
# ============================================================

print("==========================================")
print("CREATING FACT TABLE")
print("==========================================")


fact_source = (

    silver_df.alias("s")

    # --------------------------------------------------------
    # VIDEO
    # --------------------------------------------------------

    .join(
        updated_dim_video.alias("v"),
        F.col("s.video_id") == F.col("v.video_id"),
        "inner"
    )


    # --------------------------------------------------------
    # CHANNEL 
    # --------------------------------------------------------

    .join(
        updated_dim_channel.alias("c"),
        (F.col("s.channel_id") == F.col("c.channel_id"))
        &
        (F.col("s.ingestion_date") >= F.col("c.valid_from"))
        &
        (F.col("c.valid_to").isNull() | (F.col("s.ingestion_date") <= F.col("c.valid_to"))),
        "inner"
    )


    # --------------------------------------------------------
    # CATEGORY
    # --------------------------------------------------------

    .join(
        updated_dim_category.alias("cat"),
        F.col("s.category_id") == F.col("cat.category_id"),
        "inner"
    )


    # --------------------------------------------------------
    # COUNTRY
    # --------------------------------------------------------

    .join(
        updated_dim_country.alias("co"),
        F.col("s.country_code") == F.col("co.country_code"),
        "inner"
    )


    # --------------------------------------------------------
    # DATE
    # --------------------------------------------------------

    .join(
        updated_dim_date.alias("d"),
        F.col("s.ingestion_date") == F.col("d.full_date"),
        "inner"
    )
)


# ============================================================
# 15. SELECT FACT COLUMNS
# ============================================================

fact_source = (
    fact_source
    .select(
        F.col("d.date_key").alias("date_key"),
        F.col("v.video_key").alias("video_key"),
        F.col("c.channel_key").alias("channel_key"),
        F.col("cat.category_key").alias("category_key"),
        F.col("co.country_key").alias("country_key"),
        F.col("s.rank").alias("rank"),
        F.col("s.view_count").alias("view_count"),
        F.col("s.like_count").alias("like_count"),
        F.col("s.comment_count").alias("comment_count"),
        F.col("s.ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("s.ingestion_date").alias("ingestion_date")
    )
)


# ============================================================
# 16. REMOVE DUPLICATES WITHIN CURRENT LOAD
# ============================================================

fact_source = (
    fact_source
    .dropDuplicates(
        [
            "date_key",
            "video_key",
            "country_key"
        ]
    )
)


fact_source_count = (fact_source.count())
print(f"Fact records generated: {fact_source_count}")


# ============================================================
# 17. IDEMPOTENT FACT LOADING
# ============================================================

print("==========================================")
print("PREPARING FACT TABLE")
print("==========================================")


if path_exists(fact_path):

    print("Existing fact table found.")

    existing_fact = (spark.read.parquet(fact_path))


    # --------------------------------------------------------
    # Keep historical dates that are NOT being processed
    # --------------------------------------------------------

    historical_fact = (
        existing_fact
        .filter(~F.col("ingestion_date").isin(processing_dates_list))
    )


    # --------------------------------------------------------
    # Replace observations for processing dates
    # --------------------------------------------------------

    final_fact = (
        historical_fact
        .unionByName(fact_source)
    )


else:

    print("Fact table does not exist. \n Creating it.")

    final_fact = (fact_source)


# ============================================================
# 18. MATERIALIZE FACT BEFORE OVERWRITE
# ============================================================

final_fact = (
    final_fact
    .persist(StorageLevel.MEMORY_AND_DISK)
)


print("Materializing final fact table...")


final_fact_count = (final_fact.count())
print(f"Final fact records: {final_fact_count}")


# ============================================================
# 19. WRITE FINAL FACT TABLE
# ============================================================

print("==========================================")
print("WRITING FACT TABLE")
print("==========================================")


(
    final_fact
    .write
    .mode("overwrite")
    .partitionBy("ingestion_date")
    .parquet(fact_path)
)


final_fact.unpersist()


# ============================================================
# 20. FINAL VALIDATION
# ============================================================

print("\n==========================================")
print("FINAL GOLD VALIDATION")
print("==========================================")


print(f"Silver records: {silver_count}")

print(f"dim_video records: {dim_video_count}")
print(f"dim_channel records: {dim_channel_count}")
print(f"dim_category records: {dim_category_count}")
print(f"dim_country records: {dim_country_count}")
print(f"dim_date records: {dim_date_count}")

print(f"fact_video_trending records: {final_fact_count}")


# ============================================================
# 21. FINAL SCHEMAS
# ============================================================

print("\n==========================================")
print("DIM_VIDEO SCHEMA")
print("==========================================")

updated_dim_video.printSchema()


print("\n==========================================")
print("DIM_CHANNEL SCHEMA")
print("==========================================")

updated_dim_channel.printSchema()


print("\n==========================================")
print("DIM_CATEGORY SCHEMA")
print("==========================================")

updated_dim_category.printSchema()


print("\n==========================================")
print("DIM_COUNTRY SCHEMA")
print("==========================================")

updated_dim_country.printSchema()


print("\n==========================================")
print("DIM_DATE SCHEMA")
print("==========================================")

updated_dim_date.printSchema()


print("\n==========================================")
print("FACT_VIDEO_TRENDING SCHEMA")
print("==========================================")

final_fact.printSchema()


# ============================================================
# 22. COMMIT GLUE JOB
# ============================================================

job.commit()


print("==========================================")
print("GOLD ETL COMPLETED SUCCESSFULLY")
print("==========================================")