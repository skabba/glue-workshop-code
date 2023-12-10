import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, concat_ws, substring

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sql("set spark.sql.files.ignoreCorruptFiles=true")  # Deal with empty data files

glue_client = boto3.client("glue")

# BLOCK 1: Logging configs ============================================
logger = logging.getLogger()
log_level = "INFO"
logger.setLevel(log_level)

# Set sensible formatting
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Set stdout logger for glue
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(log_level)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

# Set stderr logger for glue (errorlogs)
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(formatter)
logger.addHandler(stderr_handler)
# BLOCK 1: Logging configs ============================================


# BLOCK 2: Glue job arguments =========================================
args = getResolvedOptions(
    sys.argv,
    [
        "s3_bucket_raw",
        "s3_bucket_processed",
        "s3_data_object_key",
        "glue_database",
        "glue_table",
        "next_glue_job",
        "DRYRUN",
    ],
)

S3_BUCKET_RAW = args["s3_bucket_raw"]
S3_BUCKET_PROCESSED = args["s3_bucket_processed"]
S3_DATA_OBJECT_KEY = args["s3_data_object_key"]
GLUE_DATABASE = args["glue_database"]
GLUE_TABLE = args["glue_table"]
NEXT_GLUE_JOB = args["next_glue_job"]
DRYRUN = bool(args["DRYRUN"].lower() == "true")
# BLOCK 2: Glue job arguments =========================================


# BLOCK 3: Process raw data ===========================================
# Read data into a Spark dataframe
raw_file_path = f"s3://{S3_BUCKET_RAW}/{S3_DATA_OBJECT_KEY}"
spark_df = spark.read.json(raw_file_path)

# Add new columns
extraction_datetime = (
    S3_DATA_OBJECT_KEY.split("_")[-1].replace(".json", "").replace(":", "")
)

spark_df = spark_df.withColumn("extraction_datetime", lit(extraction_datetime))
spark_df = spark_df.withColumn(
    "composite_pk", concat_ws("-", col("account_number"), col("iban"))
)
spark_df = spark_df.withColumn("country_code", substring("swift_bic", 5, 2))
# BLOCK 3: Process raw data ===========================================


# BLOCK 4: Write processed data to S3 =================================
spark_df.write.mode("overwrite").partitionBy("extraction_datetime").parquet(
    f"s3a://{S3_BUCKET_PROCESSED}/banks_data_spark/"
)
# BLOCK 4: Write processed data to S3 =================================


# BLOCK 5: Trigger the next job =======================================
# Start next Glue job: Processing newly ingested data
run_id = glue_client.start_job_run(
    JobName=NEXT_GLUE_JOB,
    Arguments={
        "--glue_database": GLUE_DATABASE,
        "--glue_table": GLUE_TABLE,
        "--table_partition": extraction_datetime,
        "--DRYRUN": str(DRYRUN),
    },
)
status = glue_client.get_job_run(JobName=NEXT_GLUE_JOB, RunId=run_id["JobRunId"])
logger.info("Job Status : ", status["JobRun"]["JobRunState"])
# BLOCK 5: Trigger the next job =======================================
