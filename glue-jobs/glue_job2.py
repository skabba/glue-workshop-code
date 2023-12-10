import sys
import boto3
import logging
import awswrangler as wr
from awsglue.utils import getResolvedOptions

s3_client = boto3.client("s3")
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
s3_object_path = f"s3://{S3_BUCKET_RAW}/{S3_DATA_OBJECT_KEY}"
df = wr.s3.read_json(s3_object_path)

extraction_datetime = S3_DATA_OBJECT_KEY.split("_")[-1].replace(".json", "")

# Add new columns to the dataframe
df["extraction_datetime"] = extraction_datetime
df["composite_pk"] = df[["account_number", "iban"]].apply(
    lambda x: "-".join(x.map(str)), axis=1
)
df["country_code"] = df["swift_bic"].apply(lambda x: x[4:6])
# BLOCK 3: Process raw data ===========================================


# BLOCK 4: Write processed data to S3 & index in Glue =================
wr.s3.to_parquet(
    df=df,
    path=f"s3://{S3_BUCKET_PROCESSED}/banks_data/",
    index=False,
    dataset=True,
    database=GLUE_DATABASE,
    table=GLUE_TABLE,
    mode="overwrite_partitions",
    partition_cols=["extraction_datetime"],
)
# BLOCK 4: Write processed data to S3 & index in Glue =================


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
