import sys
import boto3
import logging
import requests
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

# BLOCK 1: Logging configs ============================================
logger = logging.getLogger()
log_level = "INFO"
logger.setLevel(log_level)

# Set sensible formatting
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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
        "base_data_url",
        "data_spec",
        "data_size",
        "s3_bucket_raw",
        "next_glue_job",
        "spark",
        "DRYRUN",
    ],
)

BASE_DATA_URL = args["base_data_url"]  # https://random-data-api.com/api/v2
DATA_SPEC = args["data_spec"]  # banks
DATA_SIZE = args["data_size"]  # 100
S3_BUCKET_RAW = args["s3_bucket_raw"]
NEXT_GLUE_JOB = args["next_glue_job"]
SPARK = bool(args["spark"].lower() == "true")
DRYRUN = bool(args["DRYRUN"].lower() == "true")
# BLOCK 2: Glue job arguments =========================================

# BLOCK 3: HTTP call to fetch data ====================================
# Build full URL to call
full_data_endpoint = f"{BASE_DATA_URL}/{DATA_SPEC}?size={DATA_SIZE}"

# Execute the HTTP (GET) request to fetch data
response = requests.get(full_data_endpoint)
logger.info(
    f"HTTP call to endpoint {full_data_endpoint} returned a {response.status_code} status code."
)
# BLOCK 3: HTTP call to fetch data ====================================

# BLOCK 4: Handling the response ======================================
if response.status_code == 200:
    json_data = response.json()

    # Create S3 object name including current datetime
    current_datetime = datetime.now().strftime("%d-%m-%YT%H:%M:%S")
    s3_object_key = f"raw_files/banks_data_{current_datetime}.json"

    # Upload data as an object to S3
    s3_client.put_object(
        Body=json.dumps(json_data),
        Bucket=S3_BUCKET_RAW,
        Key=s3_object_key,
    )

    logger.info(f"Object {s3_object_key} uploaded to S3")

elif response.status_code == 400:
    logger.error(
        f"HTTP call to endpoint {full_data_endpoint} was not executed succesfully"
    )
# BLOCK 4: Handling the response ======================================

# BLOCK 5: Trigger the next job =======================================
# Start next Glue job: Processing newly ingested data
if SPARK:
    NEXT_GLUE_JOB = f"{NEXT_GLUE_JOB}-spark"

run_id = glue_client.start_job_run(
    JobName=NEXT_GLUE_JOB,
    Arguments={
        "--s3_data_object_key": s3_object_key,
        "--DRYRUN": str(DRYRUN)
    },
)
status = glue_client.get_job_run(JobName=NEXT_GLUE_JOB,
                                 RunId=run_id["JobRunId"])
logger.info("Job Status : ", status["JobRun"]["JobRunState"])
# BLOCK 5: Trigger the next job =======================================
