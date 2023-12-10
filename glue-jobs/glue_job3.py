import sys
import json
import logging
import requests
import awswrangler as wr
from awsglue.utils import getResolvedOptions

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
        "s3_bucket_processed",
        "table_partition",
        "glue_database",
        "glue_table",
        "limit_rows",
        "endpoint",
        "DRYRUN",
    ],
)

S3_BUCKET_PROCESSED = args["s3_bucket_processed"]
TABLE_PARTITION = args["table_partition"]
GLUE_DATABASE = args["glue_database"]
GLUE_TABLE = args["glue_table"]
LIMIT_ROWS = args["limit_rows"]
ENDPOINT = args["endpoint"]
DRYRUN = bool(args["DRYRUN"].lower() == "true")
# BLOCK 2: Glue job arguments =========================================


# BLOCK 3: Query data from S3 via Athena ==============================
# Query data from the specific partition of the processed table
BASE_SQL_QUERY = f"""
SELECT composite_pk, uid, bank_name, routing_number
FROM {GLUE_DATABASE}.{GLUE_TABLE}
WHERE extraction_datetime = '{TABLE_PARTITION}'
LIMIT {LIMIT_ROWS}
"""

df = wr.athena.read_sql_query(sql=BASE_SQL_QUERY, database=GLUE_DATABASE)
# BLOCK 3: Query data from S3 via Athena ==============================


# BLOCK 4: Send each row to a data endpoint (POST) ====================
list_of_dicts = df.to_dict("records")

for row in list_of_dicts:
    response = requests.post(ENDPOINT, json.dumps(row))
    logger.info(response.json())
# BLOCK 4: Send each row to a data endpoint (POST) ====================
