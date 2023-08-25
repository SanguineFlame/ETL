import json
import boto3
import pandas as pd
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Utilize the provided Spark & Glue contexts
glueContext = GlueContext(SparkContext.getOrCreate())
s3 = boto3.client('s3')


# Fetch configuration from S3
def fetch_config_from_s3(bucket_name, config_key):
    config_obj = s3.get_object(Bucket=bucket_name, Key=config_key)
    return json.load(config_obj['Body'])


# Fetch data from S3
def fetch_data_from_s3(s3_path):
    return pd.read_csv(s3_path)


# Fetch data using a Glue Connection
def fetch_data_from_glue_connection(connection_name):
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(connection=connection_name, database="your_database",
                                                                  table_name="your_table")
    return dynamic_frame.toDF()  # Converts DynamicFrame to DataFrame


# Validation Functions
def not_empty(value):
    return value != '' and not pd.isna(value)


def is_integer(value):
    return str(value).isdigit()


def is_date(value, format="%Y-%m-%d"):
    try:
        pd.to_datetime(value, format=format)
        return True
    except ValueError:
        return False


def is_email(value):
    # A basic email validation check
    if not value or pd.isna(value):
        return False
    return "@" in value and "." in value


VALIDATION_FUNCTIONS = {
    "not_empty": not_empty,
    "is_integer": is_integer,
    "is_date": is_date,
    "is_email": is_email
}


def validate_data(df, validations):
    error_rows = []

    for index, row in df.iterrows():
        for column, rules in validations.items():
            for rule in rules:
                # Extract extra arguments if present, e.g., format for is_date
                function_args = rule.get('args', [])
                function_kwargs = rule.get('kwargs', {})

                # Obtain the validation function
                validation_function = VALIDATION_FUNCTIONS[rule['name']]

                if not validation_function(row[column], *function_args, **function_kwargs):
                    error_rows.append(index)

    if error_rows:
        raise ValueError(f"Data validation failed for rows: {error_rows}")


# Configuration fetching
BUCKET_NAME = 'testing-thangs-123'
CONFIG_KEY = 'scripts/config.json'
config = fetch_config_from_s3(BUCKET_NAME, CONFIG_KEY)

file_name = 'MOCK_DATA.csv'  # This would be dynamically determined based on your use case

# Fetching data based on source type
source_type = config[file_name].get('source_type', 's3')  # Default to 's3' if not specified

if source_type == "s3":
    s3_path = config[file_name]['s3_path']
    df = fetch_data_from_s3(s3_path)
elif source_type == "database" and config[file_name]['database'] == "oracle":
    connection_name = config[file_name]['glue_connection_name']
    df = fetch_data_from_glue_connection(connection_name)

# Column Validation
if set(df.columns) != set(config[file_name]['expected_columns']):
    raise ValueError(
        f"Unexpected columns in {file_name}. Expected {config[file_name]['expected_columns']} but got {df.columns}")

# Data validation based on the rules from config
validate_data(df, config[file_name]['validations'])


# Write the validated DataFrame directly to S3 as a single pipe-delimited CSV file
s3_output_path = "s3://testing-thangs-123/outgoing/"
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)
spark_df.coalesce(1).write.option("delimiter", "|").csv(s3_output_path, mode="overwrite")
