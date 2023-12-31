import logging
import json
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("DataValidation").getOrCreate()


# Validation functions
def validate_not_null(df, col_name):
    return df.filter(f"{col_name} IS NULL")


def validate_is_numeric(df, col_name):
    return df.filter(~df[col_name].rlike("^-?\\d+(\\.\\d+)?$"))


def validate_column_count(df, expected_count):
    actual_count = len(df.columns)
    if actual_count != expected_count:
        error_message = f"Expected {expected_count} columns but found {actual_count} columns"
        return spark.createDataFrame([Row(error=error_message)])
    return spark.createDataFrame([], schema="error string")


def validate_min_row_count(df, min_count=5):
    sample_rows = df.limit(min_count + 1).collect()
    actual_count = len(sample_rows)
    if actual_count <= min_count:
        error_message = f"Expected at least {min_count} rows but found only {actual_count} rows"
        return spark.createDataFrame([Row(error=error_message)])
    return spark.createDataFrame([], schema="error string")


# Global validations configuration
VALIDATIONS_CONFIG = {
    "not_null": {
        "function": validate_not_null,
        "message": "Null values."
    },
    "is_numeric": {
        "function": validate_is_numeric,
        "message": "Non-numeric values."
    },
    "column_count": {
        "function": validate_column_count,
        "message": "Incorrect column count."
    },
    "min_row_count": {
        "function": validate_min_row_count,
        "message": "Minimum row count (5) not satisfied."
    }
}

import boto3
from datetime import datetime


def copy_file_with_date(source_s3_path, destination_s3_directory):
    """
    Copy a file from the source S3 path to the destination S3 directory with a date appended to its filename.

    Parameters:
    - source_s3_path (str): The S3 path of the source file to be copied.
    - destination_s3_directory (str): The S3 directory where the file should be copied to.

    Returns:
    str: The new S3 path where the file was copied to.
    """
    # Extract the bucket and key from the source path
    source_bucket, source_key = source_s3_path.replace("s3://", "").split("/", 1)

    # Extract the filename from the key
    filename = source_key.split("/")[-1]

    # Extract the extension (if any)
    file_extension = ""
    if "." in filename:
        filename, file_extension = filename.rsplit(".", 1)
        file_extension = "." + file_extension

    # Append date to the filename
    current_date = datetime.now().strftime("%Y%m%d")
    new_filename = f"{filename}_{current_date}{file_extension}"

    # Extract the destination bucket and construct the destination key
    destination_bucket = destination_s3_directory.replace("s3://", "").split("/")[0]
    destination_key = f"{destination_s3_directory.replace('s3://' + destination_bucket + '/', '')}{new_filename}"

    # Create an S3 client and copy the object
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key},
                          Key=destination_key)

    return f"s3://{destination_bucket}/{destination_key}"


def log_failures(sample_failing_rows, column, validation_type, message, logs_output_path):
    if sample_failing_rows:
        logging.error(f"Validation {validation_type} failed for {column}. Found {len(sample_failing_rows)} {message}.")
        output_df = spark.createDataFrame(sample_failing_rows)
        output_df.write.mode("overwrite").csv(f"{logs_output_path}/{column}_{validation_type}")
    else:
        logging.info(f"Validation {validation_type} passed for {column}. No {message} found.")


def run_validations(input_file_s3_path, logs_output_path, column_validations_config, expected_count):
    df = spark.read.csv(input_file_s3_path, header=True, inferSchema=True)
    for column, column_validations in column_validations_config.items():
        for validation in column_validations:
            validation_function = VALIDATIONS_CONFIG[validation]["function"]

            # Check if the validation is for column_count
            if validation == "column_count":
                failing_df = validation_function(df, expected_count)
            else:
                failing_df = validation_function(df, column)

            if failing_df.head():
                sample_failing_rows = failing_df.limit(10).collect()
                message = VALIDATIONS_CONFIG[validation]["message"]
                log_failures(sample_failing_rows, column, validation, message, logs_output_path)
            else:
                logging.info(f"Validation {validation} passed for {column}. No issues found.")


if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['s3_path', 'validations', 'output_path', 'expected_count'])
        validations_config = json.loads(args['validations'])
        expected_count = int(args['expected_count'])

        run_validations(args['s3_path'], args['output_path'], validations_config, expected_count)
    except json.JSONDecodeError:
        logging.error("Error parsing 'validations' argument. Ensure it's valid JSON.")
    except Exception as e:
        logging.exception("An error occurred during execution.")
