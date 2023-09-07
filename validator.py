import logging
import json
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

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


def log_failures(sample_failing_rows, column, validation_type, message, logs_output_path):
    if sample_failing_rows:
        logging.error(f"Validation {validation_type} failed for {column}. Found {len(sample_failing_rows)} {message}.")
        output_df = spark.createDataFrame(sample_failing_rows)
        output_df.write.mode("overwrite").csv(f"{logs_output_path}/{column}_{validation_type}")
    else:
        logging.info(f"Validation {validation_type} passed for {column}. No {message} found.")


def run_validations(input_file_s3_path, logs_output_path, column_validations_config):
    df = spark.read.csv(input_file_s3_path, header=True, inferSchema=True)

    for column, column_validations in column_validations_config.items():
        for validation in column_validations:
            validation_function = VALIDATIONS_CONFIG[validation]["function"]
            failing_df = validation_function(df, column)

            # Only collect sample failing rows if there are failures
            if failing_df.head():
                sample_failing_rows = failing_df.limit(10).collect()
                message = VALIDATIONS_CONFIG[validation]["message"]
                log_failures(sample_failing_rows, column, validation, message, logs_output_path)
            else:
                logging.info(f"Validation {validation} passed for {column}. No issues found.")


if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['s3_path', 'validations', 'output_path'])
        validations_config = json.loads(args['validations'])

        run_validations(args['s3_path'], args['output_path'], validations_config)
    except json.JSONDecodeError:
        logging.error("Error parsing 'validations' argument. Ensure it's valid JSON.")
    except Exception as e:
        logging.exception("An error occurred during execution.")
