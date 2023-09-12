import json
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import column_validations as col_val
from typing import Tuple
import boto3
import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def has_min_rows(df, min_rows: int) -> Tuple[bool, DataFrame]:
    failing_rows = df if df.count() < min_rows else None
    return failing_rows is None, failing_rows


def has_required_columns(df, expected_cols: list) -> Tuple[bool, list]:
    missing_cols = [col for col in expected_cols if col not in df.columns]
    success = len(missing_cols) == 0
    return success, missing_cols


VALIDATIONS = {
    "min_rows": has_min_rows,
    "expected_columns": has_required_columns,
    "mandatory_values": col_val.has_mandatory_values,
    "numeric_values": col_val.is_column_numeric
}


# Refactoring the run_validations_for_dataset function

def load_dataset_config(dataset_name: str, config_file: str = 'config.json'):
    """Load configuration for the given dataset name."""
    with open(config_file, 'r') as file:
        configs = json.load(file)
    return next((conf for conf in configs if conf["dataset_name"] == dataset_name), None)


def load_dataset_from_s3(s3_path: str):
    """Load dataset from the given S3 path."""
    return spark.read.csv(s3_path, header=True, inferSchema=True)


def execute_validations(df, dataset_config):
    """Execute validations on the dataset based on the provided configuration."""
    for validation_name, parameter in dataset_config.items():
        if validation_name in VALIDATIONS:
            validation_func = VALIDATIONS[validation_name]

            if isinstance(parameter, list):
                for col in parameter:
                    success, failing_data = validation_func(df, col)
                    if not success:
                        if validation_name == "expected_columns":
                            print(f"Validation {validation_name} failed. Missing columns: {', '.join(failing_data)}")
                        else:
                            print(f"Validation {validation_name} failed on column {col}!")
                            failing_data.show(5)
            else:
                success, failing_data = validation_func(df, parameter)
                if not success:
                    print(f"Validation {validation_name} failed with parameter {parameter}!")
                    failing_data.show(5)


def run_validations_for_dataset(dataset_name: str, config_file: str = 'config.json'):
    """Main function to run validations on a dataset."""
    dataset_config = load_dataset_config(dataset_name, config_file)
    if not dataset_config:
        print(f"No configuration found for dataset {dataset_name}.")
        return

    s3_path = dataset_config.get("s3_path")
    if not s3_path:
        print(f"No S3 path provided for dataset {dataset_name}.")
        return

    df = load_dataset_from_s3(s3_path)
    execute_validations(df, dataset_config)


args = getResolvedOptions(sys.argv, ['dataset_name'])
dataset_name = args['dataset_name']
run_validations_for_dataset(dataset_name)


def transform_and_copy_csv(source_path: str, destination_bucket: str, destination_prefix: str):
    # 1. Read the CSV from its S3 location.
    df = spark.read.csv(source_path, header=True, inferSchema=True)

    # Getting the current date in YYYYMMDD format
    current_date = datetime.datetime.now().strftime('%Y%m%d')

    # Extracting the file name from the source path
    filename = source_path.split("/")[-1]
    if '.csv' in filename:
        filename = filename.replace('.csv', f'_{current_date}.csv')

    # 2. Transform its delimiter to a pipe (`|`).
    # 3. Save it with new delimiter and name to another S3 location.
    destination_path = f"s3://{destination_bucket}/{destination_prefix}/{filename}"
    df.write.option("delimiter", "|").csv(destination_path)

    print(f"Data saved to {destination_path}")


# Example
source_path = "s3://source-bucket/path/to/source.csv"
destination_bucket = "destination-bucket"
destination_prefix = "path/to/destination"

transform_and_copy_csv(source_path, destination_bucket, destination_prefix)
