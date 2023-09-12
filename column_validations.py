from pyspark.sql import DataFrame
from typing import Tuple


def has_mandatory_values(df: DataFrame, column_name: str) -> Tuple[bool, DataFrame]:
    failing_rows = df.filter(df[column_name].isNull() | (df[column_name] == ""))
    success = failing_rows.count() == 0
    return success, failing_rows


def is_column_numeric(df: DataFrame, column_name: str) -> Tuple[bool, DataFrame]:
    failing_rows = df.filter(~df[column_name].cast("double").isNotNull())
    success = failing_rows.count() == 0
    return success, failing_rows

# Add more column-specific validations as needed...
