import json
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "dev/secret/mySQL"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    return secret


secrets = get_secret()

spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://database-1.c4iwusdi8hal.us-east-1.rds.amazonaws.com:3306/chrome_schema") \
    .option("dbtable", "myTable") \
    .option("user", secrets["username"]) \
    .option("password", secrets["password"]) \
    .load()

df.createOrReplaceTempView("my_table_view")

result = spark.sql("SELECT * FROM my_table_view;")

result.coalesce(1).write.option("delimiter", "|").mode("overwrite").csv("s3://testing-thangs-123/incoming/feed1/")

# Assuming you've already written the DataFrame to a unique S3 directory using Spark with coalesce(1)
unique_output_directory = 's3://testing-thangs-123/incoming/feed1/'
desired_filename = 'my_output_file.csv'

# Initialize the S3 client
s3_client = boto3.client('s3')
bucket_name = 'testing-thangs-123'
incoming_prefix = 'incoming/feed1/'

# List the objects in the specified S3 directory
objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=incoming_prefix)

# There should only be one file in this directory (the 'part-' file)
obj_key = objects.get('Contents', [{}])[0].get('Key')

if obj_key:
    source = {'Bucket': bucket_name, 'Key': obj_key}
    # Rename (actually copy to new name and then delete the original)
    s3_client.copy_object(Bucket=bucket_name, CopySource=source, Key=incoming_prefix + desired_filename)
    s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
