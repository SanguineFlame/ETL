from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://database-1.c4iwusdi8hal.us-east-1.rds.amazonaws.com:3306/chrome_schema") \
    .option("dbtable", "myTable") \
    .option("user", "z") \
    .option("password", "z") \
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
