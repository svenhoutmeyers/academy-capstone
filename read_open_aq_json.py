from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3 
import json 

#print(os.environ["AWS_ACCESS_KEY_ID"])
#print(os.environ["AWS_SECRET_ACCESS_KEY"])

packages = (
  "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
  "net.snowflake:snowflake-jdbc:3.13.3"
)

config = {
    "spark.jars.packages":",".join(packages),
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
}

conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
#spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
s3_path = "s3a://dataminded-academy-capstone-resources/raw/open_aq"
frame = spark.read.json(s3_path)

frame = (
frame.withColumn("lat", col("coordinates.latitude"))
    .withColumn("long", col("coordinates.longitude"))
    .withColumn("timestamp", to_timestamp(col("date.local")))
    .drop('coordinates','date')
)

#frame.printSchema()
#frame.show()


client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId='snowflake/capstone/login'
)
database_secrets = json.loads(response['SecretString'])

options = {
    "sfURL": database_secrets['URL'],
    "sfUser": database_secrets['USER_NAME'],
    "sfPassword": database_secrets['PASSWORD'],
    "sfDatabase":database_secrets['DATABASE'],
    "sfSchema":"SVEN",
    "sfRole": database_secrets['ROLE'],
    "sfWarehouse": database_secrets['WAREHOUSE']
}

(
  frame.write
    .format("snowflake")
    .options(**options)
    .option("dbtable", "OPEN_AQ")
    .mode("overwrite")
    .save()
)
