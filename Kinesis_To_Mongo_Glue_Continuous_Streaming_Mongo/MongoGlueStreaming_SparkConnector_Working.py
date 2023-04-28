import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.job import Job

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


spark = SparkSession.\
        builder.\
        appName("streamingExampleRead").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13::10.1.1').\
        getOrCreate()

query=(spark.readStream.format("com.mongodb.spark.sql.connector.MongoTableProvider")\
.option('spark.mongodb.connection.uri', 'mongodb+srv://mongouser:mongopassword@testdb.mongodb.net/?retryWrites=true&w=majority')\
    	.option('spark.mongodb.database', 'test') \
    	.option('spark.mongodb.collection', 'test4') \
.option('spark.mongodb.change.stream.publish.full.document.only','true') \
    	.option("forceDeleteTempCheckpointLocation", "true") \
    	.load())
# Print schema of query DataFrame
query.printSchema()

query.writeStream\
    .format("com.mongodb.spark.sql.connector.MongoTableProvider")\
     .option("checkpointLocation", "s3://gluetestingbucketmar25/checkpoint/")\
  .option("forceDeleteTempCheckpointLocation", "true")\
    .option('spark.mongodb.connection.uri', 'mongodb+srv://mongouser:mongopassword@testdb.mongodb.net/?retryWrites=true&w=majority')\
    .option('spark.mongodb.database', 'test')\
    .option('spark.mongodb.collection', 'test7')\
    .trigger(continuous="10 seconds")\
    .outputMode("append")\
    .start().awaitTermination()
