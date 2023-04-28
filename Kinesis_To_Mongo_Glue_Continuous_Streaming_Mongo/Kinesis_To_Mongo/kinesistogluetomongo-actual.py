import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

write_uri = "mongodb+srv://testdb.mongodb.net/"

write_mongo_options = {
    "uri": write_uri,
    "database": "test",
    "collection": "test5",
    "username": "mongouser",
    "password": "mongopwd"
}

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1678912302013 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:849259389086:stream/testKinesisStream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_AmazonKinesis_node1678912302013",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1678912302013 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
    AmazonS3_node1678912313002 = glueContext.write_dynamic_frame.from_options(frame=AmazonKinesis_node1678912302013, connection_type="mongodb", connection_options=write_mongo_options,transformation_ctx="AmazonS3_node1678912313002",)
 
 
glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1678912302013,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
        "outputMode": "append"
    },
)

job.commit()
