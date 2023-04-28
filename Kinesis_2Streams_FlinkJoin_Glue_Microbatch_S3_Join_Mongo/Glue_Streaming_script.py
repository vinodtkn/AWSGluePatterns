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
    "username": "mongodbuser",
    "password": "mongodbpassword"
}

# Script generated for node Amazon S3
AmazonS3_node1680239408533 = glueContext.create_dynamic_frame.from_catalog(
    database="countrydb",
    table_name="countries",
    transformation_ctx="AmazonS3_node1680239408533",
)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1680238917667 = glueContext.create_data_frame.from_catalog(
    database="streamingpocdb",
    table_name="delivery_stream",
    additional_options={"startingPosition": "latest", "inferSchema": "false"},
    transformation_ctx="dataframe_AmazonKinesis_node1680238917667",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1680238917667 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
        Join_node1680239436181 = Join.apply(
            frame1=AmazonKinesis_node1680238917667,
            frame2=AmazonS3_node1680239408533,
            keys1=["countryid"],
            keys2=["country_id"],
            transformation_ctx="Join_node1680239436181",
        )

        # Script generated for node Drop Fields
        DropFields_node1680239514764 = DropFields.apply(
            frame=Join_node1680239436181,
            paths=["country_id", "injestion_time"],
            transformation_ctx="DropFields_node1680239514764",
        )

        AmazonS3_node1680238984196 = glueContext.write_dynamic_frame.from_options(
            frame=DropFields_node1680239514764,
            connection_type="mongodb",
            connection_options=write_mongo_options,
            transformation_ctx="AmazonS3_node1680238984196",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1680238917667,
    batch_function=processBatch,
    options={
        "windowSize": "1 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
