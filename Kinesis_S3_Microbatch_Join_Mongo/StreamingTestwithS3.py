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
    "collection": "test6",
    "username": "mongouser",
    "password": "mongopwd"
}

# Script generated for node AWS Glue Data Catalog
dataframe_AWSGlueDataCatalog_node1679055428923 = (
    glueContext.create_data_frame.from_catalog(
        database="order",
        table_name="orders",
        additional_options={"startingPosition": "earliest", "inferSchema": "true"},
        transformation_ctx="dataframe_AWSGlueDataCatalog_node1679055428923",
    )
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1679055442479 = glueContext.create_dynamic_frame.from_catalog(
    database="customerdb",
    table_name="customers",
    transformation_ctx="AWSGlueDataCatalog_node1679055442479",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AWSGlueDataCatalog_node1679055428923 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
        Join_node1679055466607 = Join.apply(
            frame1=AWSGlueDataCatalog_node1679055442479,
            frame2=AWSGlueDataCatalog_node1679055428923,
            keys1=["custid"],
            keys2=["customerid"],
            transformation_ctx="Join_node1679055466607",
        )
        
        AmazonS3_node1679055494204 = glueContext.write_dynamic_frame.from_options(frame=Join_node1679055466607, connection_type="mongodb", connection_options=write_mongo_options,transformation_ctx="AmazonS3_node1678912313002",)


glueContext.forEachBatch(
    frame=dataframe_AWSGlueDataCatalog_node1679055428923,
    batch_function=processBatch,
    options={
        "windowSize": "200 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
