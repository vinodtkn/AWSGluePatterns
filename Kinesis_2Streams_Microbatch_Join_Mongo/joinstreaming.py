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
    "collection": "test10",
    "username": "mongodbuser",
    "password": "mongodbpassword"
}

# Script generated for node Amazon Kinesis-1
AWSGlueDataCatalog_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="order",
    table_name="orders",
    additional_options={"startingPosition": "earliest", "inferSchema": "true"},
    transformation_ctx="AWSGlueDataCatalog_node1",
)

# Script generated for node Amazon Kinesis-2
AWSGlueDataCatalog_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="person",
    table_name="persons",
    additional_options={"startingPosition": "earliest", "inferSchema": "true"},
    transformation_ctx="AWSGlueDataCatalog_node2",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        Join_node1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        
        # Script generated for node Join
        Join_node_tmp = Join.apply(
            frame1=Join_node1,
            frame2=person_lookup_static_df,
            keys1=["customerid"],
            keys2=["custid"],
            transformation_ctx="Join_node_tmp",
        )

        MongoWriteNode = glueContext.write_dynamic_frame.from_options(frame=Join_node_tmp, connection_type="mongodb", connection_options=write_mongo_options,transformation_ctx="MongoWriteNode",)


glueContext.forEachBatch(
    frame=AWSGlueDataCatalog_node1,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
