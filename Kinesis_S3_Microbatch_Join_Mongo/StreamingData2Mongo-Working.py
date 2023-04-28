import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node AWS Glue Data Catalog
AWSS3DataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database="sensors",
    table_name="sensornames",
    transformation_ctx="AWSS3DataCatalog",
)

# Script generated for node Amazon Kinesis
AmazonKinesisDF = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="streamsensordata",
    additional_options={"startingPosition": "earliest", "inferSchema": "false"},
    transformation_ctx="AmazonKinesisDF",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesisDynamicDF = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
        AWSS3DataCatalogDF = (
            AWSS3DataCatalog.toDF()
        )
        AmazonKinesisDynamicDFtoDF = AmazonKinesisDynamicDF.toDF()
        JoinedStream = DynamicFrame.fromDF(
            AWSS3DataCatalogDF.join(
                AmazonKinesisDynamicDFtoDF,
                (
                    AWSS3DataCatalogDF["snsid"]
                    == AmazonKinesis_node1679155818965DF["sensorid"]
                ),
                "right",
            ),
            glueContext,
            "JoinedStream",
        )

        MongoDF = glueContext.write_dynamic_frame.from_options(frame=JoinedStream, connection_type="mongodb", connection_options=write_mongo_options,transformation_ctx="MongoDF",)


glueContext.forEachBatch(
    frame=AmazonKinesisDF,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
