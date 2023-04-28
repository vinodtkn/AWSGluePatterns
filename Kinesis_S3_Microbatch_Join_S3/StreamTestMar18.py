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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1679155986652 = glueContext.create_dynamic_frame.from_catalog(
    database="sensors",
    table_name="sensornames",
    transformation_ctx="AWSGlueDataCatalog_node1679155986652",
)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1679155818965 = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="streamsensordata",
    additional_options={"startingPosition": "earliest", "inferSchema": "false"},
    transformation_ctx="dataframe_AmazonKinesis_node1679155818965",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1679155818965 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
        AWSGlueDataCatalog_node1679155986652DF = (
            AWSGlueDataCatalog_node1679155986652.toDF()
        )
        AmazonKinesis_node1679155818965DF = AmazonKinesis_node1679155818965.toDF()
        Join_node1679156045821 = DynamicFrame.fromDF(
            AWSGlueDataCatalog_node1679155986652DF.join(
                AmazonKinesis_node1679155818965DF,
                (
                    AWSGlueDataCatalog_node1679155986652DF["snsid"]
                    == AmazonKinesis_node1679155818965DF["sensorid"]
                ),
                "left",
            ),
            glueContext,
            "Join_node1679156045821",
        )

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1679156079398_path = (
            "s3://vinod-glue-poc/data/streamingoutput"
            + "/ingest_year="
            + "{:0>4}".format(str(year))
            + "/ingest_month="
            + "{:0>2}".format(str(month))
            + "/ingest_day="
            + "{:0>2}".format(str(day))
            + "/ingest_hour="
            + "{:0>2}".format(str(hour))
            + "/"
        )
        AmazonS3_node1679156079398 = glueContext.write_dynamic_frame.from_options(
            frame=Join_node1679156045821,
            connection_type="s3",
            format="json",
            connection_options={
                "path": AmazonS3_node1679156079398_path,
                "partitionKeys": [],
            },
            transformation_ctx="AmazonS3_node1679156079398",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1679155818965,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
