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

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1679076031159 = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="streamsensordata",
    additional_options={"startingPosition": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_AmazonKinesis_node1679076031159",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1679076033951 = glueContext.create_dynamic_frame.from_catalog(
    database="sensors",
    table_name="sensornames",
    transformation_ctx="AWSGlueDataCatalog_node1679076033951",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1679076031159 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
        Join_node1679076038160 = Join.apply(
            frame1=AWSGlueDataCatalog_node1679076033951,
            frame2=AmazonKinesis_node1679076031159,
            keys1=["snsid"],
            keys2=["sensorid"],
            transformation_ctx="Join_node1679076038160",
        )

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1679076040110_path = (
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
        AmazonS3_node1679076040110 = glueContext.write_dynamic_frame.from_options(
            frame=Join_node1679076038160,
            connection_type="s3",
            format="json",
            connection_options={
                "path": AmazonS3_node1679076040110_path,
                "partitionKeys": [],
            },
            transformation_ctx="AmazonS3_node1679076040110",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1679076031159,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
