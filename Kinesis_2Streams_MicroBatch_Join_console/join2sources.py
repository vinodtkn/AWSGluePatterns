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
dataframe_AmazonKinesis_node1679348688125 = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="streamsensordata",
    additional_options={"startingPosition": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_AmazonKinesis_node1679348688125",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1679348779490 = glueContext.create_dynamic_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="sensorstream3",
    transformation_ctx="AWSGlueDataCatalog_node1679348779490",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1679348688125 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        
        df=Join.apply(AmazonKinesis_node1679348688125, AWSGlueDataCatalog_node1679348779490, 'sensorid', 'snsid')
        
        df.show()


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1679348688125,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
