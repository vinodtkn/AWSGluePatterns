import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import expr

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

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_1 = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="streamsensordata",
    additional_options={"startingPosition": "earliest", "inferSchema": "false"},
    transformation_ctx="dataframe_AmazonKinesis_1",
)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_2 = glueContext.create_data_frame.from_catalog(
    database="dyanamicsensordata",
    table_name="sensorstream3",
    additional_options={"startingPosition": "earliest", "inferSchema": "false"},
    transformation_ctx="dataframe_AmazonKinesis_2",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        dataframe_AmazonKinesis_jointmp1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Join
       
        dataframe_AmazonKinesis_jointmp_df1 = dataframe_AmazonKinesis_jointmp1.toDF()
        
        #dataframe_AmazonKinesis_jointmp_df1 = dataframe_AmazonKinesis_jointmp_df1.select("sensorid", "currenttemperature", "status")
        
        dataframe_AmazonKinesis_jointmp2 = DynamicFrame.fromDF(
            dataframe_AmazonKinesis_2, glueContext, "from_data_frame2"
        )
        
        dataframe_AmazonKinesis_jointmp_df2 = dataframe_AmazonKinesis_jointmp2.toDF()
        
        #dataframe_AmazonKinesis_jointmp_df2 = dataframe_AmazonKinesis_jointmp_df2.select("snsid", "currenttem", "sts")

        # Join_node_final = DynamicFrame.fromDF(
        #     dataframe_AmazonKinesis_jointmp_df1.join(
        #         dataframe_AmazonKinesis_jointmp_df2,
        #         (
        #             dataframe_AmazonKinesis_jointmp_df1["sensorid"]
        #             == dataframe_AmazonKinesis_jointmp_df2["snsid"]
        #         ),
        #         "left",
        #     ),
        #     glueContext,
        #     "Join_node1679156045821",
        # )
        
        # Join wwo dynamic frames on an equality join
        dfyjoin = dataframe_AmazonKinesis_jointmp_df1.join(["sensorid"],["snsid"],dataframe_AmazonKinesis_jointmp_df2)


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
            frame=dfyjoin,
            connection_type="s3",
            format="json",
            connection_options={
                "path": AmazonS3_node1679156079398_path,
                "partitionKeys": [],
            },
            transformation_ctx="AmazonS3_node1679156079398",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_1,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": "s3://vinod-glue-poc/data/streamingoutput/",
    },
)


job.commit()
