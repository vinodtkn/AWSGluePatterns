import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

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
    "collection": "test4",
    "username": "mongouser",
    "password": "mongopwd"
}

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="yyz-tickets", table_name="tickets", transformation_ctx="S3bucket_node1"
)

# Script generated for node Ticket_Mapping
Ticket_Mapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("tag_number_masked", "string", "tag_number_masked", "string"),
        ("date_of_infraction", "string", "date_of_infraction", "string"),
        ("ticket_date", "string", "ticket_date", "string"),
        ("ticket_number", "decimal", "ticket_number", "string"),
        ("officer", "decimal", "officer_name", "decimal"),
        ("infraction_code", "decimal", "infraction_code", "decimal"),
        ("infraction_description", "string", "infraction_description", "string"),
        ("set_fine_amount", "decimal", "set_fine_amount", "string"),
        ("time_of_infraction", "decimal", "time_of_infraction", "decimal"),
    ],
    transformation_ctx="Ticket_Mapping_node2",
)



# Script generated for node S3 bucket
#S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
#    frame=Ticket_Mapping_node2,
#    connection_type="s3",
#    format="glueparquet",
#    connection_options={
#        "path": "s3://glue-studio-blog-849259389086",
#        "partitionKeys": [],
#    },
#    format_options={"compression": "gzip"},
#    transformation_ctx="S3bucket_node3",
#)

glueContext.write_dynamic_frame.from_options(Ticket_Mapping_node2, connection_type="mongodb", connection_options=write_mongo_options)

job.commit()
