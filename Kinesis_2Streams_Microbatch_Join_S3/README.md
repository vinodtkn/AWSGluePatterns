## Pre-steps:

1. Setup the AWS CLI in the local system or system where you are going to run the input data generation code (python code) & the kinesis stream name has to be provided as input to the python code- You can use Kinesis Data Generator instead
2. Create Glue Data Catalog for the sources you are trying to connect (you can define the schema for the sources)

## Creating a Glue Job:

1. First create a UI job using Glue UI (where you define source, destination, transformation steps)
2. Override the code generated to change the necessary steps

## Steps:

1. Run the input data (Stream generation) code in python
2. Data starts flowing into Kinesis Data Stream
3. Run the glue job
4. Data starts loading into s3

## Architecture:

![Kinesis_2Streams_Microbatch_Join_S3 drawio](https://user-images.githubusercontent.com/82138543/235253010-04ff574e-b891-477b-b4e0-f30615f52f99.png)

