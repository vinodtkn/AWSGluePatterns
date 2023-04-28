## Pre-steps:

1. Setup the AWS CLI in the local system or system where you are going to run the input data generation code (python code) & the kinesis stream name has to be provided as input to the python code- You can use Kinesis Data Generator instead
2. Store the csv lookup file in S3  bucket
2. Create Glue Data Catalog for the sources you are trying to connect (you can define the schema for the sources)

## Creating a Glue Job:

1. First create a UI job using Glue UI (where you define source, destination, transformation steps)
2. Override the code generated to change the necessary steps

## Steps:

1. Run the input data (Stream generation) code in python
2. Data starts flowing into Kinesis Data Stream
3. Run the glue job
4. Data gets stored in destination S3 bucket

## Architecture:

![Kinesis_S3_Microbatch_Join_S3 drawio](https://user-images.githubusercontent.com/82138543/235253541-864d55e7-d79f-4f03-9bfa-b2879146fcc1.png)

