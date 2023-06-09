## Pre-steps:

1. Setup the AWS CLI in the local system or system where you are going to run the input data generation code (python code) & the kinesis stream name has to be provided as input to the python code- You can use Kinesis Data Generator instead
2. Create Glue Data Catalog for the sources you are trying to connect (you can define the schema for the sources)

## Creating a Glue Job:

1. First create a UI job using Glue UI (where you define source, destination, transformation steps)
2. Override the code generated to change the necessary steps

## Steps:

1. Run the input data (Stream generation) code in python
2. Data starts flowing into Kinesis Data Stream
3. Run the KDA to combine the datastreams and write it to the output stream
3. Run the glue job
4. Data starts loading into Mongodb

## Architecture:

![AWS2Mongo_Streaming_Architecture](https://user-images.githubusercontent.com/82138543/235251667-f7212625-ef70-4c23-9da0-3ff1d5bdaaa1.png)

## ER Diagram: (Data Catalog)

![AWS2Mongo_ER](https://user-images.githubusercontent.com/82138543/235251662-1e4087e9-836f-4435-b8e3-de4dbce7b4c4.png)
