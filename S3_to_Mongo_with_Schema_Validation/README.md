## Pre-steps:

1. Store the csv lookup data in S3 bucket
2. Create Glue Data Catalog for the sources you are trying to connect (you can define the schema for the sources)

## Creating a Glue Job:

1. First create a UI job using Glue UI (where you define source, destination, transformation steps)
2. Override the code generated to change the necessary steps

## Steps:

1. Run the glue job
2. Data is loaded into Mongodb collection from csv lookup file(batch process)

## Architecture:

![S3_to_Mongo_with_Schema_Validation drawio](https://user-images.githubusercontent.com/82138543/235254505-c9f996d7-3040-4a97-bf7b-7de7ec3d66a6.png)

