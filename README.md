# Crime Data ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline to process crime data from the city of Los Angeles. The pipeline utilizes an EC2 instance to process the data. We will extract the data through an API provided by Los Angeles Open Data.

## Workflow Overview

EC2 Instance Setup: Use an EC2 instance to process the data efficiently.

Airflow Automation: Employ Apache Airflow to automate the entire ETL process. The data extraction will be scheduled weekly to ensure regular updates.

S3 Buckets for Storage:

One S3 bucket stores raw, unprocessed data.
Another S3 bucket stores cleaned and transformed data.
Snowpipe Integration:

After successful operation with Airflow, initiate Snowpipe to load the processed data into Snowflake.
Tableau Visualization:

Utilize Tableau to create compelling visualizations based on the data stored in Snowflake.

## Contribution
Feel free to contribute, open issues, or suggest improvements. If you encounter any problems or have questions, create an issue in this repository.
