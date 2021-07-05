# Sparkify Datalake using Spark
## General info
In this project, I build an ETL using pyspark.
The project simply extract data from s3 and transform in into star schema and write back to AWS S3 in parquet format
## Technologies
Project is created with:
* Pyspark, Spark 2.x
* AWS service
## Project files:
* Sparkfiy_note.ipynb: is a notebook file that can be run on AWS EMR cluster to test out the result
* etl.py: Script used to run the etl. This script would extract data from s3 and transform them into parquet files
and store them in user-defined s3 uri
  
*dl.cfg: AWS configuration
## Setup
* Please input your AWS Key, AWS secret in dl.cfg and output_data in elt.py for storing result parquet files in dl.cfg
* Create EMR cluster
* Using SCP to transfer etl.py and dl.cfg in this project into the EC2 master node.
* Install all required library in etl.py
  pip install configparser
  pip install pexpect
* Run script by command: spark-submit etl.py
## Result
After successful executing create_tables.py and etl.py, it should create parquet files with correct partitioning  : songplays, times, users, songs, artists.

