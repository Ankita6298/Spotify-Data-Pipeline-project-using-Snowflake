# Spotify-Data-Pipeline-project-using-Snowflake
### Introduction
In this project, we build an ETL(Extract, Transform, Load) pipeline, in which pipeline retrieve data from Spotify using Spotify API, transform it to a desired format, load it into an AWS data store and store data in Snowflake data warehouse and used Power BI tool for data visualization.

### Project Execution Flow
1) Integrating with Spotify API and extracting Data using Python
2) Deploying code on AWS Lambda for Data Extraction
3) Adding trigger to run the Lambda function for extraction automatically
4) Writing transformation function
5) Building automated trigger on transformation function
6) Store transformed data on S3 properly
7) Load all data onto Snowflake data warehouse
8) Used Power BI for data visualization

### Technology Used
* Programming Language - Python
* Snowflake
* Power BI
* Amazon Web Service (AWS)
  1) Lambda
  2) CloudWatch
  3) S3 (Simple Storage Service)
  4) S3 Trigger

### Dataset/API
Spotify API contain the information about Album, Artist and Songs - [Spotipy API](https://spotipy.readthedocs.io/en/2.22.1/)

### Installed Packages
```
pip install pandas
pip install numpy
pip install spotipy

```
