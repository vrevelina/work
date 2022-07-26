# QueryReport
Python class.
Pulls data with a given query from AWS Athena database(s).
Query results are still saved into S3.

# gluejob-QueryReport.py
Implementation of the QueryReport class, this was written in [AWS Glue Job](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html).
1. It retrieves queries from [DynamoDB](https://aws.amazon.com/dynamodb) tables using a given Query ID.
2. Queries from [AWS Athena](https://aws.amazon.com/athena) databases, saves the result into [S3](https://aws.amazon.com/s3/) and a [pandas dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
3. Sends a request to sharepoint to post the data in CSV format.
