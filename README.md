# Work

**This repo is still evolving.**

Compilation of ETL scripts I wrote.

## GoogleTrends
Pulls data from Google Trends. Google Trends does not have an official API at the time of writing, I used [pytrends](https://pypi.org/project/pytrends/) to request the Google Trends data.

## QueryReport
Class I created to create CSV reports from data queried from AWS Athena databases. The implementation of this class includes sending the report into a SharePoint folder.

## API Scripts
The following ETL scripts were stored in AWS Glue to pull data from their APIs.
- ispot
- similarweb
- delty
- innovid
