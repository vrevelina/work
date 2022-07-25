from QueryReport import QueryReport
import utils
import boto3
import sys
import pandas as pd
from awsglue.utils import getResolvedOptions
import config
import sharepy

args = getResolvedOptions(sys.argv, ["QueryID", "Channel"])
query_id = args["QueryID"]
db_name = query_id.split("-")[0]
channel = args["Channel"]


def get_query(query_id):
    """Fetches query from AWS dynamoDB

    Args:
        query_id (str): query ID

    Returns:
        query (str): query from dynamoDB
    """
    dyndb_client = boto3.client("dynamodb", region_name=config.access["region"])
    response = dyndb_client.get_item(
        TableName="dyndb-config-query", Key={"QueryID": {"S": query_id}}
    )
    if "Item" in response:
        return response["Item"]["Query"]["S"]
    else:
        print("No config settings")
        sys.exit()


def get_date_details(df, datecol):
    tdf = df.copy()
    tdf[datecol] = pd.to_datetime(tdf[datecol])
    return tdf[datecol].min(), tdf[datecol].max()


def to_sharepoint(df, sp_folder, filename):
    s = sharepy.connect(
        config.sharepoint["baseURL"],
        username=config.sharepoint["username"],
        password=config.sharepoint["password"],
    )
    url = config.sharepoint["URL"].format(folder=sp_folder, fname=filename)
    data = utils.export_csv(df)
    r = s.post(url, data=data, headers={"content-length": str(len(data))})
    print(r)
    print(r.content)


if __name__ == "__main__":

    site = utils.get_site(query_id)
    query = get_query(query_id)

    print(f"QueryID: {query_id}")
    print(f"Site: {site}")
    print(f"Query:\n{query}")

    report = QueryReport(db_name, channel, site, query)
    report.get_data()
    print(report.df.columns)
    df = utils.reorder_cols(report.df, site)
    sdate, edate = get_date_details(report.df, "Date")
    fname = report.get_fname(sdate, edate)
    sp_folder = utils.get_sp_folder(channel, site)
    print(sp_folder)
    to_sharepoint(df, sp_folder, fname)
