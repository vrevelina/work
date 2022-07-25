import boto3
from QueryReport.utils import query_results
import config


class QueryReport:
    def __init__(self, db_name, channel, site, query):
        self.db_name = db_name
        self.channel = channel
        self.site = site
        self.query = query
        self.params = None
        self.aws_path = None
        self.df = None
        self.save_path = None

    def _create_params(self):
        params = {
            "region": config.access["region"],
            "database": self.db_name,
            "bucket": "005-datalake",
            "path": "query",
            "query": self.query,
        }
        self.params = params

    def get_data(self):
        self._create_params()
        session = boto3.Session(
            aws_access_key_id=config.access["id"],
            aws_secret_access_key=config.access["secret"],
            region_name=config.access["region"],
        )
        self.aws_path, self.df = query_results(session, self.params)
        print("AWS Data Path: ", self.aws_path)

    def get_fname(self, start_date, end_date):
        date_details = f"{start_date:%m-%d-%y}_{end_date:%m-%d-%y}"
        return f"{self.db_name.capitalize()}_{self.channel}_{self.site}_Data_{date_details}.csv"
