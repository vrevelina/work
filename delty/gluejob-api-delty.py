import io
import re
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta
import config
import sys

class Delty:
    """ Class for representing Delty Reports """

    def __init__(
            self,
            client_name,
            start_date = None,
            end_date = None):

        """Constructor for Delty reports.

        Inputs:
            client_name (string): (ex: "Mercury Insurance")
            (optional) end_date: End date of the report. Date format: 'YYYY-MM-DD'. Any date will be converted to the latest Sunday before the given date.
                                 If the given date is a Sunday, it will default to the Sunday a week prior to the given date. (Default: today)
        """

        self.client_name = client_name
        self.start_date = start_date
        self.end_date = end_date

    def _get_auditref(self, req_result):
        r = re.search(config.re_pattern, req_result['cookies']['auditref'])
        return r.group(1)

    def _get_cookies(self, req_result):
        cookies = {k: req_result['cookies'][v] for k,v in config.request['cookie_keys'].items()}
        cookies['AuditRef'] = self._get_auditref(req_result)
        return cookies

    def _get_headers(self, req_result):
        headers = config.request['headers']
        headers['Authorization'] = req_result['auth']

    def request_auth(self):
        req = requests.post(config.auth['URL'], json=config.auth['credentials'], headers = config.auth['headers'])
        return req.json()

    def get_report(self):
        """Requests report.
        """
        req_result= self.request_auth()
        cookies = self._get_cookies(req_result)
        headers = self._get_headers(req_result)

        # GET request
        url = config.request['URL'].format(self.end_date)
        get_req = requests.get(url, cookies=cookies, headers=headers)

        # read report into dataframe
        return pd.read_csv(io.StringIO(get_req.content.decode('utf-8')))


def get_end_date():
    end_date = datetime.now() + relativedelta(days=-1)
    return end_date.strftime("%Y-%m-%d")

def get_start_date(end_date: str):
    start_date = datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(days=-6)
    return start_date.strftime("%Y-%m-%d")
        
def save_report(df, bucket, destpath, destfname):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep = ",", index=False)
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(bucket, destpath+destfname)
    s3_obj.put(Body=csv_buffer.getvalue())
    print("Data has been saved.")
    
def _get_params(args):
    params = {}
    params['client_name'] = args['Client']
    params['start_date'] = args['StartDate']
    params['end_date'] = args['EndDate']
    params['bucket'] = config.s3["bucket"]
    params['destpath'] = config.s3["destpath"].format(client_name=args['Client'])
    params['destfname'] = config.s3["destfname"].format(run_datetime=datetime.now(), client_name=args['Client'])
    return params
    
class JobError(Exception):
    def __init__(self, error, **kwargs):
        super().__init__(error)
        self.error_type = type(error).__name__
        self._kwargs = kwargs
        
    def __str__(self):
        kwargs = ", ".join(str(k) + " = " + str(v) for k, v in self._kwargs.items())
        return f"{self.error_type}: {super().__str__()} || {kwargs}"


if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['Client', 'StartDate', 'EndDate'])
        
        if args['EndDate'] == "None":
            args['EndDate'] = get_end_date()
        if args['StartDate'] == "None":
            args['StartDate'] = get_start_date(args['EndDate'])
        
        params = _get_params(args)

        delty = Delty(client_name=params['client_name'], start_date=params['start_date'], end_date=params['end_date'])
        df = delty.get_report()
        save_report(df, params['bucket'], params['destpath'], params['destfname'])

    except Exception as e:
        raise JobError(e, Job_Arguments = args)
