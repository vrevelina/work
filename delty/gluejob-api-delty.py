import io
import re
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta
from config import delty
import sys
import utils
from utils import JobError

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
        r = re.search(delty['re_pattern'], req_result['cookies']['auditref'])
        return r.group(1)

    def _get_cookies(self, req_result):
        cookies = {k: req_result['cookies'][v] for k,v in delty['request']['cookie_keys'].items()}
        cookies['AuditRef'] = self._get_auditref(req_result)
        return cookies

    def _get_headers(self, req_result):
        headers = delty['request']['headers']
        headers['Authorization'] = req_result['auth']

    def request_auth(self):
        req = requests.post(delty['auth']['URL'], json=delty['auth']['credentials'], headers = delty['auth']['headers'])
        return req.json()

    def get_report(self):
        """Requests report.
        """
        req_result= self.request_auth()
        cookies = self._get_cookies(req_result)
        headers = self._get_headers(req_result)

        # GET request
        url = delty['request']['URL'].format(self.end_date)
        get_req = requests.get(url, cookies=cookies, headers=headers)

        # read report into dataframe
        return pd.read_csv(io.StringIO(get_req.content.decode('utf-8')))

if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['Client', 'StartDate', 'EndDate'])
        
        if args['EndDate'] == "None":
            args['EndDate'] = utils.get_end_date()
        if args['StartDate'] == "None":
            args['StartDate'] = utils.get_start_date(args['EndDate'], source='delty')
        
        params = utils.get_params(args, source='delty')

        rep = Delty(client_name=params['client_name'], start_date=params['start_date'], end_date=params['end_date'])
        df = rep.get_report()
        utils.save_data(df, params['bucket'], params['destpath'], params['destfname'])

    except Exception as e:
        raise JobError(e, Job_Arguments = args)
