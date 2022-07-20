import io
import re
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta
import config




def get_start_end_dates(end_date = None):
    if end_date is None:
        temp = datetime.today()
    else:
        temp = datetime.strptime(end_date, '%Y-%m-%d')
    e_date = temp - timedelta(days=temp.weekday()+1)
    e_str = e_date.strftime('%Y-%m-%d')
    s_date = e_date - timedelta(6)
    s_str = s_date.strftime('%Y-%m-%d')
    return s_str, e_str

class Delty:
    """ Class for representing Delty Reports """

    def __init__(
            self,
            client_name,
            end_date = None):

        """Constructor for Delty reports.

        Inputs:
            client_name (string): (ex: "Mercury Insurance")
            (optional) end_date: End date of the report. Date format: 'YYYY-MM-DD'. Any date will be converted to the latest Sunday before the given date.
                                 If the given date is a Sunday, it will default to the Sunday a week prior to the given date. (Default: today)
        """

        self.client_name = client_name
        self.start_date, self.end_date = get_start_end_dates(end_date)

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
        return post_req.json()

    def get_report(self):
        """Used to request the report.

            This function will get the report and save it into a local folder. Exact location will be printed after the function finished running.

        """
        req_result= self.request_auth()
        cookies = self._get_cookies(req_result)
        headers = self._get_headers(req_result)

        # GET request
        url = config.request['URL'].format(self.end_date)
        get_req = requests.get(url, cookies=cookies, headers=headers)

        # read report into dataframe
        return pd.read_csv(io.StringIO(get_req.content.decode('utf-8')))

    def save_report(self):
        csv_buffer = io.StringIO()
        df = self.get_report()
        df.to_csv(csv_buffer, sep = ",", index=False)
        s3 = boto3.resource('s3')
        fname = config.s3['destfname'].format(run_datetime=datetime.now(), client_name=self.client_name)
        s3_obj = s3.Object(bucket, config.s3['destpath'].format(client_name=self.client_name) + '/' + fname)
        
        s3_obj.put(Body=csv_buffer.getvalue())
        
        print("Raw report is saved.")

if __name__ == '__main__':
    delty = Delty(client_name=cname)
    delty.get_report()
    delty.save_report()
