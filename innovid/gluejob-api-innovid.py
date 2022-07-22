import io
import json
import time
import boto3
import requests
import pandas as pd
from zipfile import ZipFile
from pathlib import Path
from zipfile import ZipFile
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from awsglue.utils import getResolvedOptions
import config

def get_start_end_dates(end_date=None, days_back=None):
    if end_date is None:
        e_date = datetime.today() - timedelta(1)
        e_str = e_date.strftime('%Y-%m-%d')
    else:
        e_date = datetime.strptime(end_date, '%Y-%m-%d')
        e_str = end_date
        
    if days_back is None:
        days_back = 4
    s_date = e_date - timedelta(days_back)
    s_str = s_date.strftime('%Y-%m-%d')
    return s_str, e_str

def get_end_date():
    end_date = datetime.now() + relativedelta(days=-1)
    return end_date.strftime("%Y-%m-%d")

def get_start_date(end_date: str):
    start_date = datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(days=-4)
    return start_date.strftime("%Y-%m-%d")

class Innovid:
    """ Class for representing Innovid Reports """

    def __init__(
            self,
            client_name,
            start_date = None,
            end_date = None):

        """Constructor for Innovid Report.

        Input:
            client_name (string)
            (optional) end_date (string): End date of the report. Date format: 'YYYY-MM-DD' (Default: yesterday, inclusive)
            (optional) days_back (int): Number of days from the end date to be included in the report. (Default: 3, inclusive)

        """
        self.client_name = client_name
        self.start_date = start_date
        self.end_date = end_date
        self.report_url = None
        self.df = pd.DataFrame()
        self.file_name = destfname
        self.destpath = config.s3['destpath'].format(client_name=client_name)

    def request_report(self):
        """Used to request the report.

            This function will use a get request using a predetermined report URL with start date and end date as specified
            in the constructor. After it has successfully requested the report, a status token will be assigned into the
            constructor as an attribute.

        """

        token_req_url = config.request['tokenURL'].format(client_id=config.credentials['client_id'][self.client_name], advertiser_id=config.credentials['advertiser_id'], start_date=self.start_date, end_date=self.end_date)

        # at this point, the report is being requested
        try:
            r = requests.get(token_req_url, auth=HTTPBasicAuth(user_email, user_password))
            rs_token = json.loads(r.text)['data']['reportStatusToken']

            print('The report has been requested.')
        except:
            raise Exception(requests.get(token_req_url, auth=HTTPBasicAuth(user_email, user_password)).text)

        return rs_token

    def check_report_status(self, status_token):
        """Checks the status of a report and sets the report url instance variable.

            This function will check the status of the requested report. Must be used after the 'request_report' method is called.
        There are 3 possible report status: 'IN_PROCESS' (report is still being built), 'FAIL' (failed to build the report),
        READY (report is ready).

        """
        req_url = config.request['statusURL'].format(token = status_token)
        req_data = json.loads(requests.get(req_url, auth=HTTPBasicAuth(user_email, user_password)).text)['data']

        report_status = req_data['reportStatus']

        if report_status == 'IN_PROCESS':
            print('Report is not ready yet.')
        elif report_status == 'FAIL':
            print('Report request has failed.')
        elif report_status == 'READY':
            self.report_url = req_data['reportUrl']
            print('Report is ready')

    def get_report(self):
        """Used to request the report.

            This function will get the report and save it into a local folder. Exact location will be printed after the function has finished running.

        """

        rs_token = self.request_report()

        # check the report status every minute
        # prints time and time elapsed every 5 minutes
        # the loop will stop trying to get the report after 30 minutes have elapsed.
        start_time = time.time()
        print("In the process of getting the report... \nCurrent time:", time.ctime())
        self.check_report_status(rs_token)
        while self.report_url is None:
            time.sleep(60)
            time_elapsed = time.time()-start_time
            self.check_report_status(rs_token)
            if time_elapsed>1800:
                print("Failed to get the report after 30 minutes.")
                break
            elif round(time_elapsed/60)%3==0:
                print("Still getting the report...\nCurrent time: {}.\nTime elapsed: {} minutes".format(time.ctime(), round(time_elapsed/60)))

        # if report is ready before 30 minutes is up:
        else:
            # get zipped folder inside the url
            r = requests.get(self.report_url)
            temp = ZipFile(io.BytesIO(r.content))
            time_elapsed = time.time()-start_time

            print('Time elapsed: {round(time_elapsed/60, 2)} minute(s).')
            return pd.read_csv(temp.open(temp.namelist()[0]))

    def save_report(self):
        csv_buffer = io.StringIO()
        self.df.to_csv(csv_buffer, sep = ",", index=False)
        s3 = boto3.resource('s3')
        fname = config.s3['destfname'].format(run_datetime=datetime.now(), client_name=self.client_name)
        s3_obj = s3.Object(config.s3['bucket'], self.destpath + '/' + fname)
        
        s3_obj.put(Body=csv_buffer.getvalue())
        
        print("Raw report is saved.")

def _save_data(df, bucket, destpath, destfname):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
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

        innovid = Innovid(client_name=params['client_name'], start_date=params['start_date'], end_date=params['end_date'])
        df = innovid.get_report()
        _save_data(df, params['bucket'], params['destpath'], params['destfname'])

    except Exception as e:
        raise JobError(e, Job_Arguments = args)

