import http.client
import json
import pandas as pd
import sys
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
import config
from awsglue.utils import getResolvedOptions

class iSpot:
    
    def __init__(self, client_name, start_date, end_date) -> None:
        self.client_name = client_name
        self.start_date = start_date
        self.end_date = end_date
        self.url = config.request["details"].format(start_date=start_date, end_date=end_date)
        self.conn = http.client.HTTPSConnection(config.request["baseURL"])
        self.df = None
        
    def _decode(self, response):
        r = response.read()
        return json.loads(r.decode("utf-8"))
        
    def _get_token(self):
        self.conn.request(config.auth["method"], config.auth["URL"], config.auth["payload"], config.auth["headers"])
        response = self._decode(self.conn.getresponse())
        return response["access_token"]
    
    def _get_params(self):
        token = self._get_token()
        headers = {"Authorization": config.request["authheaders"].format(token=token)}
        return config.request["payload"], headers
        
    def _rename_fields(self, df):
        newcols = [c.replace(".","_") for c in df.columns]
        df.rename(columns={c: nc for c, nc in zip(df.columns, newcols)}, inplace=True)
        return df
    
    def get_data(self):
        payload, headers = self._get_params()
        self.conn.request(config.request["method"], self.url, payload, headers)
        data = self._decode(self.conn.getresponse())
        df = pd.json_normalize(data['data'])
        df['client_name'] = self.client_name
        return self._rename_fields(df)
        
def get_end_date():
    end_date = datetime.now() + relativedelta(days=-1)
    return end_date.strftime("%Y-%m-%d")

def get_start_date(end_date: str):
    start_date = datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(months=-1)
    return start_date.strftime("%Y-%m-%d")
        
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
    params['destfname'] = f"{datetime.now():%Y%m%d_%H%M%S}_iSpot_TV_{args['Client']}_{args['StartDate']}_{args['EndDate']}.csv"
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
            
        req = iSpot(params['client_name'], params['start_date'], params['end_date'])
        df = req.get_data()
        _save_data(df, params['bucket'], params['destpath'], params['destfname'])
    except Exception as e:
        raise JobError(e, Job_Arguments = args)
    