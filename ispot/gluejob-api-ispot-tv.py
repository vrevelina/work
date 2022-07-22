import http.client
import json
import pandas as pd
import sys
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
from awsglue.utils import getResolvedOptions
from config import ispot
import utils
from utils import JobError

class iSpot:
    
    def __init__(self, client_name, start_date, end_date) -> None:
        self.client_name = client_name
        self.start_date = start_date
        self.end_date = end_date
        self.url = ispot['request']["details"].format(start_date=start_date, end_date=end_date)
        self.conn = http.client.HTTPSConnection(ispot['request']["baseURL"])
        self.df = None
        
    def _decode(self, response):
        r = response.read()
        return json.loads(r.decode("utf-8"))
        
    def _get_token(self):
        self.conn.request(ispot['auth']["method"], ispot['auth']["URL"], ispot['auth']["payload"], ispot['auth']["headers"])
        response = self._decode(self.conn.getresponse())
        return response["access_token"]
    
    def _get_params(self):
        token = self._get_token()
        headers = {"Authorization": ispot['request']["authheaders"].format(token=token)}
        return ispot['request']["payload"], headers
        
    def _rename_fields(self, df):
        newcols = [c.replace(".","_") for c in df.columns]
        df.rename(columns={c: nc for c, nc in zip(df.columns, newcols)}, inplace=True)
        return df
    
    def get_data(self):
        payload, headers = self._get_params()
        self.conn.request(ispot['request']['method'], self.url, payload, headers)
        data = self._decode(self.conn.getresponse())
        df = pd.json_normalize(data['data'])
        df['client_name'] = self.client_name
        return self._rename_fields(df)

        
if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv, ['Client', 'StartDate', 'EndDate'])
        
        if args['EndDate'] == "None":
            args['EndDate'] = utils.get_end_date()
        if args['StartDate'] == "None":
            args['StartDate'] = utils.get_start_date(args['EndDate'], 'ispot')
        
        params = utils.get_params(args, 'ispot')
            
        rep = iSpot(params['client_name'], params['start_date'], params['end_date'])
        df = rep.get_data()
        utils.save_data(df, params['bucket'], params['destpath'], params['destfname'])
        
    except Exception as e:
        raise JobError(e, Job_Arguments = args)
    