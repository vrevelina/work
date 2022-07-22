import io
import sys
import json
import boto3
import requests
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions
from config import similarweb
from utils import JobError
import utils

class SimilarWebApp:
    
    def __init__(self, client_name, app_id, data_type, granularity, start_month, end_month, app_os = "Google") -> None:
        self.client_name = client_name
        self.app_id = app_id
        self.data_type = data_type
        self.granularity = granularity
        self.start_month = start_month
        self.end_month = end_month
        self.app_os = app_os
        
    def _get_url(self):
        return similarweb['request']['URL'].format(app_id = self.app_id,
                                            data_type = similarweb['data_types'][self.data_type],
                                            api_key = similarweb['request']['APIKey'],
                                            start_month = self.start_month,
                                            end_month = self.end_month,
                                            granularity = self.granularity)
        
    def _get_response(self):
        url = self._get_url()
        return requests.request(similarweb['request']['method'], url, headers={}, data={})
    
    def _load_response(self):
        response = self._get_response()
        return json.loads(response.text)[self.data_type]
    
    def _get_raw_data(self):
        try:
            raw_data = self._load_response()
            return pd.json_normalize(raw_data)
        except:
            print(f"Error Code: {self._get_response()}")
            
    def _add_details(self, df):
        df['client_name'] = self.client_name
        df['app_id'] = self.app_id
        df['app_os'] = self.app_os
        df['data_type'] = self.data_type
        df['granularity'] = self.granularity
        df['run_datetime'] = datetime.now()
        return df
            
    def get_data(self):
        df = self._get_raw_data()
        return self._add_details(df)
        
    def get_fname(self):
        return f'{datetime.now():%Y%m%d_%H%M%S}_SimilarWebApp_{self.data_type}_{self.app_os}_{self.client_name}_{self.granularity}_{self.start_month}_{self.end_month}.csv'
    
if __name__ == "__main__":
    
    args = getResolvedOptions(sys.argv, ['Client', 'AppID', 'DataType', 'Granularity', 'StartMonth', 'EndMonth', 'AppOS'])
    
    try:
        params = utils.get_params(args, 'similarweb')
        app = SimilarWebApp(client_name = params['client_name'],
                            app_id = params['app_id'],
                            data_type = params['data_type'],
                            granularity = params['granularity'],
                            start_month = params['start_month'],
                            end_month = params['end_month'],
                            app_os = params['app_os'])
                            
        app_df = app.get_data()
        utils.save_data(app_df, params['bucket'], params['destpath'], app.get_fname())
        print(f"Saved to: {params['destpath']}{app.get_fname()}")
        
    except Exception as e:
        args['ErrorCode'] = app._get_response()
        raise JobError(e, Job_Arguments = args)
    
    
