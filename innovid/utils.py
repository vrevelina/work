from dateutil.relativedelta import relativedelta
from datetime import datetime
import boto3
import io
import config

def get_end_date():
    end_date = datetime.now() + relativedelta(days=-1)
    return end_date.strftime("%Y-%m-%d")

def get_start_date(end_date: str, source):
    if source != 'ispot':
        start_date = datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(days=config.days_back[source])
    else:
        start_date = datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(months=1)
    return start_date.strftime("%Y-%m-%d")
        
def save_data(df, bucket, destpath, destfname):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(bucket, destpath+destfname)
    s3_obj.put(Body=csv_buffer.getvalue())
    print("Data has been saved.")
    
def get_params(args, source):
    params = {}
    params['client_name'] = args['Client']
    params['start_date'] = args['StartDate']
    params['end_date'] = args['EndDate']
    params['bucket'] = config.s3[source]['bucket']
    params['destpath'] = config.s3[source]['destpath'].format(client_name=args['Client'])
    params['destfname'] = config.s3[source]['destfname'].format(run_datetime=datetime.now(), client_name=args['Client'])
    return params
    
class JobError(Exception):
    def __init__(self, error, **kwargs):
        super().__init__(error)
        self.error_type = type(error).__name__
        self._kwargs = kwargs
        
    def __str__(self):
        kwargs = ", ".join(str(k) + " = " + str(v) for k, v in self._kwargs.items())
        return f"{self.error_type}: {super().__str__()} || {kwargs}"