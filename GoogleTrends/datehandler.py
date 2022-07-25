import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, SA, SU
from GoogleTrends import utils
# import utils

class datehandler:
    
    def __init__(self, freq, end_date = 'None', start_date = 'None'):
        self.freq = freq
        self.end_date_str = end_date
        self.start_date_str = start_date
        self.end_date = None
        self.start_date = None
        self.set_end_date()
        self.set_start_date()
    
    def _default_end(self):
        yesterday = datetime.today()-timedelta(days=1)
        if self.freq == 'daily':
            return yesterday
        elif self.freq == 'weekly':
            return yesterday - relativedelta(weekday=SA(-1))
        elif self.freq == 'monthly':
            first_of_month = datetime.today().replace(day=1)
            return first_of_month - relativedelta(days=1)
    
    def _default_start(self):
        if self.freq == 'daily':
            return self.end_date - relativedelta(years=1)
        elif self.freq == 'weekly':
            return self.end_date - relativedelta(weeks=52, days=-1)
        elif self.freq == 'monthly':
            return self.end_date - relativedelta(months=12)
        
    def _handle_end(self):
        yesterday = datetime.today() - timedelta(days=1)
        if self.freq == 'weekly':
            next_sat_from_edate = self.end_date + relativedelta(weekday=SA(1))
            if next_sat_from_edate > yesterday:
                self.end_date = self.end_date - relativedelta(weekday=SA(-1))
            else:
                self.end_date = next_sat_from_edate
        elif self.freq == 'monthly':
            last_day = calendar.monthrange(self.end_date.year, self.end_date.month)[1]
            last_of_month = self.end_date.replace(day=last_day)
            if last_of_month > yesterday:
                first_of_month = self.end_date.replace(day=1)
                self.end_date = first_of_month - relativedelta(days=1)
            else:
                self.end_date = last_of_month
        
    def _handle_start(self):
        if self.freq == 'weekly':
            self.start_date = self.start_date - relativedelta(weekday=SU(-1))
        elif self.freq == 'monthly':
            self.start_date = self.start_date.replace(day=1)
            
    def set_end_date(self):
        if self.end_date_str == 'None':
            self.end_date = self._default_end()
        else:
            self.end_date = utils.to_dt(self.end_date_str)
        self._handle_end()
            
    def set_start_date(self):
        if self.start_date_str == 'None':
            self.start_date = self._default_start()
        else:
            self.start_date = utils.to_dt(self.start_date_str)
        self._handle_start()
        
# to_timeframe from utils
    