## GoogleTrends.py
Pulls Google Trends data using [pytrends](https://github.com/GeneralMills/pytrends) for a given list of keywords and (optional) date range.

Some things worth knowing before you read this class:
- Google Trends has several options for data granularity (hourly, daily, weekly, etc.). The data granularity returned by Google Trends depends on the given date range (see below).

| Granularity returned | Time between date range given |
| -------------------- | ----------------------------- |
| Daily                | 1-269 days                    |
| Weekly               | 270 days - 269 weeks          |
| Monthly              | \> 269 weeks                  |
- I didn't include the option to pull hourly data because we don't need it for our use case. But [pytrends](https://github.com/GeneralMills/pytrends) and Google Trends themselves do have the option to pull hourly data.