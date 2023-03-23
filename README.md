# kairos-REST-client

A kairosDB REST API Client with some extra handy graph functions.
You need [pandas](https://pypi.org/project/pandas/) to save files and [matplotlib](https://pypi.org/project/matplotlib/) to plot data.

# set up:
define these environment variables in your system:

```
KAIROSDB_AUTH_URL   :   Url for authentication  
KAIROSDB_QUERY_URL  :   Url for kairos query api
KAIROSDB_AUTH_USER  :   Authentication email/username
KAIROSDB_AUTH_PSWD  :   Authentication password
```

# Usage:

```
# import
import kairosClient
```

```
# you can use this handy function to generate iso format timestamps relative to your time.
kairosClient.getTimeStamp(-24*60*60)
```

```
# get data 
myData = kairosClient.downloader(myTags, startTimeISO, endTimeISO)
```
NOTE: 
There are no aggregators but duplicate records of same time samps are ignored. 

```
# save data
# kairosClient.save(myData, '.')
```

```
# plot in a ipynb notebook
# data: dict returned by kairosClient.downloader()
# cols = list of columns in data to be plotted.

plotTimeSeries(data:dict, cols:list=None)
```