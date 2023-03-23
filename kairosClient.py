from pandas.plotting._matplotlib.style import get_standard_colors
import requests # for kairos REST API
import pandas as pd
import os

# module variables
TOKEN = ''
STATUS = ''
LAST_PAYLOAD = {}

# remote url paths
KAIROSDB_AUTH_URL  = os.environ.get('KAIROSDB_AUTH_URL') 
KAIROSDB_QUERY_URL = os.environ.get('KAIROSDB_QUERY_URL') 
KAIROSDB_AUTH_USER = os.environ.get('KAIROSDB_AUTH_USER')
KAIROSDB_AUTH_PSWD = os.environ.get('KAIROSDB_AUTH_PSWD')

auth_payload = {
    "email"    : KAIROSDB_AUTH_USER,
    "password" : KAIROSDB_AUTH_PSWD
}

if None in [KAIROSDB_AUTH_URL, KAIROSDB_QUERY_URL, KAIROSDB_AUTH_USER, KAIROSDB_AUTH_PSWD]:
    raise RuntimeError("Your container/device does not contain environment variables needed to connect to kairosDB. Read [ReadMe.MD]")

import datetime

def getTimeStamp(seconds_offset_from_now:float=0):
    """
    Gets timestamp in ISO Format. 

    Default behaviour is to return current timestamp.\n
    You can pass argument seconds_offset_from_now, 
    positive number for future, negative number for past
    to get timestamps offset by seconds_offset_from_now.
    """
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    if seconds_offset_from_now==0:
        return now.isoformat()
    else:
        return (now + datetime.timedelta(seconds=seconds_offset_from_now)).isoformat()

lastResponse = {}

def login():
    """Logs in and saves TOKEN in the object."""
    global STATUS
    global TOKEN
    global KAIROSDB_AUTH_URL

    login_response = requests.post(KAIROSDB_AUTH_URL, json=auth_payload)
    
    if 'id' in login_response.json():
        STATUS += '\nLogin successful'
        TOKEN = login_response.json()['id']
    else:
        STATUS += '\nLogin failed'
        raise RuntimeError('Cannot connect to cloud !')
    return 0

# Basic function : to run a query
def runjson(url, body):
    """Runs a POST request and logs in automatically if needed."""
    global STATUS
    global TOKEN
    global LAST_PAYLOAD
    LAST_PAYLOAD = body
    if TOKEN == '':
        STATUS += '\nCreating a new session ...'
        login()
    
    STATUS += '\nRunning query try #1'
    # first try
    response = requests.post(url, json=body, params={'access_token':TOKEN})
    # second try
    if response.status_code not in [200, 201]:
        STATUS += '\nPrevious session must\'ve ended. Logging in again...'
        login()
        STATUS += '\nRunning query try #2'
        response = requests.post(url, json=body, params={'access_token':TOKEN})

    if response.status_code not in [200, 201]:
        STATUS += '\nResponse not received, Code : {}'.format(response.status_code)
        return None
    else:
        STATUS += '\nResponse received.'

    lastResponse = response.json()
    return lastResponse

# function : to download data into memory
def downloader(tagList:list, startTimeISO:str, endTimeISO:str):
    """
    Gets data for a given tagList & time interval from kairos & returns a dictionary: \n
    ```
    {
        '__timeStamp__'     : [t0, t1, t2, ...],
        'tag1'              : [v0, v1, v2, ...],
        'tag2'              : [v0, v1, v2, ...],
        ...
    }
    ```
    startTimeMs, endTimeMs are in ISO Format Example: '2023-03-23T01:40:00+05:30'.
    Notice the timeZone identifier at the end! This is important.
    timeStepMs is in milliseconds (can only be [1000, 60000]).\n
    data range includes start and end time.
    """

    # global variables
    global STATUS
    global TOKEN
    global KAIROSDB_QUERY_URL
    tzinfo = datetime.datetime.fromisoformat(startTimeISO).astimezone().tzinfo

    payload = {
        "start_absolute": datetime.datetime.fromisoformat(startTimeISO).timestamp()*1000,
        "end_absolute": datetime.datetime.fromisoformat(endTimeISO).timestamp()*1000,
        # "time_zone" : "Asia/Kolkata", #? 
        "metrics": []
    }        

    for tag in tagList:
        metric_schema = {
            "name": tag,
        }
        payload['metrics'].append(metric_schema)

    # Get response
    STATUS += '\nDownloading raw data...'
    response = runjson(KAIROSDB_QUERY_URL, payload)

    data = {}
    # processed_data = {}
    df = pd.DataFrame({})
    if not response==None:
        for query_index in range(len(response['queries'])):
            query = response['queries'][query_index]
            tagName = query['results'][0]['name']
            values = query['results'][0]['values'] # 
            for t,v in values:
                if t not in data:
                    data.update({t:{tagName:v}})
                else:
                    data[t].update({tagName:v})

        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_convert(tzinfo)
        # processed_data = {'__timeStamp__':list(df.index)}
        # for tag in df.columns:
        #     processed_data.update({
        #         tag : list(df[tag])
        #     })
        df.sort_index(inplace=True)
        df.index.name = 'time (ISO)'
    return df

def save(dataframe:pd.DataFrame, path = '.'):
    data = dataframe.copy()
    start = str(data.index[0])
    end = str(data.index[-1])
    tzinfo = data.index[0].tzinfo
    tzOffset = tzinfo.utcoffset(data.index[0]).seconds

    if len(data.columns):
        if '_' in data.columns[0]:
            prefix = data.columns[0][:data.columns[0].index('_')]
        else:
            prefix = ''
        filename = f'DATA_{prefix}_({start}-to-{end}).csv'.replace(':', '-')
    
    # convert time
    data.index = pd.Series([((t.timestamp()+tzOffset)/86400)+25569 for t in data.index])
    if path[-1] != '\\':
        path += '\\'
    data.to_csv(path + filename)
    pass

def plotTimeSeries(df:pd.DataFrame, cols:list=None):

    if cols is None: 
        cols = list(df.columns)
    
    colors = get_standard_colors(len(cols))
    lines = []
    labels = []
    ax = None

    for id, col in enumerate(cols):
        if ax == None:
            ax = df.plot(y=col, color=colors[id], marker='.', legend=False)
        else:
            ax = ax.twinx()
            df.plot(y=col, ax=ax, color=colors[id], marker='.', legend=False)

        ax.set_ylabel(col)
        line, label = ax.get_legend_handles_labels()
        lines += line
        labels += label
    
    ax.legend(lines, labels)