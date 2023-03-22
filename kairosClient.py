from pandas.plotting._matplotlib.style import get_standard_colors
import requests # for kairos REST API
import pandas as pd
import os

# module variables
TOKEN = ''
STATUS = ''

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
tzinfo = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
tzOffset = tzinfo.utcoffset(datetime.datetime.utcnow()).seconds
tzName = tzinfo.tzname(datetime.datetime.utcnow())

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

# def plot_multi(dataDict, cols=None, spacing=.1, title='', **kwargs):

#     data = pd.DataFrame.from_dict(dataDict)

#     from pandas.plotting._matplotlib.style import get_standard_colors

#     # Get default color style from pandas - can be changed to any other color list
#     if cols is None: cols = list(data.columns)
#     if len(cols) == 0: return
#     if '__timeStamp__' in cols:
#         data['__timeStamp__'] = pd.to_datetime(data['__timeStamp__'], unit='ms', utc=True)
#         cols.remove('__timeStamp__')
#     else:
#         print('No __timeStamp__ axis in data')
#         return None
    
#     colors = get_standard_colors(num_colors=len(cols))

#     # First axis
#     # ax = data.loc[:, cols[0]].plot(label=cols[0], color=colors[0], **kwargs)
#     ax = data.plot.scatter('__timeStamp__', cols[0], label=cols[0], color=colors[0], **kwargs)
#     ax.set_title(title)
#     ax.set_xlabel('time')
#     ax.set_ylabel(ylabel=cols[0])
#     ax.get_legend().remove()
#     lines, labels = ax.get_legend_handles_labels()

#     for n in range(1, len(cols)):
#         # Multiple y-axes
#         ax_new = ax.twinx()
#         ax_new.spines['right'].set_position(('axes', 1 + spacing * (n - 1)))
#         # data.loc[:, cols[n]].plot(ax=ax_new, label=cols[n], color=colors[n % len(colors)], **kwargs)
#         data.plot.scatter('__timeStamp__', cols[n], ax=ax_new, label=cols[n], color=colors[n % len(colors)], **kwargs)
#         ax_new.set_ylabel(ylabel=cols[n])
#         ax_new.get_legend().remove()

#         # Proper legend position
#         line, label = ax_new.get_legend_handles_labels()
#         lines += line
#         labels += label

#     ax.legend(lines, labels, loc=0)
#     return ax

# class kairosAgent():

# Basic function : to login
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
def downloader(tagList:list, startTimeISO:str, endTimeISO:str, timeStepMs=60000):
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

    payload = {
        "start_absolute": datetime.datetime.fromisoformat(startTimeISO).timestamp()*1000,
        "end_absolute": datetime.datetime.fromisoformat(endTimeISO).timestamp()*1000,
        "time_zone" : "Asia/Kolkata",
        "metrics": []
    }        
    
    if timeStepMs <= 1000:
        timeStepMs = 1000
        samplingUnit = "seconds"
    else:
        timeStepMs = 60000
        samplingUnit = "minutes"

    for tag in tagList:
        metric_schema = {
            "name": tag,
            # "limit": limit,
            "aggregators": [
                {
                    "name": "first",
                    "sampling": {
                        "value": 1,
                        "unit": samplingUnit
                    }
                }
            ]
        }
        payload['metrics'].append(metric_schema)

    # Get response
    STATUS += '\nDownloading raw data...'
    response = runjson(KAIROSDB_QUERY_URL, payload)

    data = {}
    processed_data = {}

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
        processed_data = {'__timeStamp__':list(df.index)}
        for tag in df.columns:
            processed_data.update({
                tag : list(df[tag])
            })

    return processed_data

def save(data:dict, rootPath='EXPORTED_DATA\\'):
    
    cols = list(data.keys())
    if '__timeStamp__' in cols:
        startTimeMs = data['__timeStamp__'][0]
        endTimeMs  = data['__timeStamp__'][-1]
        startTimeStr = datetime.datetime.utcfromtimestamp(startTimeMs/1000).strftime('%Y-%m-%d-%H-%M')
        endTimeStr   = datetime.datetime.utcfromtimestamp(endTimeMs/1000).strftime('%Y-%m-%d-%H-%M')
        cols.remove('__timeStamp__')
    
    prefix = ''
    if len(cols):
        if '_' in cols[0]:
            prefix = cols[0][:cols[0].index('_')]

    if rootPath[-1:]!='\\':
        rootPath += '\\'
    filename = rootPath
    filename += f'DATA_{prefix}_({startTimeStr}--{endTimeStr}).csv'

    output = {
            f'time ({tzName})':[((t+(tzOffset*1000))/86400000)+25569 for t in data['__timeStamp__']]
        }
    output.update({key:data[key] for key in data if key != '__timeStamp__'})
    df = pd.DataFrame(output)
    df.to_csv(filename, index=False)
    pass

def plotTimeSeries(data:dict, cols:list=None):
    
    # Get default color style from pandas - can be changed to any other color list
    if '__timeStamp__' not in data:
        raise KeyError('No __timeStamp__ axis in data')
    df = pd.DataFrame({key:data[key] for key in ['__timeStamp__']+cols})
    df = df.drop(columns=['__timeStamp__'])
    
    df.index = pd.to_datetime(data['__timeStamp__'], unit='ms', utc=True).tz_convert(tzinfo)

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

        line, label = ax.get_legend_handles_labels()
        lines += line
        labels += label
    
    ax.legend(lines, labels)
    pass