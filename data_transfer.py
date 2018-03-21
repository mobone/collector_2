import couchdb
import requests
from datetime import datetime, timedelta
import json
import time
from requests_toolbelt.threaded import pool
import queue


couch = couchdb.Server("http://mobone:C00kie32!@24.7.228.52:5984/")
db = couch['marketwatch_weekly']

def get_times():
    dt = datetime.now().strftime('%m-%d-%y')
    end_dt = datetime.strptime('15:00:00', '%H:%M:%S')
    start_dt = datetime.strptime('8:40:00', '%H:%M:%S')
    start_times = []
    i = 1
    while start_dt < end_dt:
        start_times.append((i, str(start_dt).split(' ')[1]))
        start_dt = start_dt + timedelta(minutes=15)
        i = i + 1

    return start_times


times = get_times()
start = time.time()
url_start = 'http://mobone:C00kie32!@24.7.228.52:5984/marketwatch_weekly/'
url = url_start+'_find'
jobs = queue.Queue()
skip_num = 10
for skip_count in range(0,100,skip_num):
    data = '{"selector": {"_id": {"$gt": "A"}}, "skip": %i, "limit": %i }' % (skip_count, skip_num)

    headers = {'content-type': 'application/json'}

    jobs.put({'method': 'post', 'url': url, 'data': json.dumps(eval(data)), 'headers': headers})
    

p = pool.Pool(job_queue=jobs, num_processes=20)
p.join_all()
for response in p.responses():
    print(response.json()['docs'])
    print(response.json()['docs'][0]['_id'])
    input()
print(time.time()-start)

"""
for i in x:
    i = eval(i.replace(',\\r',''))
    key = i['key']
    url = url_start+key
    i = requests.get(url)
    doc = eval(i.content)
    doc['update_time'] = doc['Update_Time']
    doc['last_stock_price'] = float(doc['Last_Stock_Price'])
    del doc['Update_Time']
    del doc['Last_Stock_Price']

    # todo : check for duplicate prices, indicates holidays

    doc_time = datetime.strptime(doc['update_time'], '%H:%M:%S')
    for i in range(len(times)-1):
        range_time_0 = datetime.strptime(times[i][1], '%H:%M:%S')
        range_time_1 = datetime.strptime(times[i+1][1], '%H:%M:%S')
        if doc_time>range_time_0 and doc_time<range_time_1:
            if int(doc['_id'][-2:].replace('_',''))!=(i+1):
                print(doc['_id'])
                id_split = doc['_id'].split('_')
                id_split[-1:] = str(i+1)
                doc['_id'] = '_'.join(id_split)
                break


"""
