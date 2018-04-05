from redis_queue_class import RedisQueue
import time
import requests
from datetime import datetime
from functools import reduce
from time import sleep
q = RedisQueue('options', host='192.168.1.24')

def pull_from_couchdb(skip):
    #data = '{"selector": {"_id": {"$gte": "A"}}, "skip": %i, "limit": %i }' % (skip, increase_count)
    headers = {'content-type': 'application/json'}
    url = 'http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_weekly/_all_docs?include_docs=false&limit=%s&skip=%s' % (skip_count,skip)
    #, data=json.dumps(eval(data)),
    start_time = time.time()
    response = requests.get(url = url)
    #print(response.text)
    #print(response)
    print('getting', datetime.now(), q.qsize(), skip+skip_count, '',time.time()-start_time)
    sleep(60)

    return response

counts = []

skip = 22548000
total = 46208448
skip_count = 20000
while True:
    for i in range(10):
        try:
            options = pull_from_couchdb(skip)
        except Exception as e:
            print(e)
            sleep(30)
            i -= 1
            continue

        skip += skip_count
        try:
            for row in options.json()['rows']:
                q.put(row)
        except Exception as e:
            print(e)

        last_size = q.qsize()

    while q.qsize()>1000000:
        time.sleep(30)
        counts.append(q.qsize()-last_size)
        if len(counts)>10:
            counts.pop(0)
        avg = reduce(lambda x, y: x + y, counts) / len(counts)
        try:
            seconds = (((total - skip)/avg)*30)/60/1440
            print(datetime.now(), q.qsize(), q.qsize()-last_size, avg, seconds, 'days', ((skip)/46208448.0)*100)
        except:
            pass

        last_size = q.qsize()
