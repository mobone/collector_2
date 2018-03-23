from redis_queue_class import RedisQueue
import time
import requests
from datetime import datetime
from functools import reduce
q = RedisQueue('options', host='192.168.1.5')

def pull_from_couchdb(skip):
    #data = '{"selector": {"_id": {"$gte": "A"}}, "skip": %i, "limit": %i }' % (skip, increase_count)
    headers = {'content-type': 'application/json'}
    url = 'http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_weekly/_all_docs?include_docs=false&limit=4000&skip=%s' % skip
    #, data=json.dumps(eval(data)),
    start_time = time.time()
    response = requests.get(url = url)
    #print(response.text)
    #print(response)
    print('getting', datetime.now(), q.qsize(), skip+4000, '',time.time()-start_time)

    return response

counts = []

skip = 0
total = 46208448
while True:
    options = pull_from_couchdb(skip)
    skip += 4000

    for row in options.json()['rows']:
        q.put(row)
    last_size = q.qsize()
    while q.qsize()>200000:
        time.sleep(30)
        counts.append(q.qsize()-last_size)
        if len(counts)>10:
            counts.pop(0)
        avg = reduce(lambda x, y: x + y, counts) / len(counts)
        seconds = (((total - skip)/avg)*30)/60/1440
        print(datetime.now(), q.qsize(), q.qsize()-last_size, avg, seconds, 'days', ((skip)/46208448.0)*100)

        last_size = q.qsize()
