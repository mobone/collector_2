from redis_queue_class import RedisQueue
import time
import requests
from datetime import datetime
q = RedisQueue('options')

def pull_from_couchdb(skip):
    #data = '{"selector": {"_id": {"$gte": "A"}}, "skip": %i, "limit": %i }' % (skip, increase_count)
    headers = {'content-type': 'application/json'}
    url = 'http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_weekly/_all_docs?include_docs=true&limit=2000&skip=%s' % skip
    #, data=json.dumps(eval(data)),
    start_time = time.time()
    response = requests.get(url = url)
    #print(response.text)
    #print(response)
    print(datetime.now(), q.qsize(), skip, time.time()-start_time)

    return response

skip = 0
while True:
    options = pull_from_couchdb(skip)
    skip += 2000

    for row in options.json()['rows']:
        q.put(row)

    while q.qsize()>20000:
        time.sleep(10)
        print(datetime.now(), q.qsize())
