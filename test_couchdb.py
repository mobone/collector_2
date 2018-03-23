
import requests
import time

for i in range(5):
    skip = 150000
    url = 'http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_weekly/_all_docs?include_docs=false&limit=2000&skip=%s' % skip
    #, data=json.dumps(eval(data)),
    start_time = time.time()
    response = requests.get(url = url)
    print(time.time()-start_time)
