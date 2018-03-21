import couchdb
import requests
from datetime import datetime, timedelta
import json
import time
from requests_toolbelt.threaded import pool
import queue
import pandas as pd
import pymongo
import threading
import queue
from time import sleep
from multiprocessing import Process, Queue
import argparse

ip = '68.63.209.203'

couch = couchdb.Server('http://mobone:C00kie32!@%s:5984/' % ip)
db = couch['marketwatch_weekly']


def get_times():
    dt = datetime.now().strftime('%m-%d-%y')
    end_dt = datetime.strptime('15:00:00', '%H:%M:%S')
    start_dt = datetime.strptime('8:40:00', '%H:%M:%S')
    start_times = {}
    i = 1
    while start_dt <= end_dt:
        start_times[i] = str(start_dt).split(' ')[1]
        start_dt = start_dt + timedelta(minutes=15)
        i = i + 1

    return start_times

class option_class(Process):
    def __init__(self, q):
        Process.__init__(self)
        self.q = q

    def run(self):

        mongo_string = 'mongodb://%s:27017/' % ip
        client = pymongo.MongoClient(mongo_string)
        db = client.finance
        self.collection = db.options

        sleep(5)
        while True:
            self.data = self.q.get()

            self.create_last_stock_price()
            self.create_update_time()
            self.create_id_parts()
            self.check_update_num()
            self.create_option()
            self.store_option()

    def store_option(self):
        try:
            self.collection.insert_many(eval(self.data_json))
        except Exception as e:
            pass

    def create_id_parts(self):
        self.symbol, self.expiry, self.update_date, self.update_num = self.data['_id'].replace('-','').split('_')
        self.update_date = self.update_date.replace('-','')
        self.expiry = datetime.strptime(self.expiry, '%B%d%Y').strftime('%Y%m%d')
        self.id = '_'.join([self.symbol, self.expiry, self.update_date, self.update_num, '%s', '%s'])
        del self.data['_id']
        del self.data['_rev']

    def check_update_num(self):
        doc_time = datetime.strptime(self.update_time, '%H:%M:%S')
        for i in range(1, len(times)-1):
            range_time_0 = datetime.strptime(times[i], '%H:%M:%S')
            range_time_1 = datetime.strptime(times[i+1], '%H:%M:%S')

            if doc_time > range_time_0 and doc_time < range_time_1:
                if self.update_num != str(i):
                    print(self.id, self.update_num, i)
                    self.update_num = str(i)

                    self.id = '_'.join([self.symbol, self.expiry, self.update_date, self.update_num, '%s', '%s'])
                    print(self.id, self.update_num)

    def create_last_stock_price(self):
        self.stock_last_price = self.data['Last_Stock_Price']
        del self.data['Last_Stock_Price']

    def create_update_time(self):
        self.update_time = self.data['Update_Time']
        del self.data['Update_Time']

    def create_option(self):
        df = pd.DataFrame([], columns=['Strike','Expiry', 'Type', 'Last', 'Bid', 'Ask', 'Vol', 'Open_Int', 'Root', 'Underlying_Price', 'Quote_Time', 'iteration'])

        for key in self.data.keys():
            strike_price = key[:-1]
            option_type = key[-1:]
            self.data[key]['_id'] = self.id % (option_type.lower(), strike_price)
            self.data[key]['Strike'] = float(strike_price)
            self.data[key]['Type'] = option_type.lower()
            self.data[key]['Underlying_Price'] = float(self.stock_last_price)
            self.data[key]['Quote_Time'] = self.update_date+'_'+self.update_time
            qt = datetime.strptime(self.data[key]['Quote_Time'], '%Y%m%d_%H:%M:%S')
            self.data[key]['Quote_Time'] = qt
            self.data[key]['Root'] = self.symbol
            self.data[key]['iteration'] = int(self.update_num)
            self.data[key]['Expiry'] = self.expiry

            df = df.append(pd.DataFrame(self.data[key], index=[0]))

        self.data_json = df.to_json(orient='records',date_format='iso')

def pull_from_couchdb(skip):
    data = '{"selector": {"_id": {"$gte": "A"}}, "skip": %i, "limit": %i }' % (skip, increase_count)
    headers = {'content-type': 'application/json'}
    start_time = time.time()
    response = requests.post(url = url, data=json.dumps(eval(data)), headers= headers)
    print(datetime.now(), time.time()-start_time, skip)
    return response

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('proc', help='set numnber of processes', type=int)
    parser.add_argument('skip', help='set starting doc id', type=int)
    args = parser.parse_args()

    times = get_times()

    url_start = 'http://mobone:C00kie32!@%s:5984/marketwatch_weekly/' % ip
    url = url_start+'_find'
    jobs = queue.Queue()
    data_q = Queue()

    init_count = args.skip
    increase_count = 2000

    data_list = []
    response = pull_from_couchdb(init_count)
    data_list.append(response)

    init_count += increase_count

    # start threads
    for i in range(args.proc):
        x = option_class(data_q)
        x.start()

    while True:
        start = time.time()

        response = pull_from_couchdb(init_count)
        data_list.append(response)

        rows = data_list.pop().json()['docs']
        for row in rows:
            data_q.put(row)

        while not data_q.empty():
            sleep(1)

        init_count += increase_count
