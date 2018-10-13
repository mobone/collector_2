import requests
from datetime import datetime, timedelta
import json
import time
import queue
import pandas as pd
import pymongo
import queue
from time import sleep
from multiprocessing import Process, Queue
import argparse
from redis_queue_class import RedisQueue
import json
from pymongo.errors import BulkWriteError
import configparser
import urllib
config = configparser.ConfigParser()
config.read('config.cfg')
username = config['creds']['User']
password = config['creds']['Pass']
ip = config['conn']['ip']

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

class finviz_class(Process):
    def __init__(self):
        Process.__init__(self)
        print('starting finviz class')


    def run(self):
        self.q = RedisQueue('finviz', host=ip)
        self.times = get_times()

        #mongo_string = 'mongodb://%s:%s@%s:27017/' % (username, password, ip)
        client = pymongo.MongoClient(ip+':27017',
                                     username = username,
                                     password = password,
                                     authSource='finance')
        db = client.finance
        self.collection = db.finviz

        #sleep(5)
        err = False
        while True:
            if err==False:
                doc_id = eval(str(self.q.get())[2:-1])['id']
                self.doc_id = doc_id

            try:
                response = requests.get('http://mobone:C00kie32!@%s:5984/finviz_data/%s' % (ip, doc_id))
                err = False
            except Exception as e:
                print(e)
                sleep(60)
                err = True
                continue
            #print(response.json())
            #print(type(response.json()))
            response = response.text.replace("\\",'')
            response = response.replace('Oper. Margin', 'Oper Margin')
            self.data = eval(response)
            print(doc_id)
            symbol = doc_id.split('_')[0]
            parts = doc_id.split('_')[1].split('-')
            update_date = parts[2]+parts[0]+parts[1]
            self.data['Date'] = update_date
            self.data['Root'] = symbol
            self.data['_id'] = symbol+'_'+update_date
            print(self.data['_id'])
            for col in list(self.data.keys()):
                if self.data[col]=='-':
                    del self.data[col]
                elif col == 'Volatility':

                    vol_week, vol_month = self.data[col].split(' ')
                    if vol_week !='-':
                        self.data['Volatility_Week'] = float(vol_week.rstrip('%')) / 100.0
                    if vol_month !='-':
                        self.data['Volatility_Month'] = float(vol_month.rstrip('%')) / 100.0
                    del self.data['Volatility']


                elif '%' in self.data[col]:
                    self.data[col] = float(self.data[col].rstrip('%')) / 100.0

                else:
                    try:
                        self.data[col] = float(self.data[col])
                    except:
                        pass
            """
            try:
                self.collection.insert(self.data)
            except Exception as e:
                print(e)
            """
            #print(dir(self.collection))
            try:
                #print('replacing ', self.data['_id'])
                self.collection.replace_one({'_id': self.data['_id']}, self.data, upsert=True)
            except Exception as e:
                print(e)

            """
            try:
                self.create_last_stock_price()
            except Exception as e:
                self.stock_last_price = None
                print('err', e)
            try:
                self.create_update_time()
                self.create_id_parts()
                self.check_update_num()
                self.create_option()
                self.store_option()
            except Exception as e:
                print('error', e, self.doc_id)
            """

    def store_option(self):
        try:

            result = self.collection.insert_many(json.loads(self.data_json), ordered=False)

        except BulkWriteError as bwe:

            pass



class doc_loader(Process):
    def __init__(self):
        Process.__init__(self)
        print('starting doc loader')

    def run(self):
        self.limit = 1000
        self.skip = 0
        q = RedisQueue('finviz', host='192.168.1.24')
        while self.skip<241519:
            try:
                options = self.pull_from_couchdb()
                #print(options.json())
                for row in options.json()['rows']:
                    q.put(row)
                    #print(row)
            except Exception as e:
                print(e)
                sleep(10)
                continue

            self.skip = self.skip + self.limit

    def pull_from_couchdb(self):
        headers = {'content-type': 'application/json'}
        url = 'http://mobone:C00kie32!@192.168.1.24:5984/finviz_data/_all_docs?include_docs=false&limit=%s&skip=%s' % (self.limit,self.skip)
        print('Getting ',self.limit)
        response = requests.get(url = url)
        print(url)
        #print(response)
        return response



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--proc', help='set numnber of processes', type=int, choices=[1,2,4,8])
    parser.add_argument('--redis', help='load from couchdb', action="store_true")
    args = parser.parse_args()
    if args.redis:
        x = doc_loader()
        x.start()
    else:

        data_q = RedisQueue('finviz', host='192.168.1.24')

        # start threads
        for i in range(args.proc):
            x = finviz_class()
            x.start()

        while True:
            sleep(10)
            #print(datetime.now(), data_q.qsize())
