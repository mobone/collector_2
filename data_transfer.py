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
#ip = '68.63.209.203'
ip = '192.168.1.24'

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
    def __init__(self):
        Process.__init__(self)
        print('starting')


    def run(self):
        self.q = RedisQueue('options', host='192.168.1.24')
        self.times = get_times()

        mongo_string = 'mongodb://%s:27017/' % ip
        client = pymongo.MongoClient(mongo_string)
        db = client.finance
        self.collection = db.options

        #sleep(5)
        err = False
        while True:
            if err==False:
                doc_id = eval(str(self.q.get())[2:-1])['id']
                self.doc_id = doc_id
            #print(doc_id)
            #sleep(1)
            try:
                response = requests.get('http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_weekly/%s' % doc_id)
                err = False
            except Exception as e:
                print(e)
                sleep(60)
                err = True
                continue
            #print(response.json())
            #print(type(response.json()))

            self.data = response.json()

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

    def store_option(self):
        try:
            #print(self.data_json)
            self.collection.insert_many(json.loads(self.data_json), ordered=False)
        except BulkWriteError as bwe:
            pass
            """
            print('---------------------------')
            print(datetime.now())
            #print(bwe.details,'\n\n')
            #you can also take this component and do more analysis
            werrors = bwe.details['writeErrors']
            print(json.dumps(werrors, indent=4, sort_keys=True))
            print('---------------------------')
            """


    def create_id_parts(self):
        self.symbol, self.expiry, self.update_date, self.update_num = self.data['_id'].replace('-','').split('_')
        self.update_date = self.update_date.replace('-','')
        self.expiry = datetime.strptime(self.expiry, '%B%d%Y').strftime('%Y%m%d')
        self.id = '_'.join([self.symbol, self.expiry, self.update_date, self.update_num, '%s', '%s'])
        del self.data['_id']
        del self.data['_rev']

    def check_update_num(self):
        doc_time = datetime.strptime(self.update_time, '%H:%M:%S')
        for i in range(1, len(self.times)-1):
            range_time_0 = datetime.strptime(self.times[i], '%H:%M:%S')
            range_time_1 = datetime.strptime(self.times[i+1], '%H:%M:%S')

            if doc_time > range_time_0 and doc_time < range_time_1:
                if self.update_num != str(i):
                    #print(self.id, self.update_num, i)
                    self.update_num = str(i)

                    self.id = '_'.join([self.symbol, self.expiry, self.update_date, self.update_num, '%s', '%s'])
                    #print(self.id, self.update_num)

    def create_last_stock_price(self):
        self.stock_last_price = self.data['Last_Stock_Price']
        del self.data['Last_Stock_Price']

    def create_update_time(self):
        try:
            self.update_time = self.data['Update_Time']
            del self.data['Update_Time']
        except:
            pass

    def create_option(self):
        df = pd.DataFrame([], columns=['Strike','Expiry', 'Type', 'Last', 'Bid', 'Ask', 'Vol', 'Open_Int', 'Root', 'Underlying_Price', 'Quote_Time', 'iteration'])
        #print(self.data.keys())
        #print(self.doc_id)
        #exit()
        for key in self.data.keys():

            strike_price = key[:-1]
            option_type = key[-1:]

            self.data[key]['_id'] = self.id % (option_type.lower(), strike_price)
            self.data[key]['Strike'] = float(strike_price)
            self.data[key]['Type'] = option_type.lower()
            if self.stock_last_price:
                self.data[key]['Underlying_Price'] = float(self.stock_last_price)
            self.data[key]['Quote_Time'] = self.update_date+'_'+self.update_time
            qt = datetime.strptime(self.data[key]['Quote_Time'], '%Y%m%d_%H:%M:%S')
            self.data[key]['Quote_Time'] = qt
            self.data[key]['Root'] = self.symbol
            self.data[key]['iteration'] = int(self.update_num)
            self.data[key]['Expiry'] = self.expiry
            self.data[key]['Update_Date'] = self.update_date
            self.data[key]['Update_Time'] = datetime.now().strftime('%H:%M:%S')
            self.data[key]['Original_doc_id'] = self.doc_id
            option_symbol = self.symbol+str(self.expiry)[2:]+option_type.upper()
            str_strike_price = str(strike_price)
            if '.' in str_strike_price:
                num_digits = len(str_strike_price.split('.')[1])
            else:
                num_digits = 0

            while num_digits!=3:
                str_strike_price += '0'
                num_digits += 1
            str_strike_price = str_strike_price.replace('.','').zfill(8)
            option_symbol += str_strike_price
            self.data[key]['Symbol'] = option_symbol
            df = df.append(pd.DataFrame(self.data[key], index=[0]))

        self.data_json = df.to_json(orient='records',date_format='iso')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('proc', help='set numnber of processes', type=int)
    args = parser.parse_args()

    data_q = RedisQueue('options')

    # start threads
    for i in range(args.proc):
        x = option_class()
        x.start()

    while True:
        sleep(10)
        #print(datetime.now(), data_q.qsize())
