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
import numpy as np
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

def check_update_num(update_time, start_times):

    doc_time = datetime.strptime(update_time, '%H:%M:%S')

    if doc_time<datetime.strptime('8:30:00', '%H:%M:%S'):
        doc_time += timedelta(hours=12)

    update_num = 1
    for i in range(1, len(start_times)):
        #range_time_0 = datetime.strptime(start_times[i], '%H:%M:%S')
        range_time_1 = datetime.strptime(start_times[i+1], '%H:%M:%S')

        if doc_time >= range_time_1:
            update_num = i+1

    #if update_num == 0 and doc_time>datetime.strptime(start_times[len(start_times)-1], '%H:%M:%S'):
    #    update_num = 26

    return update_num

class options_class(Process):
    def __init__(self, thread_id):
        Process.__init__(self)
        self.thread_id = thread_id
        print('starting options class')


    def run(self):
        self.q = RedisQueue('options_2', host=ip)
        self.times = get_times()

        #mongo_string = 'mongodb://%s:%s@%s:27017/' % (username, password, ip)
        client = pymongo.MongoClient(ip+':27017',
                                     username = username,
                                     password = password,
                                     authSource='finance')
        db = client.finance
        collection = db.options_2

        #sleep(5)
        start_times = get_times()
        err = False
        while True:
            if err==False:
                doc_id = eval(str(self.q.get())[2:-1])['id']
                self.doc_id = doc_id

            try:
                #print(doc_id)
                doc = requests.get('http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_2/%s' % doc_id)
                if 'missing' in doc.text:
                    continue
                err = False
            except Exception as e:
                print(e)
                sleep(60)
                err = True
                continue



            try:
                df = pd.read_json(doc.text, orient='index')
            except:

                continue

            del df['Strike']
            try:
                expiry = df.loc['expiration']['C_Ask']
            except:
                continue
            last_price = df.loc['last_stock_price']['C_Ask']
            symbol = df.loc['symbol']['C_Ask']
            update_date = df.loc['update_date']['C_Ask']
            update_time = df.loc['update_time']['C_Ask']
            final_df = []
            columns = list(df.columns.values)
            for i in ['expiration', 'last_stock_price', 'symbol', 'update_date', 'update_time', '_id', '_rev', 'update_interval']:
                df = df.drop(i)
            out_df = []
            real_index = []
            for strike in df.index:
                for i in ['C_', 'P_']:
                    row = []
                    for col in columns:
                        if i in col:
                            type, col_name = col.split('_')
                            row.append(df.ix[strike, type+'_'+col_name])
                    real_index.append(str(strike)+type)
                    out_df.append(str(row)[2:-2].replace("'",'').split(', '))
            df = pd.DataFrame(out_df)

            df.index = real_index
            df.columns = ['Ask', 'Bid', 'Change', 'Last', 'Open_Int', 'Vol']
            for i in df.columns:
                df[i] = df[i].replace('null', np.nan)
                df[i] = pd.to_numeric(df[i])
            options = []
            for i in df.iterrows():
                strike, type = i[0][:-1],i[0][-1:]

                row = i[1]
                row['Underlying_Price'] = float(last_price)

                row['Quote_Time'] = update_date+'_'+update_time

                qt = datetime.strptime(row['Quote_Time'], '%m-%d-%Y_%H:%M:%S')
                row['Update_Date'] = datetime.strptime(update_date, '%m-%d-%Y').strftime('%Y%m%d')
                row['Update_Time'] = update_time
                iteration = check_update_num(update_time, start_times)

                row['iteration'] = iteration
                row['Quote_Time'] = qt
                row['Strike'] = float(strike)
                row['Type'] = type.lower()
                row['Root'] = symbol
                row['Expiry'] = datetime.strptime(expiry, '%B-%d-%Y').strftime('%Y%m%d')
                row['_id'] = '_'.join([symbol, row['Expiry'], row['Update_Date'], strike, type, str(iteration)])
                row = row.replace('null', np.nan)

                option_symbol = symbol+str(row['Expiry'])[2:]+type.upper()
                str_strike_price = str(strike)
                if '.' in str_strike_price:
                    num_digits = len(str_strike_price.split('.')[1])
                else:
                    num_digits = 0

                while num_digits!=3:
                    str_strike_price += '0'
                    num_digits += 1
                str_strike_price = str_strike_price.replace('.','').zfill(8)
                option_symbol += str_strike_price
                row['Symbol'] = option_symbol

                options.append(row)
                row = row.dropna()
            options = pd.DataFrame(options)

            if self.thread_id == 1:
                print(options['_id'].head(1))
            options = json.loads(options.to_json(orient='records',date_format='iso'))

            try:
                #print('replacing ', self.data['_id'])
                collection.insert_many(options, ordered=False)
                #pass
            except Exception as e:
                print(e)




class doc_loader(Process):
    def __init__(self):
        Process.__init__(self)
        print('starting doc loader')

    def run(self):
        print('started')
        self.limit = 2000
        self.skip = 96000
        q = RedisQueue('options_2', host='192.168.1.24')
        while self.skip<20166410:
            while q.qsize()>100000:
                last = q.qsize()
                sleep(30)
                print(q.qsize()-last)
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
        now = time.time()

        url = 'http://mobone:C00kie32!@192.168.1.24:5984/marketwatch_2/_all_docs?include_docs=false&limit=%s&skip=%s' % (self.limit,self.skip)
        response = requests.get(url = url)
        print('Getting ',self.limit, self.skip, time.time()-now)


        #print(response)
        return response



if __name__ == '__main__':
    #print(get_times())
    #exit()
    parser = argparse.ArgumentParser()
    parser.add_argument('--proc', help='set numnber of processes', type=int, choices=[1,2,4,8,12])
    parser.add_argument('--redis', help='load from couchdb', action="store_true")
    args = parser.parse_args()
    if args.redis:
        x = doc_loader()
        x.start()
    else:

        data_q = RedisQueue('options_2', host='192.168.1.24')

        # start threads

        for i in range(args.proc):
            x = options_class(i)
            x.start()

        #x = options_class()
        #x.run()
        while True:
            sleep(10)
            print(datetime.now(), data_q.qsize())
