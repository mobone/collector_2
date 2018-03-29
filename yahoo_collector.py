from pandas_datareader.yahoo.options import Options
import pandas as pd
import requests as r
import re
import queue
import threading
import time
from time import sleep
import pymongo
import ssl
from requests_toolbelt.threaded import pool
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from nyse_holidays import *
import json
from pymongo.errors import BulkWriteError
finviz_url = 'https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option&r=%s'

class option_getter(threading.Thread):
    def __init__(self, threadID, q, out_q, iteration):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.q = q
        self.out_q = out_q
        self.iteration = iteration
        self.s = r.Session()


    def run(self):
        self.update_date = datetime.now().strftime('%Y%m%d')
        while self.q.qsize():
            symbol = self.q.get()
            options = self.get_options(symbol)
            if options is not None:
                self.out_q.put(options)


    def get_options(self, symbol):
        forward_data = None


        try:
            o = Options(symbol, session=self.s)
            forward_data = o.get_forward_data(5, call=True, put=True).reset_index()

            del forward_data['JSON']
            del forward_data['Underlying']

            forward_data['Expiry'] = forward_data['Expiry'].astype(str).str.replace('-','')
            forward_data['iteration'] = self.iteration

            forward_data['Type'] = forward_data['Type'].str[0]
            forward_data['Update_Date'] = self.update_date
            forward_data['Update_Time'] = datetime.now().strftime('%H:%M:%S')
            forward_data['_id'] = forward_data['Root'].astype(str)+'_'+ \
                                  forward_data['Expiry'].astype(str)+'_'+ \
                                  self.update_date+'_'+ \
                                  str(self.iteration)+'_'+ \
                                  forward_data['Type'].astype(str)+'_'+ \
                                  forward_data['Strike'].astype(str)
            forward_data['_id'] = forward_data['_id'].str.replace('-','')


        except:
            pass
        return forward_data

class data_storer(Process):
    def __init__(self, q, out_q):
        Process.__init__(self)
        self.q = q
        self.out_q = out_q

    def run(self):
        #mongo_string = 'mongodb://68.63.209.203:27017/'
        mongo_string = 'mongodb://192.168.1.24:27017/'
        client = pymongo.MongoClient(mongo_string)
        db = client.finance
        collection = db.options
        #print('process started')
        #while True:
        while self.out_q.qsize()==0:
            sleep(10)
        while self.out_q.qsize() or self.q.qsize():
            data = self.out_q.get()
            data = data.to_json(orient='records',date_format='iso')



            try:

                collection.insert_many(json.loads(data))

            except BulkWriteError as bwe:
                print(bwe.details)
                #you can also take this component and do more analysis
                werrors = bwe.details['writeErrors']
                print('\n\n')
                print(werrors)


        print('process exiting')

class thread_starter(Process):
    def __init__(self, q, out_q, iteration):
        Process.__init__(self)
        self.q = q
        self.out_q = out_q
        self.iteration = iteration

    def run(self):
        for i in range(20):
            x = option_getter(i, self.q, self.out_q, self.iteration)
            x.start()
        while self.q.qsize():
            sleep(10)



def get_start_times():
    dt = datetime.now().strftime('%m-%d-%y')
    end_dt = datetime.strptime(dt+' 15:00:00', '%m-%d-%y %H:%M:%S')
    dt = datetime.strptime(dt+' 8:40:00', '%m-%d-%y %H:%M:%S')
    start_times = []
    while dt < end_dt:
        start_times.append(dt)
        dt = dt + timedelta(minutes=15)

    # skip to current time window
    for start_index in range(len(start_times)):
        if datetime.now()<start_times[start_index]:
            break
    return start_times, start_index

def get_symbols():
    finviz_page = r.get(finviz_url % 1)
    symbol_count = int(re.findall('Total: </b>[0-9]*', finviz_page.text)[0].split('>')[1])
    urls = []

    for symbol_i in range(1, symbol_count, 20):
        urls.append(finviz_url % symbol_i)

    p = pool.Pool.from_urls(urls)
    p.join_all()
    symbols_list = ['SPY','DIA']
    start = time.time()

    for response in p.responses():
        symbols = re.findall(r'primary">[A-Z]*', response.text)
        for symbol in symbols:
            symbols_list.append(symbol.split('>')[1])
        #if len(symbols_list)>1000:
        #    break
    return symbols_list

def start_threads():


    starter = thread_starter(symbols_q, out_q, start_index+1)
    starter.start()
    starter = thread_starter(symbols_q, out_q, start_index+1)
    starter.start()
    starter = thread_starter(symbols_q, out_q, start_index+1)
    starter.start()
    if out_q.qsize()==0:
        storer = data_storer(symbols_q, out_q)
        storer.start()



if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()
    start_times, start_index = get_start_times()

    symbols_list = get_symbols()

    symbols_q = Queue()
    out_q = Queue()

    for start_index in range(start_index, len(start_times)):
        print("Collector sleeping", datetime.now(), start_times[start_index], start_index+1)
        while datetime.now()<start_times[start_index]:
            sleep(1)


        start = time.time()
        for symbol in symbols_list:
            symbols_q.put(symbol)

        start_threads()
        #print('>>',symbols_q.qsize(), out_q.qsize(), time.time()-start)
        while symbols_q.qsize():
            sleep(15)
            print(symbols_q.qsize(), out_q.qsize(), time.time()-start)
