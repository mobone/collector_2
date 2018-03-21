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


finviz_url = 'https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o100,sh_opt_option&r=%s'

class option_getter(threading.Thread):
    def __init__(self, threadID, q, out_q, iteration):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.q = q
        self.out_q = out_q
        self.iteration = iteration

    def run(self):
        while self.q.qsize():
            symbol = self.q.get()
            options = self.get_options(symbol)
            if options is not None:
                self.out_q.put(options)


    def get_options(self, symbol):
        data_json = None
        o = Options(symbol)
        try:
            forward_data = o.get_forward_data(4, call=True, put=True).reset_index()

            del forward_data['JSON']
            del forward_data['Underlying']
            del forward_data['IsNonstandard']
            del forward_data['Symbol']
            self.update_date = datetime.now().strftime('%Y%m%d')
            forward_data['Expiry'] = forward_data['Expiry'].astype(str).str.replace('-','')
            forward_data['iteration'] = self.iteration

            forward_data['Type'] = forward_data['Type'].str[0]
            forward_data['_id'] = forward_data['Root'].astype(str)+'_'+ \
                                  forward_data['Expiry'].astype(str)+'_'+ \
                                  self.update_date+'_'+ \
                                  str(self.iteration)+'_'+ \
                                  forward_data['Type'].astype(str)+'_'+ \
                                  forward_data['Strike'].astype(str)
            forward_data['_id'] = forward_data['_id'].str.replace('-','')

            data_json = forward_data.to_json(orient='records',date_format='iso')
        except Exception as e:
            print("option err", e)
            pass
        return data_json

class data_storer(threading.Thread):
    def __init__(self, out_q):
        threading.Thread.__init__(self)
        self.out_q = out_q

    def run(self):
        mongo_string = 'mongodb://68.63.209.203:27017/'
        client = pymongo.MongoClient(mongo_string)
        db = client.testdb
        collection = db.options
        while True:
            while self.out_q.qsize():
                data = self.out_q.get()
                try:
                    collection.insert_many(eval(data))
                    exit()
                except Exception as e:
                    print(e)
                    input()
                    pass
            sleep(1)

def get_start_times():
    dt = datetime.now().strftime('%m-%d-%y')
    end_dt = datetime.strptime(dt+' 15:00:00', '%m-%d-%y %H:%M:%S')
    dt = datetime.strptime(dt+' 8:40:00', '%m-%d-%y %H:%M:%S')
    start_times = []
    while dt < end_dt:
        start_times.append(dt)
        dt = dt + timedelta(minutes=15)

    # skip to current time window
    for start_index in range(1, len(start_times)):
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
    symbols_list = []
    start = time.time()

    for response in p.responses():
        symbols_df = pd.read_html(response.text, header=0)[14]
        for symbol in list(symbols_df['Ticker']):
            symbols_list.append(symbol)
        if len(symbols_list):
            break
    return symbols_list

def start_threads():
    for i in range(30):
        x = option_getter(i, symbols_q, out_q, start_index)
        x.start()

    x = data_storer(out_q)
    x.start()


if __name__ == '__main__':
    start_times, start_index = get_start_times()

    symbols_list = get_symbols()

    symbols_q = queue.Queue()
    out_q = queue.Queue()

    for start_index in range(start_index, len(start_times)):
        print("Collector sleeping", datetime.now(), start_times[start_index])
        #while datetime.now()<start_times[start_index]:
        #    sleep(1)


        start = time.time()
        for symbol in symbols_list:
            symbols_q.put(symbol)

        start_threads()

        while symbols_q.qsize() or out_q.qsize():
            sleep(15)
            print(symbols_q.qsize(), out_q.qsize(), time.time()-start)
        exit()
