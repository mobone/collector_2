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
from rq import Queue, Connection, Worker
from redis import Redis
from redis_functions import get_option

class worker_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        with Connection():
            w = Worker(['symbols'])
            w.work()


def get_symbols():
    finviz_url = 'https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option&r=%s'
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
        symbols = re.findall(r'primary">[A-Z]*', response.text)
        for symbol in symbols:
            symbols_list.append(symbol.split('>')[1])
    return symbols_list

if __name__ == '__main__':
    q = Queue('symbols', connection=Redis())

    symbols_list = get_symbols()
    print(len(symbols_list))
    for symbol in symbols_list:
        result = q.enqueue(
                 get_option, symbol)

    print('done storing')


    for i in range(20):
        x = worker_thread()
        x.start()
