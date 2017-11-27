import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import time, sleep
import requests_cache
from multiprocessing import Process, Queue
from threading import Thread
import re
from datetime import date, datetime, timedelta
import couchdb
import pprint
from dateutil import rrule
from requests_toolbelt.threaded import pool
from nyse_holidays import *

#requests_cache.install_cache('demo_cache')

class processor(Process):
    def __init__(self, process_id, process_q):
        Process.__init__(self)
        self.process_id = process_id
        self.process_q = process_q

        dt = datetime.now().strftime('%m-%d-%y')
        self.market_close = datetime.strptime(dt+' 15:20:00', '%m-%d-%y %H:%M:%S')

    def run(self):
        db = self.connect()

        while self.process_q.empty():
            sleep(1)

        start_t = None
        while True:
            (update_int, symbol, html_text) = self.process_q.get()

            if start_t is None and self.process_id==0:
                print("Process %i started" % self.process_id, datetime.now())
                start_t = time()

            tables = self.find_tables(html_text)
            expirations = re.findall(b'Expires [A-Za-z0-9 ]*, [0-9]*', html_text)

            price = None
            update_t = None
            #json_docs = []
            for i in range(1,len(tables)):
                df = pd.read_html(str(tables[i]))[0]
                try:
                    return_values = self.format_df(html_text, df, expirations[i-1], symbol, price, update_t)
                    exp, price, update_t, df = return_values
                    #print(exp, price, update_t)
                    json_doc = df.to_json(orient="index")
                    json_doc = json_doc.replace('null', '"null"')
                    json_doc = eval(json_doc)
                except Exception as e:
                    print(symbol, e)
                    continue
                update_date = update_t.split(b' ')[0].decode().replace('/','-')
                json_doc['_id'] = symbol+'_'+exp+'_'+update_date+'_'+str(update_int)
                json_doc['update_time'] = update_t.split(b' ')[1].decode()
                json_doc['last_stock_price'] = price
                json_doc['symbol'] = symbol
                json_doc['expiration'] = exp
                json_doc['update_date'] = update_date
                json_doc['update_interval'] = update_int
                #json_docs.append(json_doc)
                #print(json_doc['_id'])

                #json_docs = eval('{"docs": '+str(json_docs)+'}')

                saved = False
                while saved == False:
                    try:
                        db.save(json_doc, batch='ok')
                        saved = True
                    except couchdb.http.ResourceConflict:
                        print('conflict', json_doc['_id'])
                        saved = True
                    except Exception as e:
                        print(e)
                        sleep(20)
                        db = self.connect()

            if self.process_q.qsize()==0 and self.process_id==0:
                print("Process %i complete" % self.process_id, datetime.now(), time()-start_t)
                start_t = None

    def connect(self):
        try:
            couch = couchdb.Server("http://mobone:C00kie32!@192.168.1.18:5984/")
            db = couch['marketwatch_2']
            return db
        except Exception as e:
            print(e)

        return None

    def find_tables(self, html_text):
        start = html_text.find('<p class="data bgLast">'.encode('utf-8'))
        end = html_text.find('</table>'.encode('utf-8'), start)+9
        html_text = html_text.replace(b'<tr class="chainrow heading colhead ', b'</table><table class="chainrow heading colhead')
        html_text = html_text.replace(b'<tr class="optiontoggle">',b'</table><tr class="optiontoggle">')

        soup = BeautifulSoup(html_text[start:end],'lxml')
        tables = soup.find_all('table')

        return tables

    def format_df(self, html_text, df, expiration, symbol, price, update_t):
        df.columns = ['C_Symbol', 'C_Last', 'C_Change', 'C_Vol', 'C_Bid', 'C_Ask',
                      'C_Open Int.', 'Strike', 'P_Symbol', 'P_Last', 'P_Change',
                      'P_Vol', 'P_Bid', 'P_Ask', 'P_Open Int.']
        df = df.ix[2:len(df)-2]
        df.index = df['Strike']
        df = df.drop(['C_Symbol', 'P_Symbol'], axis=1)
        if price is None:
            price = re.findall(b'bgLast\">[0-9]*.[0-9]*', html_text)[0].split(b'>')[1]
            price = float(price.replace(b',',b''))
            #price = df[df['Strike'].isnull()]['C_Last'].values[0]
            update_t = re.findall(b'Current price as of [0-9/ :]*[PM]*[AM]*', html_text)[0].split(b'as of ')[1]
            #update_t = df[df['Strike'].isnull()]['C_Change'].values[0][20:]
        exp = expiration.decode().replace('Expires ', '').replace(',','').replace(' ','-')
        df = df.dropna(subset=['C_Open Int.', 'P_Open Int.'], how='all')
        #if not df.empty:
        #    print(df.head())

        return exp, price, update_t, df

def get_symbols_list():
    res = requests.get('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option')
    pages = int(re.findall(b'Page [0-9]*\/[0-9]*', res.content)[0][7:])

    urls = []

    for i in range(1,20*pages,20):
        urls.append('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option&r=%s' % i)

    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()
    symbols_list = []
    for response in p.responses():
        soup = BeautifulSoup(response.content,'lxml')
        tables = soup.find_all('table')
        df = pd.read_html(str(tables[16]))[0]
        for symbol in df.ix[1:,1].values:
            symbols_list.append(symbol)
    return symbols_list

def get_options(symbols_list, process_q, start_index):
    urls = []
    for symbol in symbols_list:
        urls.append('http://www.marketwatch.com/investing/stock/%s/options?showAll=True' % symbol)

    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    for response in p.responses():
        symbol = response.request_kwargs['url'].split('/')[5]
        process_q.put((start_index, symbol, response.content))

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


if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()

    process_q = Queue()
    finviz_q = Queue()

    symbols_list = get_symbols_list()
    print(len(symbols_list))
    start_times, start_index = get_start_times()
    print(start_times[start_index])

    processor_started = False
    for start_index in range(start_index, len(start_times)):
        print("Collector sleeping", datetime.now(), start_times[start_index])
        while datetime.now()<start_times[start_index]:
            sleep(1)

        get_options(symbols_list, process_q, start_index)

        if processor_started == False:
            for process_id in range(3):
                p = processor(process_id, process_q)
                p.start()
            processor_started = True
