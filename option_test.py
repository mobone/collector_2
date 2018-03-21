from pandas_datareader.data import Options
import pandas_datareader.data as web
import pandas as pd
import threading
from multiprocessing import Process
import queue
from time import sleep
import time
import pandas as pd
import requests as r
import couchdb
from datetime import datetime
import re
from nyse_holidays import *
from requests_toolbelt.threaded import pool
from bs4 import BeautifulSoup

# old version of yahoo collector

pd.options.display.max_rows = 999
couch = couchdb.Server("http://mobone:C00kie32!@24.7.228.52:5984/")

# try creating the dbs
for db_name in ['options','quotes','finviz']:
    try:
        del couch[db_name]
    except:
        pass
    try:
        couch.create(db_name)
    except:
        pass
option_db = couch['options']
quote_db = couch['quotes']


class option_thread(threading.Thread):
    def __init__(self, q, thread_id):
        threading.Thread.__init__(self)
        self.q = q
        self.thread_id = thread_id

    def run(self):
        print("started option thread")
        while True:

            while self.q.qsize()==0:
                sleep(1)
            while self.q.qsize():
                self.symbol, self.pull_num = self.q.get()
                self.get_quote()
                self.get_options()

    def get_quote(self):
        try:
            _id = [self.symbol, datetime.now().strftime('%Y%m%m'), str(self.pull_num)]
            html_source = r.get("https://finance.yahoo.com/quote/"+self.symbol).content
            quote = re.findall(rb'"regularMarketPrice":{"raw":[0-9.]*,', html_source)[0]
            self.quote = float(quote.split(b':')[2][:-1])
            doc = {'symbol': self.symbol, 'price': self.quote, 'update_time': str(datetime.now()), 'update_num': self.pull_num}
            doc['_id'] = '_'.join(_id)
            quote_db.save(doc)
        except Exception as e:
            print(e)
            pass


    def get_options(self):
        try:
            option_data = Options(self.symbol, 'yahoo')
            dates = option_data.expiry_dates
            option_data = option_data.get_all_data()
            option_data = option_data.reset_index()
        except Exception as e:
            print(self.symbol, e)
            return

        for date in dates:
            _id = [self.symbol, datetime.now().strftime('%Y%m%m'), str(date).replace('-',''), str(self.pull_num)]

            try:
                df = option_data[option_data['Expiry']==date]
                del df['JSON']
                del df['PctChg']
                if 'IsNonstandard' in df.columns:
                    del df['IsNonstandard']

                doc = eval(df.to_json())
                doc['_id'] = '_'.join(_id)
                doc['last_price'] = self.quote
                doc['symbol'] = self.symbol
                doc['update_time'] = str(datetime.now())
                doc['expiration'] = str(date)

                option_db.save(doc)     # to read it: pd.DataFrame(dict(doc))
            except Exception as e:
                pass


class finviz_process(Process):
    def __init__(self, urls):
        Process.__init__(self)
        self.urls = urls

    def run(self):
        couch = couchdb.Server("http://mobone:C00kie32!@24.7.228.52:5984/")
        self.finviz_db = couch['finviz']
        start = time.time()
        print("starting finviz processing")
        self.get_finviz_data()
        for response in self.p.responses():
            self.get_data(response.content, response.request_kwargs['url'].split('=')[1])
        print("ending finviz processing", time.time()-start)

    def get_finviz_data(self):
        p = pool.Pool.from_urls(self.urls, num_processes=20)
        p.join_all()

        self.p = p

    def get_data(self, html_text, ticker):
        start = html_text.find(b'"fullview-title"')
        end = html_text.find(b'</table>', start)
        soup = BeautifulSoup(html_text[start:end], 'html.parser')

        categories = soup.find_all(b'a')
        sector = categories[2].text
        industry = categories[3].text

        start = html_text.find(b'snapshot-table2')
        end = html_text.find(b'</table>', start)

        ticker_df = pd.read_html(html_text[start-100:end])[0]

        ticker_table = None
        for i in range(0,len(ticker_df.columns),2):
            table_section = ticker_df.ix[:,i:i+1]
            table_section.columns = ['Key', 'Value']
            table_section.index = table_section['Key']
            table_section = table_section['Value']
            if ticker_table is None:
                ticker_table = table_section
            else:
                ticker_table = ticker_table.append(table_section)

        series_index = list(ticker_table.index)
        series_index[28] = 'EPS Next Y Perc'
        ticker_table.index = series_index

        ticker_table['Ticker'] = ticker
        ticker_table['Sector'] = sector
        ticker_table['Industry'] = industry
        ticker_table['Date'] = datetime.now().strftime('%m-%d-%Y')
        ticker_table['_id'] = ticker+'_'+datetime.now().strftime('%m-%d-%Y')
        pd.set_option('display.max_rows', len(ticker_table))
        try:
            json_doc = eval(ticker_table.to_json(orient="index"))

            self.finviz_db.save(json_doc)
        except Exception as e:
            print(e)
            pass



def get_symbols_list():
    print('getting option symbols', end=' ')
    res = r.get('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option')
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
    print(len(symbols_list))
    return symbols_list

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

def get_finviz_urls():
    print('getting finviz urls', end=' ')
    html_text = r.get('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o50').content
    total_count = int(re.findall(b'Total: </b>[0-9]*', html_text)[0].split(b'>')[1])
    urls = []
    for page_count in range(1, int(total_count), 20):
        urls.append('https://finviz.com/screener.ashx?v=111&f=cap_smallover&r=' + str(page_count))



    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    urls = []
    for response in p.responses():
        response = response.content
        df = pd.read_html(response[response.find(b'Total:'):])[0]
        df.columns = df.iloc[0]
        df = df.drop(0)

        for ticker in df['Ticker']:
            urls.append('https://finviz.com/quote.ashx?t=' + ticker)
    print(len(urls))
    return urls

if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()
    start = None

    option_q = queue.Queue()

    symbols_list = get_symbols_list()
    start_times, start_index = get_start_times()

    finviz_urls = get_finviz_urls()

    option_threads = []
    for i in range(20):
        #print(i)
        x = option_thread(option_q, i)
        x.start()
        sleep(.5)
        option_threads.append(x)

    for start_index in range(start_index, len(start_times)):
        print('next gathering time', start_index, start_times[start_index])
        while datetime.now()<start_times[start_index]:
            sleep(1)
            if option_q.qsize()==0 and start is not None:
                print("option getting complete", time.time()-start)
                start = None

        start = time.time()
        print("option getting started", start_index)

        for symbol in symbols_list:
            option_q.put((symbol,start_index))

        if (start_index % 5) == 1 or start_index == 25:
            x = finviz_process(finviz_urls)
            x.start()

        while option_q.qsize():
            sleep(1)
