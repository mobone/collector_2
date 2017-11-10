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



# Generate ruleset for holiday observances on the NYSE

def NYSE_holidays(a=date.today(), b=date.today()+timedelta(days=365)):
    rs = rrule.rruleset()

    # Include all potential holiday observances
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth=12, bymonthday=31, byweekday=rrule.FR)) # New Years Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 1, bymonthday= 1))                     # New Years Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 1, bymonthday= 2, byweekday=rrule.MO)) # New Years Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 1, byweekday= rrule.MO(3)))            # Martin Luther King Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 2, byweekday= rrule.MO(3)))            # Washington's Birthday
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, byeaster= -2))                                  # Good Friday
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 5, byweekday= rrule.MO(-1)))           # Memorial Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 7, bymonthday= 3, byweekday=rrule.FR)) # Independence Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 7, bymonthday= 4))                     # Independence Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 7, bymonthday= 5, byweekday=rrule.MO)) # Independence Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth= 9, byweekday= rrule.MO(1)))            # Labor Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth=11, byweekday= rrule.TH(4)))            # Thanksgiving Day
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth=12, bymonthday=24, byweekday=rrule.FR)) # Christmas
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth=12, bymonthday=25))                     # Christmas
    rs.rrule(rrule.rrule(rrule.YEARLY, dtstart=a, until=b, bymonth=12, bymonthday=26, byweekday=rrule.MO)) # Christmas

    # Exclude potential holidays that fall on weekends
    rs.exrule(rrule.rrule(rrule.WEEKLY, dtstart=a, until=b, byweekday=(rrule.SA,rrule.SU)))

    return rs

# Generate ruleset for NYSE trading days

def NYSE_tradingdays(a=date.today(), b=date.today()+timedelta(days=365)):
    rs = rrule.rruleset()
    rs.rrule(rrule.rrule(rrule.DAILY, dtstart=a, until=b))

    # Exclude weekends and holidays
    rs.exrule(rrule.rrule(rrule.WEEKLY, dtstart=a, byweekday=(rrule.SA,rrule.SU)))
    rs.exrule(NYSE_holidays(a,b))

    return rs


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

        print('Process %i started' % self.process_id)
        start_t = None

        while True:
            (update_int, symbol, html_text) = self.process_q.get()

            if start_t is None:
                print("Process %i started" % self.process_id, datetime.now())
                start_t = time()

            tables = self.find_tables(html_text)
            expirations = re.findall(b'Expires [A-Za-z0-9 ]*, [0-9]*', html_text)

            price = None
            update_t = None
            json_docs = []
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
                json_docs.append(json_doc)
                #print(json_doc['_id'])

            json_docs = eval('{"docs": '+str(json_docs)+'}')


            saved = False
            while saved == False:
                try:
                    db.save(json_docs, batch='ok')
                    saved = True
                except couchdb.http.ResourceConflict:
                    print('conflict', json_doc['_id'])
                    saved = True
                except Exception as e:
                    print(e)
                    sleep(20)
                    db = self.connect()


            if self.process_q.empty():
                print("Process %i complete" % self.process_id, time()-start_t, datetime.now())
                start_t = None
                #if datetime.now()>self.market_close:
                #    break

    def connect(self):
        try:
            couch = couchdb.Server("http://mobone:C00kie32!@192.168.1.2:5984/")
            db = couch['marketwatch_2']
            return db
        except Exception as e:
            print(e)

        return None

    def find_tables(self, html_text):
        start = html_text.find('<p class="data bgLast">'.encode('utf-8'))
        end = html_text.find('</table>'.encode('utf-8'), start)+9
        html_text = html_text.replace(b'<tr class="chainrow heading colhead ', b'</table><table class="chainrow heading colhead')
        html_text = html_text.replace(b'<tr class="optiontoggle">',b'</tabble><tr class="optiontoggle">')

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


class collector(Thread):
    def __init__(self, thread_id, symbol_q, process_q):
        Thread.__init__(self)
        self.thread_id = thread_id
        self.symbol_q = symbol_q
        self.process_q = process_q
        self.symbols_list = []


    def run(self):
        #requests_cache.install_cache('demo_cache')
        while not self.symbol_q.empty():
            page = self.symbol_q.get()
            self.get_finviz(page)
        #requests_cache.uninstall_cache()

        self.get_start_times()
        self.session = requests.session()

        for self.start_index in range(self.start_index, len(self.start_times)):
            if self.thread_id==0:
                print("Thread %i sleeping" % self.thread_id, datetime.now(), self.start_times[self.start_index])
            while datetime.now()<self.start_times[self.start_index]:
                sleep(1)

            for symbol in self.symbols_list:
                url = "https://www.marketwatch.com/investing/stock/%s/options?countrycode=US&showAll=True" % symbol
                #with requests_cache.disabled():
                try:
                    res = self.session.get(url)
                    self.process_q.put((self.start_index, symbol,res.content))
                except:
                    pass


    def get_start_times(self):
        dt = datetime.now().strftime('%m-%d-%y')
        end_dt = datetime.strptime(dt+' 15:00:00', '%m-%d-%y %H:%M:%S')
        dt = datetime.strptime(dt+' 8:40:00', '%m-%d-%y %H:%M:%S')
        self.start_times = []
        while dt < end_dt:
            self.start_times.append(dt)
            dt = dt + timedelta(minutes=15)


        # skip to current time window
        for self.start_index in range(len(self.start_times)):
            if datetime.now()<self.start_times[self.start_index]:
                break

    def get_finviz(self, page):
        url = 'https://finviz.com/screener.ashx?v=111&f=sh_avgvol_o500,sh_opt_option&r=%s' % page
        res = requests.get(url)

        soup = BeautifulSoup(res.content,'lxml')
        tables = soup.find_all('table')
        df = pd.read_html(str(tables[16]))[0]

        for symbol in df.ix[1:,1].values:
            self.symbols_list.append(symbol)

        if self.thread_id==0:
            print('Length: ', len(self.symbols_list), self.thread_id)


def load_thread_q():
    res = requests.get('https://finviz.com/screener.ashx?v=111&f=sh_avgvol_o500,sh_opt_option')
    pages = int(re.findall(b'Page [0-9]*\/[0-9]*', res.content)[0][7:])
    for page in range(1,20*pages,20):
        finviz_q.put(page)


if __name__ == '__main__':
    if NYSE_holidays()[0].strftime('%b %d %Y')==datetime.now().strftime('%b %d %Y'):
        exit()
    process_q = Queue()
    finviz_q = Queue()

    load_thread_q()

    for thread_id in range(25):
        c = collector(thread_id, finviz_q, process_q)
        c.start()
        sleep(.5)

    for process_id in range(2):
        p = processor(process_id, process_q)
        p.start()
