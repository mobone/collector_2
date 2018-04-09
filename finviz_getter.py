import pandas as pd
import requests as r
import numpy as np
import queue
import threading
import multiprocessing
from time import sleep
import couchdb
from datetime import datetime
from bs4 import BeautifulSoup
import holidays
from requests_toolbelt.threaded import pool
import re
from nyse_holidays import *
import pymongo
import configparser
import urllib

config = configparser.ConfigParser()
config.read('config.cfg')
username = urllib.parse.quote_plus(config['creds']['User'])
password = urllib.parse.quote_plus(config['creds']['Pass'])

mongo_string = 'mongodb://%s:%s@%s:27017/' % (username, password, ip)
client = pymongo.MongoClient(mongo_string)
db = client.finance
collection = db.finviz
def get_data(html_text, ticker):
        start = html_text.find(b'"fullview-title"')
        end = html_text.find(b'</table>', start)
        soup = BeautifulSoup(html_text[start:end], 'html.parser')

        categories = soup.find_all(b'a')

        sector = categories[2].text
        industry = categories[3].text
        html_text = html_text.replace(b'Oper. Margin', b'Oper Margin')
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

        ticker_table['Root'] = ticker
        ticker_table['Sector'] = sector
        ticker_table['Industry'] = industry
        ticker_table['Date'] = datetime.now().strftime('%Y%m%d')
        ticker_table['_id'] = ticker+'_'+datetime.now().strftime('%Y%m%d')

        pd.set_option('display.max_rows', len(ticker_table))
        for col in ticker_table.keys():
            if ticker_table[col]=='-':
                del ticker_table[col]
            elif col == 'Volatility':

                vol_week, vol_month = ticker_table[col].split(' ')
                if vol_week !='-':
                    ticker_table['Volatility_Week'] = float(vol_week.rstrip('%')) / 100.0
                if vol_month !='-':
                    ticker_table['Volatility_Month'] = float(vol_month.rstrip('%')) / 100.0
                del ticker_table['Volatility']


            elif '%' in ticker_table[col]:
                ticker_table[col] = float(ticker_table[col].rstrip('%')) / 100.0

            else:
                try:
                    ticker_table[col] = float(ticker_table[col])
                except:
                    pass


        try:
            json_doc = eval(ticker_table.to_json(orient="index").replace('\\',''))

            collection.insert_one(json_doc)
            print(ticker)
        except Exception as e:
            print(e)

def p2f(x):
    return float(x.strip('%'))/100

if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()

    # get symbol count
    html_text = r.get('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option').content
    total_count = int(re.findall(b'Total: </b>[0-9]*', html_text)[0].split(b'>')[1])

    urls = []
    for page_count in range(1, int(total_count), 20):
        urls.append('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option&r=' + str(page_count))
        print(page_count)


    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    urls = []
    symbols_list = []
    for response in p.responses():
        symbols = re.findall(r'primary">[A-Z-]*', response.text)
        for symbol in symbols:
            symbols_list.append(symbol.split('>')[1])


    for ticker in symbols_list:
        urls.append('https://finviz.com/quote.ashx?t=' + ticker)



    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    for response in p.responses():
        get_data(response.content, response.request_kwargs['url'].split('=')[1])
