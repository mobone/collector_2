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

couch = couchdb.Server("http://mobone:C00kie32!@192.168.1.24:5984/")
db_data = couch['finviz_data']

def get_data(html_text, ticker):
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

            db_data.save(json_doc)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()

    # get symbol count
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

    print(urls)
    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    for response in p.responses():
        get_data(response.content, response.request_kwargs['url'].split('=')[1])
