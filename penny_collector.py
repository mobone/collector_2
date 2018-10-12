import pandas as pd
import requests as r
import numpy as np
import queue
import threading
import multiprocessing
from time import sleep
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from requests_toolbelt.threaded import pool
import re
from nyse_holidays import *
import pymongo
import configparser
import urllib

config = configparser.ConfigParser()
config.read('config.cfg')
username = config['creds']['User']
password = config['creds']['Pass']
ip = config['conn']['ip']


client = pymongo.MongoClient(ip+':65534',
                             username = username,
                             password = password,
                             authSource='finance')
db = client.finance
collection = db.penny_trading

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

        bid, ask = get_bid_ask(ticker)
        ticker_table['Bid'] = bid
        ticker_table['Ask'] = ask

        if ticker in down_symbols:
            ticker_table['Channel'] = 'Down'
        elif ticker in strong_down_symbols:
            ticker_table['Channel'] = 'Strong Down'
        elif ticker in up_symbols:
            ticker_table['Channel'] = 'Up'
        elif ticker in strong_up_symbols:
            ticker_table['Channel'] = 'Strong Up'


        try:
            json_doc = eval(ticker_table.to_json(orient="index").replace('\\',''))

            collection.replace_one({'_id': ticker_table['_id']}, json_doc, upsert=True)
            print(ticker)
        except Exception as e:
            print(e)

def get_bid_ask(ticker):
    bid, ask = None, None
    try:
        url = "https://finance.yahoo.com/q?s=%s" % ticker
        soup = BeautifulSoup(requests.get(url).text)
        bid = soup.find(attrs={"data-test":"BID-value"}).text.split(' x ')[0]
        ask = soup.find(attrs={"data-test":"ASK-value"}).text.split(' x ')[0]
    except Exception as e:
        print('err with getting bid and ask')
        print(e)

    return (bid, ask)

def p2f(x):
    return float(x.strip('%'))/100


def get_symbols(url):
    # get symbol count
    html_text = r.get(url).content
    total_count = int(re.findall(b'Total: </b>[0-9]*', html_text)[0].split(b'>')[1])

    urls = []
    for page_count in range(1, int(total_count), 20):
        urls.append(url + str(page_count))



    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    symbols_list = []
    for response in p.responses():
        symbols = re.findall(r'primary">[A-Z-]*', response.text)
        for symbol in symbols:
            symbols_list.append(symbol.split('>')[1])
    print('Got ',len(symbols_list),' symbols')
    return symbols_list


if __name__ == '__main__':
    if datetime.now().strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
        exit()





    symbols = get_symbols('https://finviz.com/screener.ashx?v=111&f=sh_avgvol_o50,sh_opt_short,sh_price_u5&r=')

    down_symbols = get_symbols('https://finviz.com/screener.ashx?v=111&f=sh_price_u5,ta_pattern_channeldown&r=')

    strong_down_symbols = get_symbols('https://finviz.com/screener.ashx?v=111&f=sh_price_u5,ta_pattern_channeldown2&r=')

    up_symbols = get_symbols('https://finviz.com/screener.ashx?v=111&f=sh_price_u5,ta_pattern_channelup&r=')

    strong_up_symbols = get_symbols('https://finviz.com/screener.ashx?v=111&f=sh_price_u5,ta_pattern_channelup2&r=')

    urls=[]
    for ticker in symbols:
        urls.append('https://finviz.com/quote.ashx?t=' + ticker)



    p = pool.Pool.from_urls(urls, num_processes=20)
    p.join_all()

    for response in p.responses():
        get_data(response.content, response.request_kwargs['url'].split('=')[1])
