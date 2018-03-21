import requests as r
import pandas as pd
import re
from time import sleep
from datetime import datetime, timedelta
from couchdb import Server
from selenium import webdriver
from PIL import Image
from io import StringIO
from selenium.webdriver.common.action_chains import ActionChains
from io import BytesIO
import pandas as pd

db = Server('speedycalls')
server = Server('http://24.7.228.52:5984')
server.resource.credentials = ('mobone', 'C00kie32!')
option_db = server['marketwatch_2']

def capture_element(element,driver, option_title):
  location = element.location
  size = element.size
  img = driver.get_screenshot_as_png()
  img = Image.open(BytesIO(img))
  left = location['x']+300
  top = location['y']-190
  right = location['x']+300 + size['width']+335
  bottom = location['y']-190 + size['height']+210
  img = img.crop((int(left), int(top), int(right), int(bottom)))
  img.save('./images/'+option_title+'.png')

class option(object):
    def __init__(self, alert_string, driver):
        self.driver = driver
        self.alert = alert_string
        self.create_alert()

    def create_alert(self):
        parts = self.alert.split(' ')
        self.symbol, self.strike_price, self.option_type = parts[0][1:], float(parts[1]), parts[2][:-1]
        self.demand, self.vol = parts[3].title(), parts[10]
        self.exp = parts[8]+' '+parts[7][:-2]+' '
        self.formatted_exp, self.exp_datetime = self.get_exp_date()
        try:
            self.option_title = self.get_option()
        except Exception as e:
            print(self.symbol, "unable to find option", self.strike_price, self.option_type, self.formatted_exp)
            return
        #self.get_image(symbol, option_title)

        self.get_option_history()
        print(self.symbol, self.strike_price, self.option_type, self.demand, self.vol, self.formatted_exp, self.option_title)



    def get_exp_date(self):
        exp_datetime_0 = datetime.strptime(self.exp+str(datetime.now().year), '%b, %d %Y')
        exp_datetime_1 = datetime.strptime(self.exp+str(datetime.now().year+1), '%b, %d %Y')
        start = datetime.now()
        end = datetime.now() + timedelta(days=182)

        if exp_datetime_1<end and exp_datetime_1>start:
            exp_date = exp_datetime_1
        else:
            exp_date = exp_datetime_0

        return exp_date.strftime('%B %d, %Y'), exp_date

    def get_option(self):
        url = 'https://www.marketwatch.com/investing/stock/%s/options?countrycode=US&showAll=True' % self.symbol
        marketwatch_page = r.get(url).text
        #title="HZNPX15175600000"
        strike_price = str(self.strike_price).replace('.','')
        exp = self.exp_datetime
        match_me = 'title="'+self.symbol+'[A-Z]'+str(exp.day)+str(exp.year)[2:]+'[0-9]'+str(strike_price)+'[0-9]*'

        options = re.findall(match_me, marketwatch_page)

        if self.option_type == 'Call':
            return options[0].split('"')[1]
        else:
            return options[1].split('"')[1]

    def get_image(self, symbol, option_title):
        url = 'https://www.marketwatch.com/investing/stock/%s/option/' % symbol

        driver = self.driver
        driver.get(url+option_title)
        elem = driver.find_element_by_id('stockchart')
        driver.execute_script("window.scrollTo(10, 400);")
        #driver.execute_script("arguments[0].scrollIntoView();", elem)
        capture_element(elem, driver, option_title)

    def get_option_history(self):
        'AAAP_April-20-2018_11-13-2017_0'
        url_start = 'http://mobone:C00kie32!@24.7.228.52:5984/marketwatch_2/'
        df_total = None
        for i in range(0,25):
            key = [self.symbol,
                   self.exp_datetime.strftime('%B-%d-%Y'),
                   datetime.now().strftime('%m-%d-%Y'), str(i)]

            url = url_start+'_'.join(key)
            json_doc = r.get(url)
            if json_doc.status_code == 404:
                return
            df = pd.read_json(r.get(url).text).transpose()

            if df_total is None:
                df = df[df['Strike']==self.strike_price]
                df.index = [i]
                df_total = df

            else:
                df = df[df['Strike']==self.strike_price]
                df.index = [i]
                df_total = df_total.append(df)

            print(df_total)

        #print(pd.read_json(doc.items()))
        pass






    #BBYL08174560000
    #BBYX08174555000



#driver = webdriver.Chrome('./chromedriver')
driver = None
first_alert = None
while True:
    twits_html = r.get('https://stocktwits.com/SpeedyCalls').text
    alerts = re.findall('\$[A-Za-z0-9 .]*[BUYING|SELLING] Activity expiring on [0-9]*[a-z]* [A-Za-z]*, Vol [0-9]*',twits_html)

    if first_alert is None:
        first_alert = alerts[0]
        for alert in alerts:
            option(alert, driver)


    for alert in alerts[:alerts.index(first_alert)]:
        option(alert, driver)

    first_alert = alerts[0]

    #print(pd.read_html(twits_html))
    sleep(60)
