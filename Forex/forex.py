##Simple foreign exchange script

import configparser
from sqlalchemy import create_engine
import pandas as pd
import datetime
import datetime, time
import os
from slackclient import SlackClient
import io
from tqdm import tqdm
from forex_python.converter import CurrencyRates


def send_message(channels,initial_comment):
    for channel in channels:
        k=sc.api_call(
          "chat.postMessage",
          channel=channel,
          text=initial_comment
        )



def add_df(df,table,engine):
    placeholder = ", ".join(["%s"] * len(df))
    stmt = "insert into {table} ({columns}) values ({values});".format(table=table, columns=",".join(df.keys()), values=placeholder)
    cnx.execute(stmt, list(df.values()))


c = CurrencyRates()
cwd = os.getcwd()

try:
    config=configparser.ConfigParser()
    config.read(cwd + '/forex_config.ini')
    print('Configuration loaded')
    if int(config['default']['test'])==1:
        print('Test mode, to turn off test mode edit the configuration file with test=0')
    slack_token = config['slack']['slack_api_key']
    sc = SlackClient(slack_token)
except:
    print('Configuration file read error')
    
    
database_str='redshift'
if int(config['default']['test'])==1:
    database_str='test'
    
try:
    ADDRESS = str(config[database_str]['ADDRESS'])
    PORT = config[database_str]['PORT']
    USERNAME = str(config[database_str]['USERNAME'])
    PASSWORD = str(config[database_str]['PASSWORD'])
    DBNAME = str(config[database_str]['DBNAME'])
    redshift_str = ('redshift://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=USERNAME, password=PASSWORD, ipaddress=ADDRESS, port=PORT, dbname=DBNAME))
    cnx = create_engine(redshift_str) 
    print(cnx)
    print('Connected to Redshift')
except:
    print('Could not connect to Redshift, check config file')
    
    
base_currencies=config['forex']['base'].split(',')
foreign_currencies=config['forex']['foreign'].split(',')
table='supplementary.exchange_rates'
date_str=datetime.datetime.now().strftime('%Y-%m-%d')
date=datetime.datetime.now()

row={}
for i,base_currency in enumerate(base_currencies):
    row={}
    row['currency_pair']=base_currency + '-' +foreign_currencies[i]
    row['date']=date_str
    row['exchange_rate']=c.get_rate(base_currency, foreign_currencies[i], date)
    add_df(row,table,cnx)
    print(row)
try:
    send_message(config['slack']['user_id'].split(','),'Updated exchange rates for ' + date_str)
except:
    print('Slack message send failure')
    
