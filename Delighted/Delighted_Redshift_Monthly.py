##Simple ETL script to copy NPS scores from Delighted over to Redshift

import delighted
import configparser
from sqlalchemy import create_engine
import pandas as pd
import datetime
from delighted import Client
import datetime, time
import os
from slackclient import SlackClient
import io


def send_message(channels,initial_comment):
    for channel in channels:
        k=sc.api_call(
          "chat.postMessage",
          channel=channel,
          text=initial_comment
        )

def create_table(table_name):
    response=cnx.execute('''CREATE TABLE {table_name} (detractor_count int, detractor_percent numeric,index int, nps int, passive_count int, passive_percent numeric, period_end date, period_start date, promoter_count int, promoter_percent numeric, response_count int, trend int); '''.format(table_name=table_name))

def get_dates():
    last_day_str=last_day_of_month(datetime.datetime.now()).strftime('%Y-%m-%d')
    first_day_str=datetime.datetime.now().strftime('%Y-%m-')+'01'
    since=time.mktime(datetime.datetime.strptime(first_day_str, "%Y-%m-%d").timetuple())
    till=time.mktime(datetime.datetime.strptime(last_day_str, "%Y-%m-%d").timetuple())
    return last_day_str, first_day_str, since, till
    

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  
    return next_month - datetime.timedelta(days=next_month.day)

def truncate_all(table_name):
    cnx.execute('''
TRUNCATE TABLE {table_name};
'''.format(table_name=table_name))

def check_database(trend,first_day_str,table_name):
    database_read=pd.read_sql_query('''select
    period_start,
    nps
    from {table_name}
    where period_start>='{first_day_str}' and trend={trend} '''.format(trend=trend,first_day_str=first_day_str,table_name=table_name), cnx)
    return database_read
    
    
def dict_format(delighted_dict,since,till,trend):
    delighted_dict['trend']=trend
    delighted_dict['period_start']=since
    delighted_dict['period_end']=till
    delighted_dict['index']=0
    return delighted_dict

def create_df(delighted_client,trend,since,till,first_day_str,last_day_str):
    temp_dict=delighted.Metrics.retrieve(client=delighted_client, trend=trend, since=since, till=till)
    temp_dict=dict_format(temp_dict,first_day_str,last_day_str,trend)
    return temp_dict
    
def add_df(df,table,engine):
    placeholder = ", ".join(["%s"] * len(df))
    stmt = "insert into {table} ({columns}) values ({values});".format(table=table, columns=",".join(df.keys()), values=placeholder)
    cnx.execute(stmt, list(df.values()))

def create_delighted_clients(markets_api,markets):
    delighted_client_list=[]
    for api_key in markets_api:
        delighted_client_list+=[Client(api_key=api_key,
                api_base_url='https://api.delighted.com/v1/')]
    return delighted_client_list

cwd = os.getcwd()
try:
    config=configparser.ConfigParser()
    config.read(cwd+'/config.ini')
    print('Configuration loaded')
    if int(config['default']['test'])==1:
        print('Test mode, to turn off test mode edit the configuration file with test=0')
    slack_token = config['slack']['slack_api_key']
    sc = SlackClient(slack_token)
    channels=config['slack']['user_id'].split(',')
except:
    print('Configuration file read error')
    
markets_api=config['delighted']['api_keys'].split(',')
markets=config['delighted']['markets'].split(',')
client_list=create_delighted_clients(markets_api,markets)
last_day_str, first_day_str, since, till=get_dates()


if int(config['default']['test'])==1:
    client_list=create_delighted_clients([config['delighted']['test_api_key']],['test'])


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
    
    
for i, delighted_client in enumerate(client_list):
    print('Market : '+ markets[i])
    try:
        trends=config['trends'][markets[i]].split(',')
        for trend in trends:
            if check_database(trend,first_day_str,config['redshift']['table_name']):
                try:
                    row=create_df(delighted_client,trend,since,till,first_day_str,last_day_str)
                    add_df(row,config['redshift']['table_name'],cnx)
                    print('Sucessfully added ' + first_day_str + 'for trend '+str(trend))
                    initial_comment='Updated NPS scores on Redshift for ' + markets[i] + ' for trend: ' + str(trend) + ' - '
                    try:
                        send_message(channels,initial_comment)
                    except:
                        print('Slack message send failure')
                except:
                    print('Failed trend: ' +str(trend))
            else:
                print('Figures already updated, skipping')
    except:
        print('Failure in updating')


    print('Completed')