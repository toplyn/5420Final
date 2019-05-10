import requests
import pandas as pd
from pandas import ExcelFile
from pandas import ExcelWriter
from ast import literal_eval
import time
import requests
from retrying import retry
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from concurrent.futures import wait
import ujson

purchase_topic = 'shopping'
brokers = 'kafka-1:9092'
admin = AdminClient({'bootstrap.servers': brokers})
print('Creating functions')
def delete_purchase_topic():
    """Deletes the station status topic if it exists
    """
    topics = admin.list_topics(timeout=10).topics.keys()

    # delete topic if exists 
    while purchase_topic in topics:
        print("Trying to delete " + purchase_topic)
        status = admin.delete_topics([purchase_topic])
        fut = status[purchase_topic]
        try:
            fut.result()
        except Exception as e:
            print(e)
        topics = admin.list_topics(timeout=10).topics.keys()

def create_purchase_topic():
    """Creates the station status topic if it doesn't already exist
    """
    topics = admin.list_topics(timeout=10).topics.keys()

    # create topic if doesn't already exist
    while purchase_topic not in topics:
        print("Trying to create " + purchase_topic)
        status = admin.create_topics([NewTopic(purchase_topic, num_partitions=3, replication_factor=1)])

        fut = status[purchase_topic]
        try:
            fut.result()
        except Exception as e:
            print(e)
        topics = admin.list_topics(timeout=10).topics.keys()

def get_es_indices():
    '''See all elasticsearch indices
    '''
    r = requests.get("http://elasticsearch:9200/_cat/indices?format=json")
    if r.status_code != 200:
        print("Error listing indices")
        return None
    else:
        indices_full = r.json()  # contains full metadata as a dict
        indices = []  # let's extract the names separately
        for i in indices_full:
            indices.append(i['index'])
        return indices, indices_full
        
indices, indices_full = get_es_indices()
print(indices)

def create_es_index(index, index_config):
    '''Create and elasticsearch index
    '''
    r = requests.put("http://elasticsearch:9200/{}".format(index),
                     json=index_config)
    if r.status_code != 200:
        print("Error creating index")
    else:
        print("Index created")
        

def delete_es_index(index):
    r = requests.delete("http://elasticsearch:9200/{}".format(index))
    if r.status_code != 200:
        print("Error deleting index")
    else:
        print("Index deleted")

print('Loading data file')
dfe = pd.ExcelFile('Online Retail.xlsx')

print('Transforming data')
df_base = dfe.parse('Online Retail')
df_base = df_base.sort_values(['InvoiceDate'],ascending = 1)

baskets = df_base.groupby('InvoiceNo')['Description'].apply(list)
baskets = baskets.to_frame()

basket_tot_price = df_base.groupby('InvoiceNo')['UnitPrice'].agg('sum')
basket_tot_price = basket_tot_price.to_frame()

basket_tot_items = df_base.groupby('InvoiceNo')['Quantity'].agg('sum')
basket_tot_items = basket_tot_items.to_frame()
basket_tot_items = basket_tot_items.rename(columns = {0:'TotalItems'})

country = df_base.groupby('InvoiceNo').apply(lambda x: x['Country'].unique())
country = country.to_frame()
country = country.rename(columns = {0:'Country'})

purch_date = df_base.groupby('InvoiceNo').apply(lambda x: x['InvoiceDate'].unique())
purch_date = purch_date.to_frame()
purch_date = purch_date.rename(columns = {0:'InvoiceDate'})

df_base['StringQuantity'] = df_base['Quantity'].apply(lambda x: str(x))
df_base['StringPrice'] = df_base['UnitPrice'].apply(lambda x: str(x))

quantities = df_base.groupby('InvoiceNo')['StringQuantity'].apply(list)
quantities = quantities.to_frame()
prices = df_base.groupby('InvoiceNo')['StringPrice'].apply(list)
prices = prices.to_frame()

messages = 0
messages = baskets.join(basket_tot_price)
messages = messages.join(basket_tot_items)
messages = messages.join(country)
messages = messages.join(purch_date)
messages = messages.join(quantities)
messages = messages.join(prices)

messages.reset_index(level = 0, inplace=True)
messages_train = messages.iloc[0:(int(messages['Country'].count()*.8))]
messages_test = messages.iloc[(int(messages['Country'].count()*.8)+1):]

delete_purchase_topic()
create_purchase_topic()

print('Producing messages')
p = Producer({'bootstrap.servers': 'kafka-1:9092'})

data = messages_train
for msg in data.index:
    msg_base = data.iloc[msg]
    msgbytes = msg_base.to_json()
    #msgbytes = ujson.dumps(msg_base).encode('utf-8')
    p.produce(purchase_topic, msgbytes) 
    print('Producing message ' + str(msg))
    p.flush()
