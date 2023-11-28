from kafka import KafkaProducer
from kafka.errors import KafkaError
from newsapi import NewsApiClient
import time

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split

news_api = NewsApiClient(api_key='aab12b45a6d24541aee56741b679aead')

# Make 4 requests to News Api in 15 minute intervals
for i in range(4):
    #sleep for 15 minute intervals
    time.sleep(900) 

    data = news_api.get_everything(q='premier league',\
                                    from_param='2023-11-26',\
                                    language='en')

    articles = data['articles']

    article_descriptions = []
    for ind_artticle in articles:
        for key, value in ind_artticle.items():
            if key=="description" or key =="content":   #description and content are in sentence format
                if value != None:
                    article_descriptions.append(value)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    article_bytes = []
    for desc in article_descriptions:
        as_bytes = bytes(desc, 'utf8')
        article_bytes.append(as_bytes)

    for elemtn_bites in article_bytes:
        future = producer.send('news_query', value=elemtn_bites)

    producer.flush()

    producer = KafkaProducer(retries=5)