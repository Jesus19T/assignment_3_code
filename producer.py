from kafka import KafkaProducer
from kafka.errors import KafkaError
from newsapi import NewsApiClient
import time

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split

news_api = NewsApiClient(api_key='aab12b45a6d24541aee56741b679aead')

#'economic policy in dallas'
#'premier league'
for i in range(4):
    #sleep for 15 minute intervals
    time.sleep(900) 

    data = news_api.get_everything(q='premier league',\
                                    from_param='2023-11-26',\
                                    language='en')

    # data.keys()
    # print(data.keys())
    print()
    print(data['totalResults'])
    print()

    articles = data['articles']

    # for x,y in enumerate(articles):
    #     print(f'{x} {y["title"]}')

    # print()
    # print("The ARTICLES")
    # print(articles[0])
    article_descriptions = []
    for ind_artticle in articles:
        # print(f"\n{key.ljust(15)} {type(value)}   {value}")
        for key, value in ind_artticle.items():
            if key=="description" or key =="content":
                if value != None:
                    article_descriptions.append(value)

    print("article_descriptions length: ", len(article_descriptions))
    print()

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    article_bytes = []
    for desc in article_descriptions:
        as_bytes = bytes(desc, 'utf8')
        article_bytes.append(as_bytes)

    print("article_bytes length: ", len(article_bytes))
    print()

    for elemtn_bites in article_bytes:
        future = producer.send('news_query', value=elemtn_bites)

    # record_metadata = future.get(timeout=10)

    # print (record_metadata.topic)
    # print (record_metadata.partition)
    # print (record_metadata.offset)

    producer.flush()

    producer = KafkaProducer(retries=5)