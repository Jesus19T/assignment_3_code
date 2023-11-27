#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
"""
from __future__ import print_function

import sys
import spacy

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf, col
from pyspark.sql.functions import split
from kafka import KafkaProducer

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    nlp = spacy.load("en_core_web_sm")
    # text= nlp(lines.value)
    # named_entities = [named_entiy.text for named_entiy in text.ents]
    print()
    print("PRINTING LINES VALUE")
    print(type(lines))
    print(lines.columns)
    print(lines.dtypes)
    print()

    # @udf
    def extract_entities(data):
        # spacy_model = get_spacy_model()
        # docs = spacy_model.pipe(documents)
        # tokens = [[tok.lemma_ for tok in doc if not tok.is_stop and tok.text]
        #         for doc in docs]
        # tokens_series = pd.Series(tokens)

        text = nlp(data)
        named_entities = [named_entiy.text for named_entiy in text.ents]
        return named_entities

    extract_entities_UDF = udf(lambda z: extract_entities(z))
    entities = lines.select(extract_entities_UDF(lines.value).alias('value'))

    # print()
    # print("PRINTING LINES VALUE")
    # print(type(entities))
    # print(entities.columns)
    # print(entities.dtypes)
    # print(entities)
    # print()

    # Split the lines into words
    words = entities.select(
        # explode turns each item in an array into a separate row
        explode(split(entities.value, ' ')).alias('word')
    )


    # words = lines.select(
    #     # explode turns each item in an array into a separate row
    #     (extract_entities_UDF(lines.value)).alias('values').explode(split(lines.value, ' ')).alias('word')
    # )

    # Generate running word count
    wordCounts = words.groupBy('word').count().orderBy(col("count").desc()).limit(10)

    # Start running the query that prints the running counts to the console
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    

    # query_2 = wordCounts \
    #         .writeStream \
    #         .outputMode('complete')\
    #         .format("kafka") \
    #         .option("kafka.bootstrap.servers", bootstrapServers) \
    #         .option("topic", "news_topic2") \
    #         .start()
    
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # producer.send('news_topic2', value=wordCounts)
    # producer.flush()
    # producer = KafkaProducer(retries=5)
    
    query.awaitTermination()
    # query_2.awaitTermination()
