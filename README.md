HOW TO RUN:

1. producer.py:
   1. run ./producer.py

2.structured_kafka_wordcount.py:
  1. Start Kafka as indicated https://kafka.apache.org/quickstart
  2. Run the code file as:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 structured_kafka_wordcount.py localhost:9092 subscribe news_query

  *make sure packages argument matches your Spark and Scala environment. You can check Spark and Scala versions by seeing them on spark-shell


  
