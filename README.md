CSYE7374_FinalProject_ Group3

Deployment Instructions

1. Start Zookeeper server

bin/zookeeper-server-start.sh /Users/avikalchhetri/kafka_2.11-0.8.2.1/config/zookeeper.properties

2. Start Kafka server

bin/kafka-server-start.sh /Users/avikalchhetri/kafka_2.11-0.8.2.1/config/server.properties

3. Mention the #hashtag in 'TwitterStream.keyword' in Kafka Producer program

4. Start Kafka producer

./gradlew produce 

This will start to read recent tweets, encode them to Avro and send to the Kafka cluster

5. Start Kafka consumer

 ./gradlew consume
 
6. Run python script for visualisation in plotly
