# ApacheKafka-Advanced-2

Link: 

#Comando para replicar os tópicos
./bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic ECOMMERCE_NEW_ORDER --partitions 3 --replication-factor 2