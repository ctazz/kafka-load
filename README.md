bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server1.properties &
bin/kafka-server-start.sh config/server2.properties &

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic orders
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic metrics
And to verify that your topics have been created:
bin/kafka-topics.sh --zookeeper localhost:2181 --describe

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic <theTopic>

/Users/charlestassoni/scala/blueprint/dev/SC-SCTEL-Plat-Common/src/test/resources/com/microsoft/shm/bpm.testevents/JemLoopsjson.txt bpm_in 10.0.0.247:9093,10.0.0.247:9094
dummFile bpm_in 10.0.0.39:9093,10.0.0.39:9094
dummFile bpm_in 10.1.10.52:9093,10.1.10.52:9094

gogo air
172.19.131.166
host.name=172.19.131.166

10.1.10.52