# kafka-topic-manager

A simple REST service to help delete kafka topics.

## Motivation

Kafka 1.1.1 has a bug where if you delete topics too quickly, it will overwhelm the controller, and the deletions will get stuck and never complete.  I wrote this simple webservice to allow me to throttle topic deletions to kafka. You can submit a whole bunch of topic deletion requests to this webservice. It will store them internally and delete them from the cluster one by one.

## Usage

### Start the service
```
$ java -jar build/libs/kafka-topic-manager-0.0.1.jar
```

### To delete a topic
To delete a topic named `my-topic` on the broker at address `example.com`. (This code assumes that the broker and zookeeper are running on the same node)
```
$ curl -X "DELETE" localhost:8080/broker/example.com/topic/my-topic
```

### To see pending deletions
```
$ curl -X "DELETE" localhost:8080/broker/example.com/topic/my-topic1
$ curl -X "DELETE" localhost:8080/broker/example.com/topic/my-topic2
$ curl -X "DELETE" localhost:8080/broker/example.com/topic/my-topic3
$ curl localhost:8080/deletions
[{"broker":"example.com","topic":"my-topic1"},{"broker":"example.com","topic":"my-topic2"},{"broker":"example.com","topic":"my-topic3"}
```
