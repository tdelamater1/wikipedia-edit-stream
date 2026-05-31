# Streaming of wikipedia events using Kafka #
This simple Python script makes use of the [EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) web service which exposes a stream of structured events over HTTP following SSE protocol. Those events include information about the editing of wikipedia web pages, creation of new ones and more. For the sake of this project we filter out only the events related to the editing of existing pages. Those events are being parsed into an appropriate format and get sent back to a Kafka topic.

The producer maintains a persistent connection to the stream and automatically reconnects if the connection drops, resuming from the last received event using `Last-Event-ID`.

We construct events that are sent to Kafka with the following format:
```json
{
"id": 1426354584, 
"domain": "en.wikipedia.org", 
"namespace": "main namespace", 
"title": "articles_title", 
"timestamp": "2021-03-14T21:55:14Z", 
"user_name": "a_user_name", 
"user_type": "human", 
"old_length": 6019, 
"new_length": 8687
}
```

## In order to reproduce this project ##
- Start a Kafka Broker and note its bootstrap server address.
- Create a topic named **wikipedia-events**

### Run without Docker ###
Create a Python 3 virtual environment and install dependencies:

```sh
python3 -m venv kafaka_venv
source kafaka_venv/bin/activate
pip install -r requirements.txt
```

Execute the producer:
```sh
python wikipedia_events_kafka_producer.py
```

The default bootstrap server is `192.168.4.201:9094`. Override with arguments:
```sh
python wikipedia_events_kafka_producer.py --bootstrap_server localhost:9092 --topic_name wikipedia-events
```

For all options:
```sh
python wikipedia_events_kafka_producer.py -h
```

### Run with Docker ###
Build docker image:
```sh
docker build -t wikipedia_events_kafka_producer .
```

Run docker app:
```sh
docker run wikipedia_events_kafka_producer
```

Override the bootstrap server if needed:
```sh
docker run wikipedia_events_kafka_producer --bootstrap_server localhost:9092
```

## Medium article ##
You can find the original tutorial [in this Medium article](https://towardsdatascience.com/introduction-to-apache-kafka-with-wikipedias-eventstreams-service-d06d4628e8d9).
