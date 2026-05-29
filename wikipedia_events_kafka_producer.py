import json
import time
import argparse
import requests
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def construct_event(event_data, user_types):
    try:
        namespace = namespace_dict[event_data['namespace']]
    except KeyError:
        namespace = 'unknown'

    user_type = user_types[event_data['bot']]
    length = event_data.get('length', {})

    return {
        "id": event_data['id'],
        "domain": event_data['meta']['domain'],
        "namespace": namespace,
        "title": event_data['title'],
        "timestamp": event_data['meta']['dt'],
        "user_name": event_data['user'],
        "user_type": user_type,
        "old_length": length.get('old'),
        "new_length": length.get('new'),
    }


def init_namespaces():
    return {
        -2: 'Media',
        -1: 'Special',
        0: 'main namespace',
        1: 'Talk',
        2: 'User', 3: 'User Talk',
        4: 'Wikipedia', 5: 'Wikipedia Talk',
        6: 'File', 7: 'File Talk',
        8: 'MediaWiki', 9: 'MediaWiki Talk',
        10: 'Template', 11: 'Template Talk',
        12: 'Help', 13: 'Help Talk',
        14: 'Category', 15: 'Category Talk',
        100: 'Portal', 101: 'Portal Talk',
        108: 'Book', 109: 'Book Talk',
        118: 'Draft', 119: 'Draft Talk',
        446: 'Education Program', 447: 'Education Program Talk',
        710: 'TimedText', 711: 'TimedText Talk',
        828: 'Module', 829: 'Module Talk',
        2300: 'Gadget', 2301: 'Gadget Talk',
        2302: 'Gadget definition', 2303: 'Gadget definition Talk',
    }


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')
    parser.add_argument('--bootstrap_server', default='192.168.4.201:9094',
                        help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia-events',
                        help='Destination topic name', type=str)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_command_line_arguments()
    producer = create_kafka_producer(args.bootstrap_server)
    namespace_dict = init_namespaces()
    user_types = {True: 'bot', False: 'human'}

    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    last_event_id = None
    backoff = 1

    print('Starting Wikipedia event stream -> Kafka topic "{}"'.format(args.topic_name))

    while True:
        try:
            headers = {'User-Agent': 'Wikimedia edits kafka producer (tdelamater@gmail.com)'}
            if last_event_id:
                headers['Last-Event-ID'] = last_event_id

            response = requests.get(url, stream=True, headers=headers)
            for event in EventSource(response.raw).events():
                if event.id:
                    last_event_id = event.id
                if event.event == 'message':
                    try:
                        event_data = json.loads(event.data)
                    except ValueError:
                        continue
                    if event_data.get('type') == 'edit':
                        try:
                            event_to_send = construct_event(event_data, user_types)
                            producer.send(args.topic_name, value=event_to_send)
                            backoff = 1
                        except Exception as e:
                            print('Error processing event: {}'.format(e))

            # stream ended cleanly — reconnect immediately using Last-Event-ID
            print('Stream ended, reconnecting...')

        except Exception as e:
            print('Connection error: {}. Reconnecting in {}s...'.format(e, backoff))
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
