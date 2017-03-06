#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    Example of high-level Kafka 0.10 balanced consumer
"""

import sys
import argparse
import confluent_kafka

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--broker',
        default='kafka.bj.baidubce.com:9091',
        help='The broker address can be found in https://cloud.baidu.com/doc/Kafka/QuickGuide.html'
    )
    parser.add_argument(
        '--topic',
        required=True,
        help='The topic to consume from.  You can create a topic from console.')
    parser.add_argument(
        '--security_protocol',
        default='ssl',
        help='security.protocol=ssl is required to access Baidu Kafka service.')
    parser.add_argument(
        '--client_pem',
        default='client.pem',
        help='File path to client.pem provided in kafka-key.zip from console.')
    parser.add_argument(
        '--client_key',
        default='client.key',
        help='File path to client.key provided in kafka-key.zip from console.')
    parser.add_argument(
        '--ca_pem',
        default='ca.pem',
        help='File path to ca.pem provided in kafka-key.zip from console.')

    if len(sys.argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    topic = args.topic
    group = topic.split('__')[0] + '_kafka-samples-python'

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    conf = {
        'bootstrap.servers': args.broker,
        'group.id': group,
        'security.protocol': args.security_protocol,
        'ssl.ca.location': args.ca_pem,
        'ssl.certificate.location': args.client_pem,
        'ssl.key.location': args.client_key,
    }

    c = confluent_kafka.Consumer(**conf)
    c.subscribe([topic])
    numOfRecords = 10

    try:
        while numOfRecords > 0:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code(
                ) == confluent_kafka.KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        '%s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise confluent_kafka.KafkaException(msg.error())
            else:
                sys.stderr.write('parition: %d, offset: %d, message: %s\n' %
                                 (msg.partition(), msg.offset(), msg.value()))
        numOfRecords = numOfRecords - 1
    except KeyboardInterrupt:
        sys.stderr.write('Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()
