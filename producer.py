#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    Example of high-level Kafka 0.10 producer
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

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': args.broker,
        'security.protocol': args.security_protocol,
        'ssl.ca.location': args.ca_pem,
        'ssl.certificate.location': args.client_pem,
        'ssl.key.location': args.client_key
    }

    p = confluent_kafka.Producer(**conf)

    def _callback(err, msg):
        if err:
            sys.stderr.write('Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))

    numOfRecords = 10
    for i in range(0, numOfRecords):
        try:
            p.produce(
                topic,
                value='%d-hello kafka' % i,
                key=str(i),
                callback=_callback)

        except BufferError as e:
            sys.stderr.write('Local producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(p))

        p.poll(0)

    p.flush()
