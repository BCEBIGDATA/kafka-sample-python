#!/bin/bash

yum install -y epel-release
yum install -y openssl-devel zlib zlib-devel lz4 lz4-devel cyrus-sasl-devel
wget https://github.com/edenhill/librdkafka/archive/v0.9.4.tar.gz
tar zxf v0.9.4.tar.gz && cd librdkafka-0.9.4
./configure && make && make install && ldconfig

export LD_LIBRARY_PATH=/usr/local/lib
# or add /usr/local/lib to /etc/ld.so.conf and run ldconfig
