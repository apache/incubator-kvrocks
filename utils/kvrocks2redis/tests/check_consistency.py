#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import redis
import codecs
import argparse
import time

class RedisComparator:
    def __init__(self, src_host, src_port, src_password, dst_host, dst_port, dst_password):
        self.src_cli = self._get_redis_client(src_host, src_port, src_password)
        self.dst_cli = self._get_redis_client(dst_host, dst_port, dst_password)

    def _get_redis_client(self, host, port, password):
        return redis.Redis(host=host, port=port, decode_responses=True, password=password)

    def _compare_string_data(self, key):
        src_data = self.src_cli.get(key)
        dst_data = self.dst_cli.get(key)
        return src_data, dst_data

    def _compare_hash_data(self, key):
        src_data = self.src_cli.hgetall(key)
        dst_data = self.dst_cli.hgetall(key)
        return src_data, dst_data

    def _compare_list_data(self, key):
        src_data = self.src_cli.lrange(key, 0, -1)
        dst_data = self.dst_cli.lrange(key, 0, -1)
        return src_data, dst_data

    def _compare_set_data(self, key):
        src_data = self.src_cli.smembers(key)
        dst_data = self.dst_cli.smembers(key)
        return src_data, dst_data

    def _compare_zset_data(self, key):
        src_data = self.src_cli.zrange(key, 0, -1, withscores=True)
        dst_data = self.dst_cli.zrange(key, 0, -1, withscores=True)
        return src_data, dst_data

    def _compare_bitmap_data(self, key, pos):
        src_data = self.src_cli.getbit(key, pos)
        dst_data = self.dst_cli.getbit(key, pos)
        return src_data, dst_data

    def _compare_data(self, keys : list, data_type):
        if data_type == "string":
            return self._compare_string_data(keys[0])
        elif data_type == "hash":
            return self._compare_hash_data(keys[0])
        elif data_type == "list":
            return self._compare_list_data(keys[0])
        elif data_type == "set":
            return self._compare_set_data(keys[0])
        elif data_type == "zset":
            return self._compare_zset_data(keys[0])
        elif data_type == 'bitmap':
            return self._compare_bitmap_data(keys[0], keys[1])
        elif data_type == 'none':
            return self.src_cli.type(keys[0]), 'none'
        else:
            raise ValueError(f"Unsupported data type '{data_type}' for key '{keys[0]}'")

    def compare_redis_data(self, key_file=''):
        if key_file:
            with open(key_file, 'rb') as f:
                for line in f:
                    keys = codecs.decode(line.strip()).split('-')
                    data_type = self.src_cli.type(keys[0])
                    src_data, dst_data = self._compare_data(keys, data_type)
                    if src_data != dst_data:
                        raise AssertionError(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

        self._import_and_compare(100)
        print('All tests passed.')

    def _import_and_compare(self, num):
        for i in range(num):
            key = f'key_{i}'
            value = f'value_{i}'
            self.src_cli.set(key, value)
            incr_key = 'incr_key'
            self.src_cli.incr(incr_key)
            hash_key = f'hash_key_{i}'
            hash_value = {'field1': f'field1_value_{i}', 'field2': f'field2_value_{i}'}
            self.src_cli.hset(hash_key, mapping=hash_value)
            set_key = f'set_key_{i}'
            set_value = [f'set_value_{i}_1', f'set_value_{i}_2', f'set_value_{i}_3']
            self.src_cli.sadd(set_key, *set_value)
            zset_key = f'zset_key_{i}'
            zset_value = {f'member_{i}_1': i+1, f'member_{i}_2': i+2, f'member_{i}_3': i+3}
            self.src_cli.zadd(zset_key, zset_value)
            time.sleep(0.02)
            keys = [key, incr_key, hash_key, set_key, zset_key]
            for key in keys:
                attempts = 0
                while attempts <= 3:
                    data_type = self.src_cli.type(key)
                    src_data, dst_data = self._compare_data([key], data_type)
                    if src_data == dst_data:
                        break
                    attempts += 1
                    time.sleep(0.1)
                else:
                    raise AssertionError(f"Data mismatch for key '{key}': source data: '{src_data}' destination data: '{dst_data}'")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Redis Comparator')
    parser.add_argument('--src_host', type=str, default='127.0.0.1', help='Source Redis host')
    parser.add_argument('--src_port', type=int, default=6666, help='Source Redis port')
    parser.add_argument('--src_password', type=str, default='foobared', help='Source Redis password')
    parser.add_argument('--dst_host', type=str, default='127.0.0.1', help='Destination Redis host')
    parser.add_argument('--dst_port', type=int, default=6379, help='Destination Redis port')
    parser.add_argument('--dst_password', type=str, default='', help='Destination Redis password')
    parser.add_argument('--key_file', type=str, help='Path to the file containing keys to compare')

    args = parser.parse_args()

    redis_comparator = RedisComparator(args.src_host, args.src_port, args.src_password, args.dst_host, args.dst_port, args.dst_password)
    redis_comparator.compare_redis_data(args.key_file)
