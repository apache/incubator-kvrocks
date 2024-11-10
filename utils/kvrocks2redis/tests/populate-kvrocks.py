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

import time
import argparse
import redis
import sys


filename = 'user_key.log'
file = open(filename, 'w')

PopulateCases = [
    ('string', [
        [('set', 'foo', 1), True],
        [('setex', 'foo_ex', 3600, 1), True],
    ]),
    ('zset', [
        [('zadd', 'zfoo', 1, 'a', 2, 'b', 3, 'c'), 3]
    ]),
    ('list', [
        [('rpush', 'lfoo', 1, 2, 3, 4), 4]
    ]),
    ('set', [
        [('sadd', 'sfoo', 'a', 'b', 'c', 'd'), 4]
    ]),
    ('hash', [
        [('hset', 'hfoo', 'a', 1), 1]
    ]),
    ('bitmap', [
        [('setbit', 'bfoo', 0, 1), 0],
        [('setbit', 'bfoo', 1, 1), 0],
        [('setbit', 'bfoo', 800000, 1), 0]
    ]),
    ('expire', [
        [('expire', 'foo', 3600), True],
        [('expire', 'zfoo', 3600), True]
    ])
]

AppendCases = [
    ('string', [
        [('set', 'foo', 2), True],
        [('set', 'foo2', 2), True],
        [('setex', 'foo_ex', 7200, 2), True]
    ]),
    ('zset', [
        [('zadd', 'zfoo', 4, 'd'), 1],
        [('zrem', 'zfoo', 'd'), 1]
    ]),
    ('list', [
        [('lset', 'lfoo', 0, 'a'), 1],
        [('rpush', 'lfoo', 'a'), 5],
        [('lpush', 'lfoo', 'b'), 6],
        [('lpop', 'lfoo'), 'b'],
        [('rpop', 'lfoo'), 'a'],
        [('ltrim', 'lfoo', 0, 2), True]
    ]),
    ('set', [
        [('sadd', 'sfoo', 'f'), 1],
        [('srem', 'sfoo', 'f'), 1]
    ]),
    ('hash', [
        [('hset', 'hfoo', 'b', 2), 1],
        [('hdel', 'hfoo', 'b'), 1]
    ]),
    ('bitmap', [
        [('setbit', 'bfoo', 0, 0), 1],
        [('setbit', 'bfoo', 900000, 1), 0]
    ]),
    ('expire', [
        [('expire', 'foo', 7200), True],
        [('expire', 'zfoo', 7200), True]
    ]),
    ('delete', [
        [('del', 'foo'), True],
        [('del', 'zfoo'), True]
    ])
]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1', type=str, help='host')
    parser.add_argument('--port', default=6666, type=int, help='port')
    parser.add_argument('--password', default='foobared')
    parser.add_argument('--flushdb', default=False, type=str, help='need to flushdb')

    return parser.parse_args()

def check(cmd, r):
    if len(cmd) == 1:
        print('EXEC %s' % (str(cmd[0]),))
        return True
    if hasattr(cmd[1], '__call__'):
        isPass = cmd[1](r)
    else:
        isPass = r == cmd[1]
    if not isPass:
        print('FAIL %s:%s != %s' % (str(cmd[0]), repr(r), repr(cmd[1])))
        return False
    return True

def pipeline_execute(client, name, cmds):
    succ = True
    p = client.pipeline(transaction=False)
    try:
        for cmd in cmds:
            if (name != 'bitmap'):
                file.write(cmd[0][1] + '\n')
            else:
                file.write(f"{cmd[0][1]}-{cmd[0][2]}" + '\n')
            p.execute_command(*cmd[0])
        res = p.execute()
        for i in range(0, len(cmds)):
            if not check(cmds[i], res[i]):
                succ = False
    except Exception as excp:
        succ = False
        print('EXCP %s' % str(excp))
    return succ



def run_test(client, cases : list):
    fails = []
    for case in cases:
        if not pipeline_execute(client, case[0], case[1]):
            fails.append(case[0])
    if len(fails) > 0:
        print('******* Some case test fail *******')
        for cmd in fails:
            print(cmd)
        return False
    else:
        print('All case passed.')
        return True


if __name__ == '__main__':
    args = parse_args()
    client = redis.Redis(host=args.host, port=args.port, decode_responses=True, password=args.password)
    if args.flushdb:
        client.flushdb()
    succ = True
    if not run_test(client, PopulateCases):
        succ = False
    if not run_test(client, AppendCases):
        succ = False
    if not succ:
        raise AssertionError("Test failed. See details above.")
