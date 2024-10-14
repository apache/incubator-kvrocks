<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

<img src="https://kvrocks.apache.org/img/kvrocks-featured.png" alt="kvrocks_logo" width="350"/>

[![CI](https://github.com/apache/kvrocks/actions/workflows/kvrocks.yaml/badge.svg?branch=unstable)](https://github.com/apache/kvrocks/actions/workflows/kvrocks.yaml)
[![License](https://img.shields.io/github/license/apache/kvrocks)](https://github.com/apache/kvrocks/blob/unstable/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/apache/kvrocks)](https://github.com/apache/kvrocks/stargazers)

---
* [Chat on Zulip](https://kvrocks.zulipchat.com/)
* [Mailing List](https://lists.apache.org/list.html?dev@kvrocks.apache.org) ([how to subscribe](https://www.apache.org/foundation/mailinglists.html#subscribing))

**Apache Kvrocks** is a distributed key value NoSQL database that uses RocksDB as storage engine and is compatible with Redis protocol. Kvrocks intends to decrease the cost of memory and increase the capacity while compared to Redis. The design of replication and storage was inspired by [rocksplicator](https://github.com/pinterest/rocksplicator) and [blackwidow](https://github.com/Qihoo360/blackwidow).

Kvrocks has the following key features:

* Redis Compatible: Users can access Apache Kvrocks via any Redis client.
* Namespace: Similar to Redis SELECT but equipped with token per namespace.
* Replication: Async replication using binlog like MySQL.
* High Availability: Support Redis sentinel to failover when master or slave was failed.
* Cluster: Centralized management but accessible via any Redis cluster client.

## Who uses Kvrocks

You can find Kvrocks users at [the Users page](https://kvrocks.apache.org/users/).

Users are encouraged to add themselves to the Users page. Either leave a comment on the ["Who is using Kvrocks"](https://github.com/apache/kvrocks/issues/414) issue, or directly send a pull request to add company or organization [information](https://github.com/apache/kvrocks-website/blob/main/src/components/UserLogos/index.tsx) and [logo](https://github.com/apache/kvrocks-website/tree/main/static/media/users).

## Build and run Kvrocks

### Prerequisite

```shell
# Ubuntu / Debian
sudo apt update
sudo apt install -y git build-essential cmake libtool python3 libssl-dev

# CentOS / RedHat
sudo yum install -y centos-release-scl-rh
sudo yum install -y git devtoolset-11 autoconf automake libtool libstdc++-static python3 openssl-devel
# download and install cmake via https://cmake.org/download
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh -O cmake.sh
sudo bash cmake.sh --skip-license --prefix=/usr
# enable gcc and make in devtoolset-11
source /opt/rh/devtoolset-11/enable

# openSUSE / SUSE Linux Enterprise
sudo zypper install -y gcc11 gcc11-c++ make wget git autoconf automake python3 curl cmake

# Arch Linux
sudo pacman -Sy --noconfirm autoconf automake python3 git wget which cmake make gcc

# macOS
brew install git cmake autoconf automake libtool openssl
# please link openssl by force if it still cannot be found after installing
brew link --force openssl
```

### Build

It is as simple as:

```shell
$ git clone https://github.com/apache/kvrocks.git
$ cd kvrocks
$ ./x.py build # `./x.py build -h` to check more options;
               # especially, `./x.py build --ghproxy` will fetch dependencies via ghproxy.com.
```

To build with TLS support, you'll need OpenSSL development libraries (e.g. libssl-dev on Debian/Ubuntu) and run:

```shell
$ ./x.py build -DENABLE_OPENSSL=ON
```

To build with lua instead of luaJIT, run:

```shell
$ ./x.py build -DENABLE_LUAJIT=OFF
```

Build with debug mode, run:

```shell
# The default build type is RelWithDebInfo and its optimization level is typically -O2.
# You can change it to -O0 in debug mode.

$ ./x.py build -DCMAKE_BUILD_TYPE=Debug
```

### Running Kvrocks

```shell
$ ./build/kvrocks -c kvrocks.conf
```

### Running Kvrocks using Docker

```shell
$ docker run -it -p 6666:6666 apache/kvrocks --bind 0.0.0.0
# or get the nightly image:
$ docker run -it -p 6666:6666 apache/kvrocks:nightly
```

Please visit [Apache Kvrocks on DockerHub](https://hub.docker.com/r/apache/kvrocks) for additional details about images.

### Connect Kvrocks service

```sh
$ redis-cli -p 6666

127.0.0.1:6666> get a
(nil)
```

### Running test cases

```shell
$ ./x.py build --unittest
$ ./x.py test cpp # run C++ unit tests
$ ./x.py test go # run Golang (unit and integration) test cases
```

### Supported platforms

* Linux
* macOS

## Namespace

Namespace is used to isolate data between users. Unlike all the Redis databases can be visited by `requirepass`, we use one token per namespace. `requirepass` is regraded as admin token, and only admin token allows to access the namespace command, as well as some commands like `config`, `slaveof`, `bgsave`, etc. See the [Namespace](https://kvrocks.apache.org/docs/namespace) page for more details.

```sh
# add token
127.0.0.1:6666> namespace add ns1 my_token
OK

# update token
127.0.0.1:6666> namespace set ns1 new_token
OK

# list namespace
127.0.0.1:6666> namespace get *
1) "ns1"
2) "new_token"
3) "__namespace"
4) "foobared"

# delete namespace
127.0.0.1:6666> namespace del ns1
OK
```

## Cluster

Kvrocks implements a proxyless centralized cluster solution but its accessing method is completely compatible with the Redis cluster client. You can use Redis cluster SDKs to access the kvrocks cluster. More details, please see: [Kvrocks Cluster Introduction](https://kvrocks.apache.org/docs/cluster/)

## Documents

Documents are hosted at the [official website](https://kvrocks.apache.org/docs/getting-started/).

* [Supported Commands](https://kvrocks.apache.org/docs/supported-commands/)
* [Design Complex Structure on RocksDB](https://kvrocks.apache.org/community/data-structure-on-rocksdb/)
* [Replication Design](https://kvrocks.apache.org/docs/replication)

## Tools

* To manage Kvrocks clusters for failover, scaling up/down and more, use [kvrocks-controller](https://github.com/apache/kvrocks-controller)
* To export the Kvrocks monitor metrics, use [kvrocks_exporter](https://github.com/RocksLabs/kvrocks_exporter)
* To migrate from Redis to Kvrocks, use [RedisShake](https://github.com/tair-opensource/RedisShake)
* To migrate from Kvrocks to Redis, use `kvrocks2redis` built via `./x.py build`

## Contributing

Kvrocks community welcomes all forms of contribution and you can find out how to get involved on the [Community](https://kvrocks.apache.org/community/) and [How to Contribute](https://kvrocks.apache.org/community/contributing) pages.

## License

Apache Kvrocks is licensed under the Apache License Version 2.0. See the [LICENSE](LICENSE) file for details.

## Social Media

- [Medium](https://kvrocks.medium.com/)
- [X (Twitter)](https://twitter.com/apache_kvrocks)
- [Zhihu](https://www.zhihu.com/people/kvrocks) (in Chinese)
- WeChat Official Account (in Chinese, scan the QR code to follow)

![WeChat official account](assets/wechat_account.jpg)
