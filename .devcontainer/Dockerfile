FROM ubuntu:noble
RUN apt update \
    && apt install -y \
        git build-essential cmake libtool python3 libssl-dev python3-pip \
        wget curl clang-format-14 clang-tidy-14 golang-go ninja-build \
        redis-tools vim python3-redis redis-server clang lld mold gdb fish
RUN BUILD_DIR=$(pwd) && git clone https://github.com/jsha/minica /opt/minica \
    && cd /opt/minica && git checkout 96a5c93723cf3d34b50b3e723a9f05cd3765bc67 && go build && cd $BUILD_DIR \
    && echo 'export PATH=/opt/minica:$PATH' >> $HOME/.bashrc
