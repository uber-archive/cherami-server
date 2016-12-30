#!/bin/bash

sudo mkdir -p rocksdb/lib
sudo mkdir -p rocksdb/include
git clone https://github.com/facebook/rocksdb.git /tmp/rocksdb
pushd /tmp/rocksdb
git checkout -b tmp b78c8e07de599f8152edf4a30b994aaa217c870b
make shared_lib
popd
sudo cp --preserve=links /tmp/rocksdb/librocksdb.* rocksdb/lib/
sudo cp -r /tmp/rocksdb/include/rocksdb/ rocksdb/include/ 
