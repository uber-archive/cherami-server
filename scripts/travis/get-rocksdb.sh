#!/bin/bash

sudo mkdir rocksdb
git clone https://github.com/facebook/rocksdb.git rocksdb
pushd rocksdb
git checkout -b tmp b78c8e07de599f8152edf4a30b994aaa217c870b
make shared_lib
sudo mkdir lib
sudo cp --preserve=links librocksdb.* lib/
popd
