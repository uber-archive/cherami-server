#!/bin/bash

# retrieve rocksdb via github.com/cockroachdb/c-rocksdb
make vendor/glide.updated
DIR=vendor/github.com/cockroachdb/c-rocksdb/internal

pushd $DIR
make shared_lib
popd

if [ -f $DIR/librocksdb.4.11.2.dylib ]; then
	ln -sf $DIR/librocksdb.4.11.2.dylib librocksdb.4.11.dylib 
fi

if [ -f $DIR/librocksdb.so.4.11.2 ]; then
	ln -sf $DIR/librocksdb.so.4.11.2 librocksdb.so.4.11
fi
