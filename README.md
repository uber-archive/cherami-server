cherami-server
==============

Cherami is a distributed, scalable, durable, and highly available message queue system we developed at Uber Engineering to transport asynchronous tasks. 

cherami-server is the actual implementation of the cherami architecture.

Getting started
---------------

To get cherami-server:

```
git clone git@github.com:uber/cherami-server.git
```

Developing
----------

The cherami-server repository holds the implementation of cherami architecture. Before getting started, cherami-server has a bunch of dependencies as follows:

* Make certain that `thrift` (OSX: `brew install thrift`) and `glide` are
in your path (above).
* `thrift-gen` is needed to autogenerate thrift APIs (`go get github.com/uber/tchannel-go/thrift/thrift-gen`)
* `cassandra` which is our metadata store is required to be up and running
* `rocksdb` is our data store and needs to be installed with the libraries being placed along with the cherami code in a folder called `rocksdb`:
	* In OSX, we need rocksdb3.13 (which can be installed using brew)
	* In Linux, download https://github.com/facebook/rocksdb and compile to get all the rocksdb libraries manually.

After installing all the requisite dependencies, one can do the following to start cherami:

* Setup the cherami keyspace for metadata (cassandra):
```
./scripts/cherami-setup-schema
```

* Build the cherami `standalone` and other binaries:
```
make bins
```

Once built, the service can be started as follows:
```
CHERAMI_ENVIRONMENT=laptop CHERAMI_CONFIG_DIR=`pwd`/config CHERAMI_STORE=/tmp/store ./cmd/standalone/standalone start all
```

One can use the CLI to verify if cherami is running properly:
```
./cmd/tools/cli/cherami-cli --hostport=<localIP>:4922 create destination /test/cherami
```

Documentation
--------------

Interested in learning more about Cherami? Read the blog post:
[eng.uber.com.cherami](https://eng.uber.com/cherami/)
