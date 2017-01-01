cherami-server
==============
[Cherami](https://eng.uber.com/cherami) is a distributed, scalable, durable, and highly available message queue system we developed at Uber Engineering to transport asynchronous tasks. 

This repo contains the source code of Cherami server, cross-zone replicator server, and several tools. Your application needs to use the client to interact with the server. The client can be found [here](https://github.com/uber/cherami-client-go).

Getting started
---------------

To get cherami-server:

```
git clone git@github.com:uber/cherami-server.git $GOPATH/src/github.com/uber/cherami-server
```

Build
-----
We use [`glide`](https://glide.sh) to manage Go dependencies. Additionally, we need a Cassandra running locally in order to run the integration tests. Please make sure `glide` and `cqlsh` are in your PATH, and `cqlsh` can connect to the local Cassandra server.

* Build the `cherami-server` and other binaries:
```
make bins
```

Run Cherami locally
-------------------

* Setup the cherami keyspace for metadata:
```
./scripts/cherami-setup-schema
```

* The service can be started as follows:
```
CHERAMI_ENVIRONMENT=laptop CHERAMI_CONFIG_DIR=`pwd`/config CHERAMI_STORE=/tmp/store ./cherami-server start all
```

One can use the CLI to verify if Cherami is running properly:
```
./cherami-cli --hostport=<localIP>:4922 create destination /test/cherami
```

Deploy Cherami as a cluster
---------------------------
Documentation coming soon....

Contributing
------------

We'd love your help in making Cherami great. If you find a bug or need a new feature, open an issue and we will respond as fast as we can. If you want to implement new feature(s) and/or fix bug(s) yourself, open a pull request with the appropriate unit tests and we will merge it after review.

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes.

Documentation
--------------

Interested in learning more about Cherami? Read the blog post:
[eng.uber.com/cherami](https://eng.uber.com/cherami/)

License
-------
MIT License, please see [LICENSE](https://github.com/uber/cherami-server/blob/master/LICENSE) for details.
