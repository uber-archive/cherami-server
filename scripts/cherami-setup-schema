#!/bin/bash
set -xeo pipefail
# This script is used to setup the "cherami" keyspace
# and load all the tables in the .cql file within this keyspace,
# assuming cassandra is up and running.
# The default keyspace is "cherami", default replication factor is 3.

# To run against a specific Cassandra deployment, set CQLSH_HOST to the 
# Cassandra IP address

if [ -z "$KEYSPACE" ]; then
    KEYSPACE="cherami"
fi

if [ -z "$RF" ]; then
    RF=3
fi

if [ -z "$CQLSH" ]; then
    CQLSH=cqlsh
fi

$CQLSH -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '$RF'};"
$CQLSH -k $KEYSPACE -f `dirname $0`/../clients/metadata/schema/metadata.cql
