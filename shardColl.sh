#!/bin/bash
# set MONGOBIN and MONGOURI environment variables before calling

# The number of warehouses
NUMW=$1

# Number os shards
SHARDS=$2

# The database name
DB=$3


if [ -z "$NUMW" ]
then
    echo "Must specify number of warehouses"
    exit 1
fi
if [ -z "$DB" ]
then
    echo "No db passed in, using default (tpcc)"
    DB=tpcc
fi
if [ -z "$SHARDS" ]
then
    echo "No shards passed in will figure it out"
    SHARDS=""
fi
if [ -z "$MONGOURI" ]
then
    echo "No connection string set in MONGOURI, using default"
    MONGOURI="mongodb://localhost:27017"
fi
if [ -z "$MONGOBIN" ]
then
    MONGO=`command -v mongo`
    echo $MONGO
    if ! [ "$MONGO" ] 
    then 
       echo "Must specify MONGOBIN or have 'mongo' in the path"
       exit 1
    fi 
else
    MONGO=$MONGOBIN/mongo
fi

echo "$MONGO is mongo and $NUMW is warehouses $DB is DB, $MONGOURI is connection string and there are $SHARDS shards"

sed "s/_NUMWAREHOUSES_/${NUMW}/" shardColl.js | sed "s/_SHARDS_/$SHARDS/" | sed "s/_DBNAME_/$DB/" > shardTemp.js
$MONGO $MONGOURI --tls --tlsAllowInvalidHostnames --tlsAllowInvalidCertificates shardTemp.js