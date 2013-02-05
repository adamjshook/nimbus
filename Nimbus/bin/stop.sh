#!/bin/bash

if [ $# != 1 ]
then
	echo "Usage: $0 <cachename>"
	exit -1
fi

. "$NIMBUS_HOME/bin/nimbus-env.sh"

echo "stopping cache $1 "

for server in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`
do
	ssh $server "kill \$(cat $NIMBUS_HOME/pids/$1.pid)"
	ssh $server "rm $NIMBUS_HOME/pids/$1.pid"
done
