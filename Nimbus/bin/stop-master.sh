#!/bin/bash

. "$NIMBUS_HOME/bin/nimbus-env.sh"

echo "stopping master service"

for server in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`
do
        ssh $server "kill \$(cat $NIMBUS_HOME/pids/master.pid)"
        ssh $server "rm $NIMBUS_HOME/pids/master.pid"
done
