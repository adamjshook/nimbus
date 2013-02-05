#!/bin/bash
if [ $# != 0 ]
then
        echo -e "Usage: $0"
	exit -1
fi

. "$NIMBUS_HOME/bin/nimbus-env.sh"

linenum=`cat $CONFIG_FILE | grep -n nimbus.master.port`
linenum=${linenum:0:2}
let linenum=$linenum+1
portline=$(head -n $linenum $CONFIG_FILE | tail -n 1)
PORT=$(echo $portline | sed 's/[^0-9]*//g')
NAME=master
TYPE=MASTER

LOG_FILE=$LOG_FILE/nimbus-$NAME-$USER-$TYPE.log

echo "starting nimbus using $CONFIG_FILE on port $PORT... logging to $LOG_DIR$LOG_FILE"

for server in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`
do
	ssh $server "mkdir -p $LOG_DIR"
	ssh $server "$NIMBUS_EXEC $NIMBUS_JAVA_OPTS -Djava.library.path=$NIMBUS_HOME/bin/native $NIMBUS_HOME/bin/nimbus.jar --config $CONFIG_FILE -n $NAME -p $PORT -t $TYPE > $LOG_DIR$LOG_FILE 2>&1 &"
	ssh $server "mkdir -p $NIMBUS_HOME/pids"
	ssh $server "ps -eo pid,args | grep \"\-n $NAME \-p $PORT \-t $TYPE\" | grep -v grep | cut -c1-6 > $NIMBUS_HOME/pids/$NAME.pid"
done
