#!/bin/bash

# REQUIRED: This is the path to where your Nimbus installation is.
NIMBUS_HOME=/home/ashook/nimbus/Nimbus

# REQUIRED: This is the path to where your Hadoop installation is.
HADOOP_HOME=/usr/local/hadoop

# These variables you don't need to mess with
CONFIG_FILE=$NIMBUS_HOME/conf/nimbus-default.xml
LOG_DIR=$NIMBUS_HOME/logs
HOSTLIST=$NIMBUS_HOME/conf/servers
NIMBUS_EXEC="$JAVA_HOME/bin/java -jar"
