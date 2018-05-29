#GC log path has to be defined here because it needs to access CASSANDRA_HOME
JVM_OPTS="$JVM_OPTS -Xloggc:/foo/bar/baz/home/log-directory/gc.log"


#GC log path has to be defined here because it needs to access CASSANDRA_HOME
JVM_OPTS="$JVM_OPTS -Xloggc:${CASSANDRA_LOG_DIR}/gc.log"
