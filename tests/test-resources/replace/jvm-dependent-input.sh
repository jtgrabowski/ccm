# only add -Xlog:gc if it's not mentioned in jvm-server.options file
mkdir -p ${CASSANDRA_LOG_DIR}
# See notes about -Xlog in jvm11-server.options file
JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=${CASSANDRA_LOG_DIR}/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
# JVM information, gives basic information about OS, CPU, memory + container
JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=${CASSANDRA_LOG_DIR}/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
# Debugging JIT
#JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=${CASSANDRA_LOG_DIR}/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

# only add -Xloggc if it's not mentioned in jvm-server.options file
mkdir -p ${CASSANDRA_LOG_DIR}
JVM_OPTS="$JVM_OPTS -Xloggc:${CASSANDRA_LOG_DIR}/gc.log"





# only add -Xlog:gc if it's not mentioned in jvm-server.options file
mkdir -p ${CASSANDRA_HOME}/logs
# See notes about -Xlog in jvm11-server.options file
JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=${CASSANDRA_HOME}/logs/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
# JVM information, gives basic information about OS, CPU, memory + container
JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=${CASSANDRA_HOME}/logs/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
# Debugging JIT
#JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=${CASSANDRA_HOME}/logs/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

# only add -Xloggc if it's not mentioned in jvm-server.options file
mkdir -p ${CASSANDRA_HOME}/logs
JVM_OPTS="$JVM_OPTS -Xloggc:${CASSANDRA_HOME}/logs/gc.log"
