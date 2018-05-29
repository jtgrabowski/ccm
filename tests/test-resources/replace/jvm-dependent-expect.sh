# only add -Xlog:gc if it's not mentioned in jvm-server.options file
mkdir -p /foo/bar/baz/log-directory
# See notes about -Xlog in jvm11-server.options file
JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=/foo/bar/baz/log-directory/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
# JVM information, gives basic information about OS, CPU, memory + container
JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=/foo/bar/baz/log-directory/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
# Debugging JIT
#JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=/foo/bar/baz/log-directory/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

# only add -Xloggc if it's not mentioned in jvm-server.options file
mkdir -p /foo/bar/baz/log-directory
JVM_OPTS="$JVM_OPTS -Xloggc:/foo/bar/baz/log-directory/gc.log"





# only add -Xlog:gc if it's not mentioned in jvm-server.options file
mkdir -p /foo/bar/baz/home/log-directory
# See notes about -Xlog in jvm11-server.options file
JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=/foo/bar/baz/home/log-directory/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
# JVM information, gives basic information about OS, CPU, memory + container
JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=/foo/bar/baz/home/log-directory/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
# Debugging JIT
#JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=/foo/bar/baz/home/log-directory/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

# only add -Xloggc if it's not mentioned in jvm-server.options file
mkdir -p /foo/bar/baz/home/log-directory
JVM_OPTS="$JVM_OPTS -Xloggc:/foo/bar/baz/home/log-directory/gc.log"
