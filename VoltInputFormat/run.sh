#!/usr/bin/env bash

#set -o nounset #exit if an unset variable is used
set -o errexit #exit on any single command fail

# find voltdb binaries in either installation or distribution directory.
VOLTDB_BIN=$(dirname "$(which voltdb)")
# move voltdb commands into path for this script
PATH=$VOLTDB_BIN:$PATH

VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
VOLTDB_LIB="$VOLTDB_BASE/lib"
VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"

SPARK_BIN=$(dirname "$(which spark-submit)")
SPARK_BASE=$(dirname "$SPARK_BIN")
SPARK_LIB="$VOLTDB_BASE/lib"

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
    \ls -1 "$SPARK_LIB"/*.jar; \
    \ls -1 lib/*.jar; \
} 2> /dev/null | paste -sd ':' - )

JARS=$({ \
    \ls -1 lib/*.jar; \
} 2> /dev/null | paste -sd ',' - )
    
echo $JARS

# compile java source
javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH \
	src/org/voltdb/mapred/inputformat/*.java \
	src/org/voltdb/spark/*.java
jar cf spark-job.jar -C src org/voltdb
rm -rf src/org/voltdb/mapred/inputformat/*.class src/org/voltdb/spark/*.class

spark-submit --class org.voltdb.spark.SparkJob --master local[2] --jars $JARS spark-job.jar 
