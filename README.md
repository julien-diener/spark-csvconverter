
A simple java program to convert csv files on HDFS to another csv format, using spark.

More precisely:

 - Any text file can be converted if all lines can be converted independently
 - It can run locally or on a spark cluster (see the `-master` argument)
 - It can run on the local FS or on hdfs (see the `-namenode` argument)
 - Output are part files such as produced by map-reduce
 - If it exists, the output folder is deleted before running the conversion

Author: [julien-diener](https://github.com/julien-diener)

**Compilation using maven:**

    mvn package

**Run:**

    java -cp "HdfsCsvConverter.jar:/path/to/spark-assembly-1.1.1-hadoop2.4.0.jar" \
         hdfs.csvconvert.Converter \
         [-master   spark://xxxxx:pppp] \
         [-namenode hdfs://yyyyyy:pppp] \
         /input/file /output/dir


`-master` (optional)

By default spark run locally (-master local) but other spark master can be used. See the `master` parameter
      [described here](https://spark.apache.org/docs/1.1.1/programming-guide.html#initializing-spark). For example,
      the address of a spark master can be given. </dd>

`-namenode` (optional)

By default, the /input/file and /output/dir are looked on the computer running the program. The namenode option
can be used to give the address of the HDFS namenode, in which case the input and output will be on the HDFS

`/input/file`

Path to the file to convert

`/output/dir`

Path to the output directory, where the output is stored as map-reduce part files

