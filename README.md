
A simple java program to convert csv files on HDFS to another csv format, using spark.

More precisely, the constraint is that each line can be converted independently.
Also, the output are part files such as produced by map-reduce.

This example is primarily made to run locally, but can run on a spark cluster depending on the (1st) argument.
See the run section below fopr more details.

Author: [julien-diener](https://github.com/julien-diener)

**Compilation using maven:**

    mvn package

**Run:**

    java -cp "HdfsCsvConverter.jar:/path/to/spark-assembly-1.1.1-hadoop2.4.0.jar" \
         test.spark.csvconvert.Converter local /input/file /output/dir

    java -cp "HdfsCsvConverter.jar:/path/to/spark-assembly-1.1.1-hadoop2.4.0.jar" \
         test.spark.csvconvert.Converter spark://host:port /input/file /output/file

The first argument define how/where to run spark. This is the `master` parameter
[described here](https://spark.apache.org/docs/1.1.1/programming-guide.html#initializing-spark).

<dl>
  <dt>local</dt>
  <dd>Run locally<dd>

  <dt>spark://host:port</dt>
  <dd>Run on spark cluster where `host:port` is the address of the spark master</dd>
</dl>

The second and third arguments are the input file and the output directory to store the output file as map-reduce part
files. If it is of the form `/some/folder/some/file-or-dir` then it works on local disc. To run on hdfs, the format
`hdfs://host:port/some/file-or-dir` where `host:port` is the hdfs ipc interface. Note however that in the later case,
the deletion of the output folder preceding the file conversion does not work: if it already exist, the output folder
should be deleted manually before running the conversion.
