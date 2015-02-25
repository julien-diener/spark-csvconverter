
A simple java program to convert csv files on HDFS to another csv format, using spark.

More precisely, the constraint is that each line can be converted independently.
Also, the output are part files such as produced by map-reduce.

This example is made to run locally.
See [this](https://spark.apache.org/docs/1.1.1/programming-guide.html#initializing-spark) for more details

Author: julien-diener

**Compilation using maven:**

    mvn package

**To run:**

    java -cp "HdfsCsvConverter.jar:/path/to/spark-assembly-1.1.1-hadoop2.4.0.jar" test.spark.csvconvert.Converter inputFile outputDir