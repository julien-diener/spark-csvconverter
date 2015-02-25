package test.spark.csvconvert;

import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;

/**
 * Provides a simple example of using spark to convert csv file.
 *
 * It must be possible to convert each line independently.
 * The output are part files such as produce by map-reduce.
 *
 * Author: julien-diener
 */
public final class Converter {
    static String appName = "CSV-Conversion";  // spark app name
    static String master = "local";            // spark master (see readme for details)

    /**
     * The function that convert each file line.
     *
     * It is static (i.e. does not requires 'this') and does not use other object.
     * If external objects (or not static method) are required, they must be
     * serializable so that a copy can be send to each worker node.
     * It is however better to avoid or at least minimize such data transfer.
     */
    public static String convertLine(String line){
        return line.toUpperCase();
    }


    public static void main(String[] args){
        if(args.length!=2) {
            System.out.println("Invalid number of arguments. Usage: HdfsCsvConverter inputFile outputDir");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputDir = args[1];

        FileUtil.fullyDelete(new File(outputDir));

        // Init spark context
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // file conversion using spark
        JavaRDD<String> inputRdd = sc.textFile(inputFile);
        //   for Java 8:
        //   JavaRDD<String> outputRdd = inputRdd.map(Converter::convertLine);

        //   for Java 7:
        Function fct = new Function<String,String>() {
            @Override
            public String call(String line) throws Exception {
                return convertLine(line);
            }
        };
        JavaRDD<String> outputRdd = inputRdd.map(fct);

        outputRdd.saveAsTextFile(outputDir);
    }
}
