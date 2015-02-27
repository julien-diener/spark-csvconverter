package test.spark.csvconvert;

import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.IOException;

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
    //static String master = "local";            // spark master: local run (see readme for details)
    //static String master = "yarn-client";      // spark master:  run with yarn (see readme for details)

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
        if(args.length!=3) {
            System.out.println("Invalid number of arguments");
            System.out.println("  Usage:  HdfsCsvConverter [local|yarn-client|spark://xxx:yy] inputFile outputDir");
            System.exit(1);
        }

        String inputFile = args[1];
        String outputDir = args[2];

        System.out.println("\n\n *****************************");
        File output = new File(outputDir);
        System.out.println(output.getPath());
        try {
            for (String filename : FileUtil.list(output))
                System.out.println(" - " + filename);
        }catch (IOException e){
            System.out.println("could not read folder:"+e);
        }
        boolean deleted = FileUtil.fullyDelete(new File(outputDir));
        System.out.println(" *****************************\n\n");


        // Init spark context
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.setMaster(args[0]);
        conf.setJars(JavaSparkContext.jarOfClass(Converter.class));

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
