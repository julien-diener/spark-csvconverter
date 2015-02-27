package hdfs.csvconvert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.IOException;
import java.net.URI;

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
        String master = "local";
        String namenode = null;

        // parse arguments
        // ---------------
        int i = 0;
        while(i<(args.length-1) && args[i].startsWith("-")){
            switch(args[i].substring(1).toLowerCase()){
                case "master":   master   = args[i+1]; break;
                case "namenode": namenode = args[i+1]; break;
            }
            i += 2;
        }

        if(args.length-i!=2) {
            System.out.println("Invalid number of arguments");
            System.out.println("  Usage:  HdfsCsvConverter [-master spark://xxx:yy] [-namenode hdfs://...] inputFile outputDir");
            System.exit(1);
        }

        String inputFile = args[i];
        String outputDir = args[i+1];


        // delete output directory
        // -----------------------
        try {
            if (namenode != null) {
                Configuration conf = new Configuration();
                conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                FileSystem hdfs = FileSystem.get(URI.create(namenode), conf);

                hdfs.delete(new Path(outputDir), true);   // true => recursive
            } else {
                FileUtil.fullyDelete(new File(outputDir));
            }
        }catch (IOException e){
            System.out.println("\n*** could not delete output directory: "+outputDir);
            System.out.println(e.getMessage());
        }


        // Init spark context
        // ------------------
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.setJars(JavaSparkContext.jarOfClass(Converter.class));

        JavaSparkContext sc = new JavaSparkContext(conf);


        // file conversion with spark
        // --------------------------
        if(namenode!=null){
            inputFile = namenode+inputFile;
            outputDir = namenode+outputDir;
        }
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
