package test.spark.csvconvert;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by juh on 11/02/15.
 */
public class Converter {
    static String appName = "CSV-Conversion";
    static String master = "local";

    JavaSparkContext sc;

    public Converter(){
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        sc = new JavaSparkContext(conf);
    }

    public static String convertLine(String line){
        return line.toUpperCase();
    }

    public void convertFile(String inputFile, String outputFile){
        JavaRDD<String> inputRdd = sc.textFile(inputFile);
        JavaRDD<String> outputRdd = inputRdd.map(s -> convertLine(s));
        outputRdd.saveAsTextFile(outputFile);
    }

    public static void main(String[] args){
        if(args.length!=2) {
            System.out.println("Invalid number of arguments. Usage: Converter inputFile outputFile");
            System.exit(1);
        }

        String inputFile  = args[0];
        String outputFile = args[1];

        Converter c = new Converter();
        c.convertFile(inputFile,outputFile);
    }
}
