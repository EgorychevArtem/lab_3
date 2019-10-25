package com.examples.Laba;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App {
    private static String DELIMETR = "\"";
    private static String EMPTY = "\"";
    private static String COMMA = ",";
    private static String FLIGHTINFO = "664600583_T_ONTIME_sample.csv";
    private static String AIRPORTINFO = "L_AIRPORT_ID.csv";

    private static String getSubstring(String s, int first, int second){
        return s.substring(first,second);
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab_3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile(FLIGHTINFO);
        JavaRDD<String> airportFile = sc.textFile(AIRPORTINFO);

        JavaPairRDD<String> AirData = airportFile
                .mapToPair(s->{
                    s = s.replaceAll(DELIMETR,EMPTY);
                    int indexOfFirstComma = s.indexOf(COMMA);
                    return new Tuple2<>(
                            Integer.valueOf(getSubstring(s,0,indexOfFirstComma)),
                            //getSubstring(s,indexOfFirstComma++,s.length()),
                            s.substring(indexOfFirstComma+1,s.length())
                    );
                });

    }
}
