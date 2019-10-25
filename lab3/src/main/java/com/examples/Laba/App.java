package com.examples.Laba;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import java.util.Optional;

public class App {
    private static String DELIMETR = "\"";
    private static String EMPTY = "\"";
    private static String COMMA = ",";
    private static String FLIGHTINFO = "664600583_T_ONTIME_sample.csv";
    private static String AIRPORTINFO = "L_AIRPORT_ID.csv";

    private static String getSubstring(String s, int first, int second){
        return s.substring(first,second);
    }

    private static int getParseInt(String str){
        return Integer.parseInt(str);
    }

    private static float getOptionalDelayString(String str){
        if(str.isEmpty()) {
            return 0.0f;
        } else {
            return Float.parseFloat(str);
        }
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab_3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightFile = sc.textFile(FLIGHTINFO);
        JavaRDD<String> airportFile = sc.textFile(AIRPORTINFO);

        JavaPairRDD<Integer,String> AirData = airportFile
                .mapToPair(s->{
                    s = s.replaceAll(DELIMETR,EMPTY);
                    int indexOfFirstComma = s.indexOf(COMMA);
                    return new Tuple2<>(
                            Integer.valueOf(getSubstring(s,0,indexOfFirstComma)),
                            getSubstring(s,indexOfFirstComma++,s.length())
                    );
                });


        final Broadcast<Map<Integer,String>> airportsBroadcasted = sc.broadcast(AirData.collectAsMap());

        JavaPairRDD<Tuple2<Integer,Integer>, Serializabl> FlightData = flightFile
                .mapToPair(s->{
                    String[] str = s.split(COMMA);
                    int AiroportID = getParseInt(str[11]);
                    int DestAiroportID = getParseInt(str[14]);
                    float DelayTime = getOptionalDelayString(str[18]);
                    float CancelledFlight = Float.parseFloat(str[19]);
                    return new Tuple2<>(new Tuple2<>(AiroportID,DestAiroportID),
                            new Serializabl(AiroportID,DestAiroportID,DelayTime,CancelledFlight));
                });

        JavaPairRDD<Tuple2<Integer,Integer>, Serializabl> FlightResult = FlightData
                .combineByKey(
                        p-> new CalculateStatistic(1,
                                p.CancelledFlight == 1.0f ? 1:0,
                                p.)
                );






















    }
}
