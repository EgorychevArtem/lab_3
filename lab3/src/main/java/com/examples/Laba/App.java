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
    private static String EMPTY = "";
    private static String COMMA = ",";
    private static String FLIGHTINFO = "664600583_T_ONTIME_sample.csv";
    private static String AIRPORTINFO = "L_AIRPORT_ID.csv";
    private static int AIROPORT_ID = 11;
    private static int AIROPORT_CODE = 14;
    private static int AIROPORT_DELAY = 18;
    private static int CANCELLED = 19;

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
                .filter(s -> !s.contains("Code"))
                .mapToPair(s->{
                    s = s.replaceAll(DELIMETR,EMPTY);
                    int indexOfFirstComma = s.indexOf(COMMA);
                    return new Tuple2<>(
                            Integer.valueOf(getSubstring(s,0,indexOfFirstComma)),
                            getSubstring(s,indexOfFirstComma + 1,s.length())
                    );
                });


        final Broadcast<Map<Integer,String>> airportsBroadcasted = sc.broadcast(AirData.collectAsMap());

        JavaPairRDD<Tuple2<Integer,Integer>, Serializabl> FlightData = flightFile
                .filter(s -> !s.contains("YEAR"))
                .mapToPair(s->{
                    String[] str = s.split(COMMA);
                    int AiroportID = getParseInt(str[AIROPORT_ID]);
                    int DestAiroportID = getParseInt(str[AIROPORT_CODE]);
                    float DelayTime = getOptionalDelayString(str[AIROPORT_DELAY]);
                    float CancelledFlight = Float.parseFloat(str[CANCELLED]);
                    return new Tuple2<>(new Tuple2<>(AiroportID,DestAiroportID),
                            new Serializabl(AiroportID,DestAiroportID,DelayTime,CancelledFlight));
                });

        JavaPairRDD<Tuple2<Integer,Integer>, String> FlightResult = FlightData
                .combineByKey(
                        p -> new CalculateStatistic(1,
                                p.CancelledFlight == 1.0f ? 1:0,
                                p.Delay > 0.0f ? 1:0,
                                p.Delay),
                        (CalculateStatistic,p) -> CalculateStatistic.AddNewValue(
                                CalculateStatistic,
                                p.CancelledFlight == 1.0f,
                                p.Delay > 0.0f,
                                p.Delay),
                        CalculateStatistic::add)
                .mapToPair(
                        a -> new Tuple2<>(a._1(), CalculateStatistic.PrintResult(a._2()))
                );

        JavaRDD<String> result = FlightResult.map(
                a -> {
                    Map<Integer, String> AiroportDestID = airportsBroadcasted.value();
                    Tuple2<Integer, Integer> key = a._1();
                    String value = a._2();

                    return "from: " + AiroportDestID.get(key._1()) + " to: " + AiroportDestID.get(key._2()) + value;
                });
        result.saveAsTextFile("hdfs://localhost:9000/user/artem/output");
    }
}
