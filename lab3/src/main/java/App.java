import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App {
    private static String DELIMETR = "\"";
    private static String EMPTY = "\"";
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab_3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");

        JavaRDD<String> AirData = airportFile
                .mapToPair(s->{
                   s= s.replaceAll(DELIMETR,EMPTY);
                   
                });
    }
}
