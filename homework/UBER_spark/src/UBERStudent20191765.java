import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Calendar;

public final class UBERStudent20191765 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBER")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

	FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
		public Iterator<String> call(String s) {
			String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
			Calendar calendar = Calendar.getInstance();	
			String[] infos = s.split(",");
			String[] date = infos[1].split("/");
			calendar.set(Calendar.YEAR, Integer.parseInt(date[2]));
			calendar.set(Calendar.MONTH, Integer.parseInt(date[2]) - 1);
			calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(date[1]));
			
			return Arrays.asList(infos[0] + "," + week[calendar.get(Calendar.DAY_OF_WEEK) - 1] + "::" + infos[3] + "," + infos[2]).iterator();
		}
	};
	JavaRDD<String> words = lines.flatMap(fmf);
	
	PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) {
			String[] strs = s.split("::");
			return new Tuple2(strs[0], strs[1]);
		}
	};
	JavaPairRDD<String, String> values = words.mapToPair(pf);
	
	Function2<String, String, String> f2 = new Function2<String, String, String>() {
	public String call(String x, String y) {
			String[] s1 = x.split(",");
			String[] s2 = x.split(",");
			int[] sum = {0, 0};

			sum[0] = Integer.parseInt(s1[0]) + Integer.parseInt(s2[0]);
			sum[1] = Integer.parseInt(s1[1]) + Integer.parseInt(s2[1]);			
			return Integer.toString(sum[0]) + "," + Integer.toString(sum[1]);
		}
	};	

	JavaPairRDD<String, String> counts = values.reduceByKey(f2);
	counts.saveAsTextFile(args[1]);
        //Write your code...
        
	spark.stop();
    }
}
