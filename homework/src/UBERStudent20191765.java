import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191765 {
	
	public static class UBERMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text u_key = new Text();
		private Text u_value = new Text();
		private String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
		private Calendar calendar = Calendar.getInstance(); 

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] infos = value.toString().split(",");
			String[] date = infos[1].split("/");
			calendar.set(Calendar.YEAR, Integer.parseInt(date[2]));
			calendar.set(Calendar.MONTH, Integer.parseInt(date[0]) - 1);
			calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(date[1]));
			u_key.set(infos[0] + "," + week[calendar.get(Calendar.DAY_OF_WEEK) - 1]);
			u_value.set(infos[3] + "," + infos[2]);
			context.write(u_key, u_value);
		}
	}

	public static class UBERReducer extends Reducer<Text, Iterable<Text>, Text, Text> {
		private int sum[] = {0, 0};
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text v : values) {
				String[] splitValues = v.toString().split(",");
				sum[0] += Integer.parseInt(splitValues[0]);
				sum[1] += Integer.parseInt(splitValues[1]);
			}
			result.set(Integer.toString(sum[0]) + "," + Integer.toString(sum[1]));
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.exit(2);
		}

		Job job = new Job(conf, "uber student20191765");
		job.setJarByClass(UBERStudent20191765.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}