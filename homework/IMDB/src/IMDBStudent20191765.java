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

public class IMDBStudent20191765 {
	
	public static class IMDBMapper extends Mapper<LongWritable,  Text, Text, IntWritable> {
		private Text g_key = new Text();
		private IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] genres = value.toString().split("::")[2].split("\\|");
			for (String g : genres) {
				g_key.set(g.trim());
				context.write(g_key, one);
			}
		}
	}

	public static class IMDBReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.exit(2);
		}

		Job job = new Job(conf, "imdb student20191765");
		job.setJarByClass(IMDBStudent20191765.class);
		job.setMapperClass(IMDBMapper.class);
		job.setCombinerClass(IMDBReducer.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
