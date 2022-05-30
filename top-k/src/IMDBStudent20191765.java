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
	
	public static class IMDBMapper extends Mapper<LongWritable, Text, Text, Text> {
		boolean fileA = true;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("::");
			Text outputKey = new Text();
			Text outputValue = new Text();
			String joinKey = "";
			String o_value = "";
			if (fileA) {
				joinKey = strs[0];
				o_value = "A," + strs[1];
			}
			else {
				joinKey = strs[1];
				o_value = "B," + strs[2];
			}
			outputKey.set(joinKey);
			outputValue.set(o_value);
			context.write(outputKey, outputValue);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

			if (filename.indexOf("movies.dat") != -1)
				fileA = true;
			else
				fileA = false;
		}
	}

	public static class IMDBReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			double sum = 0.0;
			int	cnt = 0;
			ArrayList<String> buffer = new ArrayList<String>();
			for (Text v : values) {
				String file_type;
				file_type = v.toString()[0];
				if (file_type.equals("A")) {
					reduce_key.set(v.toString().split(",")[1]);
				}
				else {
					cnt++;
					sum += Double.parseDouble(v.toString().split(",")[1]);
				}
			}
			reduce_result.set(sum / cnt);
			context.write(reduce_key, reduce_result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = Integer.parseInt(otherArgs[2]);
		if (otherArgs.length != 3)
		{
			System.exit(2);
		}
		conf.setInt("topK", topK);
		Job job = new Job(conf, "imdb student20191765");
		job.setJarByClass(IMDBStudent20191765.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
