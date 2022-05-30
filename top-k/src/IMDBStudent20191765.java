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
	
	class Movie {
		public String name;
		public Double avg;

		public Movie(String name, Double avg) {
			this.name = name;
			this.avg = avg;
		}
	}

	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			return Double.compare(x.avg, y.avg);
		}
	}

	public static void insertMovie(PriorityQueue q, String name, Double avg) {
		Movie head = (Movie)q.peek();
		if (q.size() < topK || Double.compare(head.avg, avg) < 0) {
			Movie movie = new Movie(name, avg);
			q.add(avg);
			if (q.size() > topK)
				q.remove();
		}
	}

	public static class IMDBMapper extends Mapper<LongWritable, Text, Text, Text> {
		boolean fileA = true;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("::");
			Text outputKey = new Text();
			Text outputValue = new Text();
			String joinKey = "";
			String o_value = "";
			if (fileA) {
				if (strs[2].indexOf("Fantasy") != -1) {
					joinKey = strs[0];
					o_value = "A::" + strs[1];
					outputKey.set(joinKey);
					outputValue.set(o_value);
					context.write(outputKey, outputValue);
				}
			}
			else {
				joinKey = strs[1];
				o_value = "B::" + strs[2];
				outputKey.set(joinKey);
				outputValue.set(o_value);
				context.write(outputKey, outputValue);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

			if (filename.indexOf("movies.dat") != -1)
				fileA = true;
			else
				fileA = false;
		}
	}

	public static class IMDBReducer extends Reducer<Text, Text, Text, Text> {
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String desc = "";
			String o_value = "";
			int sum = 0;
			ArrayList<String> buffer = new ArrayList<>();

			for (Text v : values) {
				String[] str = v.toString().split("::");
				if (str[0].equals("A")) {
					desc = str[1];
				}
				else {
					buffer.add(str[1]);
				}
			}
			if (desc.length() != 0 && buffer.size() != 0) {
				for (String s : buffer) {
					sum += Integer.parseInt(s);
				}
				insertMovie(queue, desc, sum / (float)buffer.size());
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Movie movie = (Movie)queue.remove();
				context.write(new Text(movie.name), new Text(Double.toString(movie.avg)));
			}
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
