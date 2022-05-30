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


public class YouTubeStudent20191765 {
	
	public static class Youtube {
		public String cat;
		public Double avg;

		public Youtube(String cat, Double avg) {
			this.cat = cat;
			this.avg = avg;
		}

		public String getString() {
			return cat + " " + avg;
		}
	}

	public static class YoutubeComparator implements Comparator<Youtube> {
		public int compare(Youtube x, Youtube y) {
			return Double.compare(x.avg, y.avg);
		}
	}

	public static void insertYoutube(PriorityQueue q, String cat, Double avg, int topK) {
		Youtube head = (Youtube)q.peek();
		if (q.size() < topK || Double.compare(head.avg, avg) < 0) {
			Youtube Youtube = new Youtube(cat, avg);
			q.add(Youtube);
			if (q.size() > topK)
				q.remove();
		}
	}

	public static class YouTubeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\\|");
			insertYoutube(queue, strs[3], Double.parseDouble(strs[6]), topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Youtube Youtube = (Youtube)queue.remove();
				context.write(new Text(Youtube.getString()), NullWritable.get());
			}
		}
	}

	public static class YouTubeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private int topK;
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(key.toString(), " ");
			insertYoutube(queue, itr.nextToken(), Double.parseDouble(itr.nextToken()), topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Youtube Youtube = (Youtube)queue.remove();
				context.write(new Text(Youtube.getString()), NullWritable.get());
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
		Job job = new Job(conf, "YouTube student20191765");
		job.setJarByClass(YouTubeStudent20191765.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setReducerClass(YouTubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
