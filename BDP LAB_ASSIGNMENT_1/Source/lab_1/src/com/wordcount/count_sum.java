package com.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class count_sum {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());
			String name = value.toString().split(":")[0];
			String value1 = value.toString().split(":")[1];
			context.write(new Text(name), new Text(value1));
		}

		public static class Combiner extends Reducer<Text, Text, Text, Text> {
			// private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				int sum = 0;
				int count = 0;
				Double avg = 0.0;

				for (Text val : values) {
					count++;
					sum += Double.parseDouble(val.toString());

				}
				avg = (double) (sum / count);
				context.write(new Text(key), new Text(avg + "," + count));
			}
		}

		public static class CReducer extends Reducer<Text, Text, Text, Text> {
			// private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				int sum = 0;
				int tcount = 0;
				Double avg = 0.0;
				String[] values1 = null;

				for (Text val : values) {
					values1 = val.toString().split(",");
					avg = Double.parseDouble(values1[0]);
					int count = Integer.parseInt(values1[1]);
					sum += avg * count;
					tcount += count;

				}
				avg = (double) (sum / tcount);
				context.write(new Text(key), new Text(avg.toString()));
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(count_sum.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(Combiner.class);
			job.setReducerClass(CReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
