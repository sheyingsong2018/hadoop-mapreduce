package com.csot.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	/**
	 * Mapper 统计每个单词的数量，先将每行的单词按逗号拆分，然后在转换成<key,value>格式
	 * 
	 * @param args
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// 减少实例化对象
		private static final IntWritable mapOutputValue = new IntWritable(1);
		private Text mapOutputKey = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String strValue = value.toString().trim();
			StringTokenizer strTokenizer = new StringTokenizer(strValue, ",");
			while (strTokenizer.hasMoreTokens()) {
				String word = strTokenizer.nextToken();
				mapOutputKey.set(word);
				context.write(mapOutputKey, mapOutputValue);
			}
		}
	}

	/**
	 * Reducer 对每个单词进行数量统计
	 * 
	 * @param args
	 */

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(WordCount.class);

		// 设置mapper相关属性
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); // 输入路径

		// 设计reducer相关属性
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int status = new WordCount().run(args);
		System.exit(status);

	}
}
