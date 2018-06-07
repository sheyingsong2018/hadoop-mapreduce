package com.csot.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sun.tools.jar.resources.jar_pt_BR;

public class SecondarySort {

	// Mapper
	public static class MyMapper extends Mapper<LongWritable, Text, SecondSortKey, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] splited = line.split(" ");
			context.write(new SecondSortKey(Integer.valueOf(splited[0]), Integer.valueOf(splited[1])), value);
		}
	}

	// Reducer
	public static class MyReducer extends Reducer<SecondSortKey, Text, NullWritable, Text> {
		@Override
		protected void reduce(SecondSortKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(NullWritable.get(), text);
			}
		}
	}

	// run job
	public int run(String[] args) throws Exception {
		// 获取hadoop环境配置信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

		// 设置jar的运行类
		job.setJarByClass(SecondarySort.class);

		// 设置job的输入数据，input -> map -> reducer -> output
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);

		// 设置mapper的输出key和value类型
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(SecondSortKey.class);
		job.setMapOutputValueClass(Text.class);

		// 设置reducer的输出key和输出value类型
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置分区器
		job.setPartitionerClass(MyPartitioner.class);
		
		// 设置分组器
		job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
		
		// 手动设置reduce数量，因数据量不大
		job.setNumReduceTasks(1);
		
		// 输出路径
		Path outPut = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPut);

		// 提交job
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = new SecondarySort().run(args);
		System.exit(status);
	}
}