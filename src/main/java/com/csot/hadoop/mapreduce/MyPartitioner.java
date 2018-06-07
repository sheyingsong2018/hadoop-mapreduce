package com.csot.hadoop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 实现自定义分区器，实现getPartition方法，默认还是根据key的hash来计算
 *
 */
public class MyPartitioner extends Partitioner<SecondSortKey, Text> {

	@Override
	public int getPartition(SecondSortKey key, Text value, int numPartitions) {
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
