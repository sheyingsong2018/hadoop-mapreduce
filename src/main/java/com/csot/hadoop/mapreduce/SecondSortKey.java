package com.csot.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义key的比较
 *
 */
public class SecondSortKey implements WritableComparable<SecondSortKey> {

	// 定义两个字段
	private Integer first;
	private Integer second;

	public Integer getFirst() {
		return first;
	}

	public void setFirst(Integer first) {
		this.first = first;
	}

	public Integer getSecond() {
		return second;
	}

	public void setSecond(Integer second) {
		this.second = second;
	}

	public SecondSortKey() {
	};

	public SecondSortKey(Integer first, Integer second) {
		this.first = first;
		this.second = second;
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(this.first);
		output.writeInt(this.second);

	}

	public void readFields(DataInput input) throws IOException {
		this.first = input.readInt();
		this.second = input.readInt();
	}

	// 比较两个key值
	public int compareTo(SecondSortKey other) {
		// 先比较第一个key不相同的情况
		if (this.first != other.first) {
			return this.first - other.first;
		} else {
			// 比较第二列key不相等的情况
			if (this.second != other.second) {
				return this.second - other.second;
			} else {
				return 0;
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondSortKey other = (SecondSortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
}
