package com.csot.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupComparator extends WritableComparator {

	public SecondarySortGroupComparator() {
		super(SecondSortKey.class, true);
	}
}
