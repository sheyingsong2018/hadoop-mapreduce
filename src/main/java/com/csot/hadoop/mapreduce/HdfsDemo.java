package com.csot.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDemo {

	public static FileSystem getFileSystem() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		return fileSystem;
	}

	public static void getFileInputStream(String fileName) {
		Configuration conf = new Configuration();
		FSDataInputStream inStream = null;
		try {
			// 获取FileSystem
			FileSystem fileSystem = FileSystem.get(conf);
			// 读取数据的文件路径
			Path path = new Path(fileName);
			// 打开文件流
			inStream = fileSystem.open(path);
			// 输出内容
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}
	
	public static void main(String[] args) throws Exception {

		// FileSystem fileSystem = getFileSystem();
		// System.out.println(fileSystem);
		String fileName = "/demo/wordcount.txt";
		getFileInputStream(fileName);
		
	}
}
