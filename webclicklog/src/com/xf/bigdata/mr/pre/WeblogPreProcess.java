package com.xf.bigdata.mr.pre;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xf.bigdata.mrBean.WebLogBean;
import com.xf.bigdata.mrBean.WebLogParser;

public class WeblogPreProcess {

	private static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, NullWritable, WebLogBean>{
		private HashSet<String> pages = new HashSet<>();
		/**
		 * 从外部加载网站url分类数据
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			pages.add("/about");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, WebLogBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean logbean = WebLogParser.parser(line);
//			由于数据量太少，不去除静态资源链接
//			WebLogParser.filtStaticResource(logbean, pages);
			if(logbean.isValid()) {
				context.write(NullWritable.get(),logbean);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WeblogPreProcess.class);
		job.setMapperClass(WeblogPreProcessMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WebLogBean.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\input"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\output2"));
		job.waitForCompletion(true);
	}
}
