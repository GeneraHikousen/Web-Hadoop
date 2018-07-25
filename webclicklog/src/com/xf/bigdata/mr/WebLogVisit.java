package com.xf.bigdata.mr;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.beanutils.BeanUtils;
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

import com.xf.bigdata.mrBean.PageViewBean;
import com.xf.bigdata.mrBean.VisitBean;

/**
 * 
 * @author XF
 *
 */
public class WebLogVisit {
	private static class WebLogVisitMapper extends Mapper<LongWritable, Text, Text, PageViewBean> {
		private PageViewBean pvbean = new PageViewBean();
		private Text keyOut = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PageViewBean>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\001");
			pvbean.setAll(fields[0], fields[1], fields[2], fields[3], Integer.parseInt(fields[4]), fields[5], fields[6], fields[7],
					fields[8], fields[9]);
			keyOut.set(pvbean.getSession());
			context.write(keyOut, pvbean);
		}
	}
	
	private static class WebLogVisitReducer extends Reducer<Text, PageViewBean, NullWritable, VisitBean>{
		private ArrayList<PageViewBean> list = new ArrayList<>();
		private VisitBean visitBean = new VisitBean();
		@Override
		protected void reduce(Text k, Iterable<PageViewBean> v,
				Reducer<Text, PageViewBean, NullWritable, VisitBean>.Context context)
				throws IOException, InterruptedException {
			list.clear();
			for(PageViewBean pvBean : v) {
				PageViewBean pageViewBean = new PageViewBean();
				try {
					BeanUtils.copyProperties(pageViewBean, pvBean);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
				list.add(pageViewBean);
			}
			Collections.sort(list,new Comparator<PageViewBean>() {
				@Override
				public int compare(PageViewBean o1, PageViewBean o2) {
					return o1.getStep()-o2.getStep();
				}
			});
			
			visitBean.setSession(k.toString());
			visitBean.setRemote_addr(list.get(0).getRemote_addr());
			visitBean.setInTime(list.get(0).getTimestr());
			visitBean.setOutTime(list.get(list.size()-1).getTimestr());
			visitBean.setInPage(list.get(0).getRequest());
			visitBean.setOutPage(list.get(list.size()-1).getRequest());
			visitBean.setReferal(list.get(0).getReferal());
			visitBean.setPageVisits(list.size());
			context.write(NullWritable.get(), visitBean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WebLogVisit.class);
		job.setMapperClass(WebLogVisitMapper.class);
		job.setReducerClass(WebLogVisitReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);
		
		//本地模式跑MR
		FileInputFormat.setInputPaths(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\pageview"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\visit"));

		job.waitForCompletion(true);
	}
}
