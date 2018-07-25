package com.xf.bigdata.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

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
import com.xf.bigdata.mrBean.WebLogBean;


/**
 * 从贴源表中获取PageView表的MapReduce程序
 * 贴源表：
 *  见{@link com.xf.bigdata.mrBean.WebLogBean}
 *  一行的内容如下：
 * 	true194.237.142.21-2013-09-19 06:26:36/wp-content/uploads/2013/07/rstudio-login.png3040"-""Mozilla/4.0 (compatible;)"
 * Pageview表：
 *  见 {@link com.xf.bigdata.mrBean.PageViewBean}
 * @author XF
 *
 */
public class WebLogPageView {
	private static class PageViewMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {
		private Text k = new Text();
		private WebLogBean webLogBean = new WebLogBean();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WebLogBean>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\001");
			webLogBean.setAll(fields[0].equals("true"), fields[1], fields[2], fields[3], fields[4],
					Integer.parseInt(fields[5]), fields[6], fields[7], fields[8]);
			k.set(webLogBean.getRemote_addr());
			context.write(k, webLogBean);
		}
	}

	private static class PageviewReducer extends Reducer<Text, WebLogBean, NullWritable, PageViewBean> {

		private ArrayList<WebLogBean> list = new ArrayList<>();
		private PageViewBean pageViewBean = new PageViewBean();
		private static final String DEFAULT_STAYTIME = "60";	//默认访问时间
		private SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void reduce(Text keyin, Iterable<WebLogBean> values,
				Reducer<Text, WebLogBean, NullWritable, PageViewBean>.Context context)
				throws IOException, InterruptedException {
			list.clear();
			for (WebLogBean bean : values) {
				WebLogBean webLogBean = new WebLogBean();
				try {
					BeanUtils.copyProperties(webLogBean, bean);
				} catch(Exception e) {
					e.printStackTrace();
				}
				list.add(webLogBean);
			}
			// 按照时间大小排序
			Collections.sort(list, new Comparator<WebLogBean>() {

				@Override
				public int compare(WebLogBean o1, WebLogBean o2) {
					if (o1 == o2) // as same as: o1==null&&o2==null
						return 0;
					if (o1 == null)
						return -1;
					if (o2 == null)
						return 1;
					return o1.getTime_local().compareTo(o2.getTime_local());
				}
			});
			int step = 1;
			String sessionID = UUID.randomUUID().toString();
			for (int i = 0; i < list.size(); i++) {
				WebLogBean bean = list.get(i);
				
				//如果只有一个访问记录,直接输出
				if (list.size() == 1) {
					pageViewBean.setAll(sessionID, bean.getRemote_addr(), bean.getTime_local(), bean.getRemote_user(),
							step, DEFAULT_STAYTIME, bean.getHttp_referer(), bean.getHttp_user_agent(),
							bean.getBody_bytes_sent(), String.valueOf(bean.getStatus()));
					context.write(NullWritable.get(), pageViewBean);
					return;
				}
				//如果是第一条记录，跳过
				if (step == 1) {
					step++;
					continue;
				}
				//计算和前一条记录的时间差
				long timeDiff = getTimeDiff(list.get(i - 1).getTime_local(), list.get(i).getTime_local());
				
				//如果时间差小于30min，则本条记录和上条记录在同一个session
				if (timeDiff < 30 * 60 * 1000) {
					int timeSeconds = (int) (timeDiff / 1000);
					WebLogBean last = list.get(i-1);
					pageViewBean.setAll(sessionID, last.getRemote_addr(), last.getTime_local(), last.getRequest(), step,
							String.valueOf(timeSeconds), last.getHttp_referer(), last.getHttp_user_agent(),
							last.getBody_bytes_sent(), String.valueOf(last.getStatus()));
					context.write(NullWritable.get(), pageViewBean);
					step++;
				}else { //时间差大于等于30min，视为不同session
					WebLogBean last = list.get(i-1);
					pageViewBean.setAll(sessionID, last.getRemote_addr(), last.getTime_local(), last.getRequest(), step,
							DEFAULT_STAYTIME, last.getHttp_referer(), last.getHttp_user_agent(),
							last.getBody_bytes_sent(), String.valueOf(last.getStatus()));
					context.write(NullWritable.get(), pageViewBean);
					step = 1;
					sessionID = UUID.randomUUID().toString();
				}
				if(i==list.size()-1) {	//如果当前是最后一条记录，直接输出
					pageViewBean.setAll(sessionID, bean.getRemote_addr(), bean.getTime_local(), bean.getRequest(),
							step, DEFAULT_STAYTIME, bean.getHttp_referer(), bean.getHttp_user_agent(),
							bean.getBody_bytes_sent(), String.valueOf(bean.getStatus()));
					context.write(NullWritable.get(), pageViewBean);
				}
			}
		}

		private long getTimeDiff(String time_local1, String time_local2) {
			Date date1 = null, date2 = null;
			try {
				date1 = df1.parse(time_local1);
				date2 = df1.parse(time_local2);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return Math.abs(date2.getTime() - date1.getTime());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WebLogPageView.class);
		job.setMapperClass(PageViewMapper.class);
		job.setReducerClass(PageviewReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PageViewBean.class);
		
		//本地模式跑MR
		FileInputFormat.setInputPaths(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\output"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\BaiduNetdiskDownload\\旧版\\day12\\作业题\\pageview"));

		job.waitForCompletion(true);
	}
}
