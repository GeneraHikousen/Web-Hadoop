package com.xf.bigdata.mrBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;


public class WebLogParser {

	private static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	private static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();
		webLogBean.setValid(true);
		String[] fields = line.split(" ");
		if (fields.length >= 12) {
			webLogBean.setRemote_addr(fields[0]);
			webLogBean.setRemote_user(fields[1]);
			webLogBean.setRequest(fields[6]);
			webLogBean.setStatus(Integer.parseInt(fields[8]));
			webLogBean.setBody_bytes_sent(fields[9]);
			webLogBean.setHttp_referer(fields[10]);
			String time_local = formatDate(fields[3].substring(1));
			if (null == time_local)
				time_local = "-invalid_time-";
			webLogBean.setTime_local(time_local);
			StringBuilder agent = new StringBuilder();
			for (int i = 11; i < fields.length; i++) {
				agent.append(fields[i] + " ");
			}
			agent.deleteCharAt(agent.length() - 1);
			webLogBean.setHttp_user_agent(agent.toString());

			if (webLogBean.getStatus() >= 400 || webLogBean.getTime_local().equals("-invalid_time-")) {
				webLogBean.setValid(false);
			}
		} else {
			webLogBean.setValid(false);
		}
		return webLogBean;
	}
	
	private  static String formatDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static void filtStaticResource(WebLogBean webLogBean,HashSet<String> pages) {
		if(!pages.contains(webLogBean.getRequest())) {
			webLogBean.setValid(false);
		}
	}
}
