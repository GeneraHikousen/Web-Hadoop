package com.xf.bigdata.mrBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 描述web日志的所有字段 将String转为webLogBean，使用com.xf.bigdata.mrBean.WebLogParser
 * 
 * @author XF
 *
 */
public class WebLogBean implements Writable {

	private boolean valid;
	private String remote_addr; // 用以记录客户端的ip地址
	private String remote_user; //用来记录客户端用户名称,这个web日志没有此字段
	private String time_local; // 用来记录访问时间与时区
	private String request; // 用来记录请求的http的方式与url,这里只保留了url
	private int status; // 用来记录请求状态；成功是200
	private String body_bytes_sent; // 记录发送给客户端文件主体内容大小
	private String http_referer; // 用来记录从那个页面链接访问过来的
	private String http_user_agent; // 记录客户毒啊浏览器的相关信息
	// private String request_time; //用来记录请求时间,这个web日志没有此字段
	// private String http_x_forwarded_for;
	//当前端有代理服务器时，设置web节点记录客户端地址的配置，此参数生效的前提是代理服务器也要进行相关的x_forwarded_for设置

	public void setAll(boolean valid, String remote_addr, String remote_user, String time_local, String request,
			int status, String body_bytes_sent, String http_referer, String http_user_agent) {
		this.valid = valid;
		this.remote_addr = remote_addr;
		this.remote_user = remote_user;
		this.time_local = time_local;
		this.request = request;
		this.status = status;
		this.body_bytes_sent = body_bytes_sent;
		this.http_referer = http_referer;
		this.http_user_agent = http_user_agent;
	}

	public WebLogBean() {

	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getTime_local() {
		return time_local;
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}
	
	public void setStatus(String status) {
		this.status = Integer.parseInt(status);
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		valid = in.readBoolean();
		remote_addr = in.readUTF();
		remote_user = in.readUTF();
		time_local = in.readUTF();
		request = in.readUTF();
		status = in.readInt();
		body_bytes_sent = in.readUTF();
		http_referer = in.readUTF();
		http_user_agent = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(valid);
		out.writeUTF(remote_addr);
		out.writeUTF(remote_user);
		out.writeUTF(time_local);
		out.writeUTF(request);
		out.writeInt(status);
		out.writeUTF(body_bytes_sent);
		out.writeUTF(http_referer);
		out.writeUTF(http_user_agent);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.valid);
		sb.append("\001").append(this.getRemote_addr());
		sb.append("\001").append(this.getRemote_user());
		sb.append("\001").append(this.getTime_local());
		sb.append("\001").append(this.getRequest());
		sb.append("\001").append(this.getStatus());
		sb.append("\001").append(this.getBody_bytes_sent());
		sb.append("\001").append(this.getHttp_referer());
		sb.append("\001").append(this.getHttp_user_agent());
		return sb.toString();
	}
}
