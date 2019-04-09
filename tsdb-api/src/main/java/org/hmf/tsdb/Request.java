
package org.hmf.tsdb;


public class Request {
	public enum Method{
		auth,
		put,
		query;
	}
	
	/**
	 * 请求的方法
	 */
	private Method method;
	
	/**
	 * 请求的内容
	 */
	private Object data;
	
	/**
	 * 写数据时需要指定分组
	 */
	private int groupIdx;
	
	
	
	
	public int getGroupIdx() {
		return groupIdx;
	}
	public void setGroupIdx(int groupIdx) {
		this.groupIdx = groupIdx;
	}
	public Method getMethod() {
		return method;
	}
	public void setMethod(Method method) {
		this.method = method;
	}
	public Object getData() {
		return data;
	}
	public void setData(Object data) {
		this.data = data;
	}
	
}
