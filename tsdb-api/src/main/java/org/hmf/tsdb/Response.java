package org.hmf.tsdb;

/**
 * 远程调用的结果
 */
public class Response<T> {
	/**
	 * 调用是否成功
	 */
	private boolean success;
	
	/**
	 * 附加信息，比如调用失败时候的异常信息
	 */
	private String info;
	
	/**
	 * 结果正文
	 */
	private T body;
	
	public static <T> Response<T> success(T body){
		Response<T> result = new Response<T>();
		result.setBody(body);
		result.setSuccess(true);
		return result;
	}
	
	public static <T> Response<T> error(String info,T body){
		Response<T> result = new Response<T>();
		result.setSuccess(false);
		result.setInfo(info);
		result.setBody(body);
		return result;
	}
	
	public boolean isSuccess() {
		return success;
	}
	
	public void setSuccess(boolean success) {
		this.success = success;
	}
	
	public String getInfo() {
		return info;
	}
	
	public void setInfo(String info) {
		this.info = info;
	}
	
	public T getBody() {
		return body;
	}
	
	public void setBody(T body) {
		this.body = body;
	}
}
