package org.hmf.tsdb;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 某个测定多次采样值的序列
 *
 */
public class DataPointSerial {
	/**
	 * 所属指标
	 */
	private String metric;
	
	private TreeMap<String,String> tags;
	
	/**
	 * 开始时间
	 */
	private long start;
	
	/**
	 * 结束时间
	 */
	private long end;
	
	private Map<String,Number> values = new LinkedHashMap<String,Number>();

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public TreeMap<String, String> getTags() {
		return tags;
	}

	public void setTags(TreeMap<String, String> tags) {
		this.tags = tags;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public Map<String, Number> getValues() {
		return values;
	}

	public void setValues(Map<String, Number> values) {
		this.values = values;
	}
	
	public void addDataPoint(long time,Number value){
		values.put(Long.toString(time), value);
	}
	
}
