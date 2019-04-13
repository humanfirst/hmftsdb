package org.hmf.tsdb.demo;

import java.util.TreeMap;

/**
 * 时序值
 */
public class DataPoint {
	
	/**
	 * 测点
	 */
	private String metric;
	
	/**
	 * 测点的tags
	 */
	private TreeMap<String,String> tags;
	
	/**
	 * 采样时间
	 */
	private Long time;
	
	/**
	 * 采样值,可以是float或者int
	 */
	private Number value;
	
	public DataPoint() {
	}
	
	public DataPoint(String metric, TreeMap<String,String> tags ,long sampleAt, float value) {
		this.metric = metric;
		this.time = sampleAt;
		this.value = value;
		this.tags = tags;
	}

	public Number getValue() {
		return value;
	}
	
	public void setValue(Number value) {
		this.value = value;
	}
	
	public Long getTime() {
		return time;
	}
	
	public void setTime(Long time) {
		this.time = time;
	}

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

	@Override
	public String toString() {
		return "DataPoint [metric=" + metric + ", tags=" + tags + ", time=" + time + ", value=" + value + "]";
	}

}

