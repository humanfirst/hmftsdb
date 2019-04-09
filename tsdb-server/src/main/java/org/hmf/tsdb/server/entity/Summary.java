package org.hmf.tsdb.server.entity;

/**
 * 某侧点在某时段的汇总信息
 */
public class Summary {
	/**
	 * 初始值
	 */
	private float begin;
	/**
	 * 终值
	 */
	private float end;
	
	/**
	 * 最大值
	 */
	private float max;
	/**
	 * 最小值
	 */
	private float min;
	/**
	 * 平均值
	 */
	private float avg;
	
	public float getBegin() {
		return begin;
	}
	
	public void setBegin(float begin) {
		this.begin = begin;
	}
	
	public float getEnd() {
		return end;
	}
	
	public void setEnd(float end) {
		this.end = end;
	}
	
	public float getMax() {
		return max;
	}
	
	public void setMax(float max) {
		this.max = max;
	}
	
	public float getMin() {
		return min;
	}
	
	public void setMin(float min) {
		this.min = min;
	}
	
	public float getAvg() {
		return avg;
	}
	
	public void setAvg(float avg) {
		this.avg = avg;
	}
	
	
	
}
