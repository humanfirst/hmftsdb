package org.hmf.tsdb;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 降频采样参数
 */
public class DownSample {
	public enum Arithmetic{
		max,
		min,
		avg,
		first,
		last,
		sum,
	}
	
	/**
	 * 算法,为什么是一个算法集合呢？因为将来可能需要一次性遍历时就就算出降频后的最大值、最小值、平均值等
	 */
	private List<Arithmetic> arithmetics =  new ArrayList<Arithmetic>();
	
	/**
	 * 采样周期
	 */
	private int frequence;
	
	/**
	 * 采样周期时间单位
	 */
	private TimeUnit unit;

	public List<Arithmetic> getArithmetics() {
		return arithmetics;
	}

	public void setArithmetics(List<Arithmetic> arithmetics) {
		this.arithmetics = arithmetics;
	}


	public DownSample addArithmetic(Arithmetic arithmetic) {
		if(this.arithmetics==null) {
			this.arithmetics  = new ArrayList<Arithmetic>();
		}
		this.arithmetics.add(arithmetic);
		return this;
	}

	public int getFrequence() {
		return frequence;
	}

	public void setFrequence(int frequence) {
		this.frequence = frequence;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public void setUnit(TimeUnit unit) {
		this.unit = unit;
	}

	@Override
	public String toString() {
		return "DownSample [arithmetics=" + arithmetics + ", frequence=" + frequence + ", unit=" + unit + "]";
	}
	
}
