package org.hmf.tsdb.server.entity;

/**
 * 快照中的某个测点的时序数据
 */
public class Fragment {
	public int metricId;
	public byte[] data;
}
