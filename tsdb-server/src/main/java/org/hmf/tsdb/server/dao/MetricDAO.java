package org.hmf.tsdb.server.dao;

import java.util.List;
import java.util.Map;

import org.hmf.tsdb.server.entity.Metric;


public interface MetricDAO extends BaseDAO<Metric,Integer>{
	
	/**
	 * 创建测点基本信息
	 * @param metric
	 * @param value
	 * @return
	 */
	public Metric createMetric(Metric metric,Number value)throws Exception;
	
	/**
	 * 查找测点信息
	 * @param name
	 * @param tags
	 * @return
	 */
	public List<Metric> findMetrics(String name,Map<String,String> tags);
	
	/**
	 * 根据查询条件删除测点信息
	 * @param name
	 * @param tags
	 * @return
	 */
	public void delete(String name,Map<String,String> tags);
	
	
}
