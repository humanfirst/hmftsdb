package org.hmf.tsdb.server.service.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.bson.conversions.Bson;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.dao.MetricDAO;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.service.MetricService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetricServiceImpl implements MetricService{
	@Autowired
	MetricDAO metricDAO;
	
	private static Map<String,Metric> metrics = null;
	
	private static Map<Integer,Metric> metricsMappedById = null;
	

	@PostConstruct
	public void init() {
		if(metrics==null) {
			metrics = new ConcurrentHashMap<String,Metric>();
			metricsMappedById = new ConcurrentHashMap<Integer,Metric>();
			List<Metric> metricsInDb = metricDAO.listAll();
			for(Metric metric :metricsInDb){
				metrics.put(metric.getKey(), metric);
				metricsMappedById.put(metric.getId(), metric);
			}
		}
	}
	
	@Override
	public Map<Integer, Metric> getMetrics() {
		return metricsMappedById;
	}

	@Override
	public synchronized Metric findAndCreateMetric(DataPoint dp) {
		Metric dto = this.getMetricFromDataPoint(dp);
		String key = dto.getKey();		
		Metric metric = metrics.get(key);	
		
		if(metric==null){
			try {
				metric = metricDAO.createMetric(dto,dp.getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
			metrics.put(key, metric);
			metricsMappedById.put(metric.getId(), metric);
		}
		return metric;
	}
	
	private Metric getMetricFromDataPoint(DataPoint dp) {
		Metric m = new Metric();
		m.setName(dp.getMetric());
		m.setTags(dp.getTags());
		return m;
	}

	@Override
	public Metric findMetricById(Integer metricId) {
		return this.metricsMappedById.get(metricId);
	}

	@Override
	public List<Metric> findMetricsBy(Bson query) {
		return this.metricDAO.findList(query);
	}

}
