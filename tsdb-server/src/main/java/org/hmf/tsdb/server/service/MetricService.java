package org.hmf.tsdb.server.service;

import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.entity.Metric;


public interface MetricService {
	public Map<Integer, Metric> getMetrics();
	public Metric findAndCreateMetric(DataPoint dp);
	public Metric findMetricById(Integer metricId);
	public List<Metric> findMetricsBy(Bson query);
}
