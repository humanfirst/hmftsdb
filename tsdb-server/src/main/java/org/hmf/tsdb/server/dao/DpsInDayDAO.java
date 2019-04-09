package org.hmf.tsdb.server.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.server.entity.DpsInDay;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Metric;

public interface DpsInDayDAO extends BaseDAO<DpsInDay,Long> {
	
	public DpsInDay findLastRecord(Integer metricId);

	public boolean exits(Long id);

	public Map<Metric, List<DpsInHour>> findDpsPack(Set<Metric> metrics, Date start, Date end, DownSample sample);
	
	public List<DpsInDay> findDayPacks(Metric metric,Date start,Date end,boolean withDetail);
	
	
}
