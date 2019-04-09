package org.hmf.tsdb.server.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Metric;


public interface DpsInHourDAO extends BaseDAO<DpsInHour,Long> {
	
	public Map<Metric,List<DpsInHour>> findSerials(Set<Metric> metrics, Date start,Date end,DownSample sample);

	public Date findFirstTimestamp(Integer metricId);

	public boolean exitsDpsAfterSecond(Integer metricId, long endSecond);

	public void removeDpsBefore(Integer metricId, Date date);

	public boolean exists(Long id);

	public List<DpsInHour> findDpsPack(Integer metricId, Date start, Date end, DownSample sample);

}
