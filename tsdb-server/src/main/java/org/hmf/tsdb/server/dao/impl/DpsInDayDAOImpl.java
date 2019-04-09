package org.hmf.tsdb.server.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.server.dao.DpsInDayDAO;
import org.hmf.tsdb.server.dao.MetricDAO;
import org.hmf.tsdb.server.entity.DpsInDay;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.entity.Summary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Repository(value="dpsInDay")
public class DpsInDayDAOImpl extends BaseDAOImpl<DpsInDay,Long> implements DpsInDayDAO{
	@Autowired
	MetricDAO metricDAO ;
	
	
	@Override
	public List<DpsInDay> findDayPacks(Metric metric, Date start, Date end,boolean withDetail) {
		List<Bson> conditions = new ArrayList<Bson>();
		conditions.add(Filters.eq("metricId",metric.getId()));
		conditions.add(Filters.gte("timestamp", start));
		if(end!=null) {
			conditions.add(Filters.lte("timestamp", end));
		}
		Bson query = Filters.and(conditions);
		int type = withDetail==true?3:2;
		BasicDBObject projection = this.getProjection(type);
		List<DpsInDay> list = super.findList(query,projection, Sorts.ascending("timestamp"));
		return list;
	}
	
	@Override
	public Map<Metric,List<DpsInHour>> findDpsPack(Set<Metric> metrics,Date start,Date end,DownSample sample){
		Map<Metric,List<DpsInHour>> result = new ConcurrentHashMap<Metric,List<DpsInHour>>();
		metrics.parallelStream().forEach(metric->findDpsInHourAndCollect(metric,start,end,sample,result));		
		return result;
	}
	
	private void findDpsInHourAndCollect(Metric metric,Date start,Date end,DownSample sample,Map<Metric,List<DpsInHour>> result) {
		List<DpsInHour> packs = findDpsPackInHour(metric,start,end,sample);
		result.put(metric, packs);
	}
	
	/**
	 * @param type type的值==3,则返回所有字段，==1返回detail，==2返回summary
	 * @return
	 */
	private BasicDBObject getProjection(int type) {
		BasicDBObject projection = new BasicDBObject();
		projection.append("timestamp", 1);
		if(type==3) {
			for(int i=0;i<24;i++) {
				projection.append("summary_"+i, 1);
				projection.append("detail_"+i, 1);
			}
		}else if(type==2){
			for(int i=0;i<24;i++) {
				projection.append("summary_"+i, 1);
			}
		}else if(type==1) {
			for(int i=0;i<24;i++) {
				projection.append("detail_"+i, 1);
			}
		}
		return projection;		
	}
	
	private BasicDBObject getProjection(DownSample sample) {
		BasicDBObject projection = new BasicDBObject();
		projection.append("_id", 1);
		if(sample!=null){
			if(sample.getUnit().equals(TimeUnit.HOURS)){
				for(int i=0;i<24;i++) {
					projection.append("summary_"+i, 1);
				}
			}else {
				for(int i=0;i<24;i++) {
					projection.append("detail_"+i, 1);
				}
			}
		}else {
			for(int i=0;i<24;i++) {
				projection.append("detail_"+i, 1);
			}
		}
		return projection;
	}
	
	
	private List<DpsInHour> findDpsPackInHour(Metric metric,Date startDate,Date endDate,DownSample sample){
		List<DpsInHour> dps = new ArrayList<DpsInHour>();	
		List<Bson> conditions = new ArrayList<Bson>();
		conditions.add(Filters.eq("metricId",metric.getId()));
		conditions.add(Filters.gte("timestamp", startDate));
		if(endDate!=null) {
			conditions.add(Filters.lte("timestamp", endDate));
		}
		Bson query = Filters.and(conditions);
		BasicDBObject projection = this.getProjection(sample);
		List<DpsInDay> list = super.findList(query,projection, Sorts.ascending("timestamp"));
		for(DpsInDay one:list) {
			Date date = one.getTimestamp();
			long startHour =  date.getTime()/3600000;
			Map<String,Object> data = one.getData();
			for(int i=0;i<24;i++) {
				long hour = startHour+i;
				long zeroAfterHour = hour * 3600*1000;
				if(zeroAfterHour>=startDate.getTime() && zeroAfterHour<endDate.getTime()) {
					DpsInHour dpsInHour =null;
					Summary summary = (Summary)data.get("summary_"+i);
					byte[] bytes = (byte[]) data.get("detail_"+i);
					if(summary!=null || bytes!=null) {
						if(summary!=null) {
							dpsInHour = new DpsInHour(metric,hour,summary);
						}
						if(bytes!=null) {
							dpsInHour = new DpsInHour(metric,hour,bytes);
						}
						dps.add(dpsInHour);
					}
				}
			}
		}
		return dps;
	}


	@PostConstruct
	public void init() {
		
	}

	@Override
	protected DpsInDay readEntityFromDocument(Document doc) {
		long id = doc.getLong("_id");
		DpsInDay dpsInDay = null;
		Map<String,Object> data = new LinkedHashMap<String,Object>();
		for(int i=0;i<24;i++) {
			String sKey = "summary_"+i;
			String dKey = "detail_"+i;
			Document summaryInHour = (Document) doc.get(sKey);
			Binary detail = (Binary) doc.get(dKey);
			if(summaryInHour!=null) {
				Summary summary = new Summary();
				summary.setAvg(summaryInHour.getDouble("avg").floatValue());
				summary.setBegin((float)summaryInHour.getDouble("begin").floatValue());
				summary.setEnd((float)summaryInHour.getDouble("end").floatValue());
				summary.setMax((float)summaryInHour.getDouble("max").floatValue());
				summary.setMin((float)summaryInHour.getDouble("min").floatValue());
				data.put(sKey, summary);
			}
			if(detail!=null) {
				data.put(dKey, detail.getData());
			}
		}
		dpsInDay = new DpsInDay(id,data);
		return dpsInDay;
	}
	


	@Override
	public DpsInDay findLastRecord(Integer metricId) {
		DpsInDay last = super.findOne(Filters.eq("metricId",metricId), new BasicDBObject("timestamp",-1));
		return last;
	}

	@Override
	public boolean exits(Long id) {
		long count = super.getCollection().count(Filters.eq("_id",id));
		if(count>0)
			return true;
		else
			return false;
	}

	

	
}
