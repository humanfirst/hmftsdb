package org.hmf.tsdb.server.dao.impl;

import java.util.ArrayList;
import java.util.Date;
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
import org.hmf.tsdb.server.dao.DpsInHourDAO;
import org.hmf.tsdb.server.dao.MetricDAO;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.entity.Summary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Repository(value="dpsInHour")
public class DpsInHourDAOImpl extends BaseDAOImpl<DpsInHour,Long> implements DpsInHourDAO {
	@Autowired
	MetricDAO metricDAO ;
	
	@PostConstruct
	public void initIndexes() {
	}
	
	@Override
	protected DpsInHour readEntityFromDocument(Document doc) {	
		long id = doc.getLong("_id");
		Date timestamp = doc.getDate("timestamp");
		Integer metricId = doc.getInteger("metricId");
		Metric metric = this.metricDAO.get(metricId);
		Binary dps = (Binary) doc.get("dps");
		byte[] data = null;
		if(dps!=null){
			data = dps.getData();
		}
		Document summaryInDB = (Document) doc.get("summary");
		if(summaryInDB!=null){
			Summary summary = new Summary();
			summary.setAvg(summaryInDB.getDouble("avg").floatValue());
			summary.setBegin((float)summaryInDB.getDouble("begin").floatValue());
			summary.setEnd((float)summaryInDB.getDouble("end").floatValue());
			summary.setMax((float)summaryInDB.getDouble("max").floatValue());
			summary.setMin((float)summaryInDB.getDouble("min").floatValue());
		}
		DpsInHour one = new DpsInHour(id,metric,data,timestamp);
		return one;
	}
	
	
	private BasicDBObject getProjection(DownSample sample) {
		BasicDBObject projection = null;
		if(sample!=null){
			if(sample.getUnit().equals(TimeUnit.HOURS)){
				projection = new BasicDBObject("summary", 1);
				projection.append("timestamp", 1);
			}
		}
		if(projection==null) {
			projection = new BasicDBObject("timestamp", 1);
			projection.append("summary", 1); //在归档的时候，获取dpsPack需要同时获得summary 和 dps detail
			projection.append("dps", 1);
		}
		projection.append("metricId", 1);
		return projection;
	}
	
	/**
	 * 获得整点的时间
	 * @param time
	 * @return
	 */
	private Date getFirstTimeOfHour(Date time) {
		return new Date((time.getTime()/3600000)*3600000);
	}
	
	@Override
	public List<DpsInHour> findDpsPack(Integer metricId, Date start, Date end,DownSample sample) {
		List<DpsInHour> list = null;		
		BasicDBObject projection = this.getProjection(sample);
		
		List<Bson> conditions = new ArrayList<Bson>();
		conditions.add(Filters.eq("metricId",metricId));
		conditions.add(Filters.gte("timestamp", this.getFirstTimeOfHour(start)));
		if(end!=null) {
			conditions.add(Filters.lte("timestamp", end));
		}
		Bson query = Filters.and(conditions);
		
		list = super.findList(query,projection,Sorts.ascending("timestamp"));
		return list;
	}

	@Override
	public void removeDpsBefore(Integer metricId, Date date) {
		super.remove(Filters.and(Filters.eq("metricId",metricId),Filters.lt("timestamp", date)));
	}

	@Override
	public boolean exitsDpsAfterSecond(Integer metricId, long endSecond) {
		long nextSecond = endSecond+1;
		Date time = new Date(nextSecond*1000);
		Bson query = Filters.and(Filters.eq("metricId",metricId),Filters.gte("timestamp",time));
		long count = super.getCollection().count(query);
		return count>0;
	}

	@Override
	public boolean exists(Long id) {
		long count = super.getCollection().count(Filters.eq("_id",id));
		return count>0;
	}

	@Override
	public Date findFirstTimestamp(Integer metricId) {
		BasicDBObject projection = new BasicDBObject();
		projection.append("timestamp", 1);
		MongoCursor<Document> cursor = super.getCollection()
				.find(Filters.eq("metricId",metricId))
				.sort(Sorts.ascending("timestamp"))
				.projection(projection).limit(1).iterator();
		if(cursor.hasNext()) {
			Document doc = cursor.next();
			return doc.getDate("timestamp");
		}else
			return null;
	}

	@Override
	public Map<Metric, List<DpsInHour>> findSerials(Set<Metric> metrics, Date start, Date end, DownSample sample) {
		Map<Metric,List<DpsInHour>> result = new ConcurrentHashMap<Metric,List<DpsInHour>>();
		if(metrics!=null && metrics.size()>0) {
			metrics.parallelStream().forEach(metric->findSerialsAndCollect(metric,start,end,sample,result));			
		}
		return result;
	}
	
	private void findSerialsAndCollect(Metric metric,Date start,Date end,DownSample sample,Map<Metric,List<DpsInHour>> result){
		List<DpsInHour> dpsList = this.findDpsPack(metric.getId(), start, end, sample);
		if(dpsList!=null && dpsList.size()>0) {
			result.put(metric, dpsList);
		}
	}
	
	
	public static void main(String[] args) {
		Date now = new Date();
		DpsInHourDAOImpl dao = new DpsInHourDAOImpl();
		Date zero = dao.getFirstTimeOfHour(now);
		System.out.println(zero);
	}

	

}
