package org.hmf.tsdb.server.dao.impl;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.hmf.tsdb.server.dao.MetricDAO;
import org.hmf.tsdb.server.entity.Metric;
import org.springframework.stereotype.Repository;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Repository(value="metric")
public class MetricDAOImpl extends BaseDAOImpl<Metric,Integer> implements MetricDAO{
	public MetricDAOImpl(){
		super();
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	protected Metric readEntityFromDocument(Document doc) {
		Metric metric = new Metric();
		metric.setId(doc.getInteger("_id"));
		metric.setName(doc.getString("name"));
		metric.setType(Metric.ValueType.values()[doc.getInteger("type")]);
		TreeMap<String,String> tags = new TreeMap<String,String>();
		tags.putAll((Map<String, String>)(doc.get("tags")));
		metric.setTags(tags);
		return metric;
	}

	public  Metric createMetric(Metric metric)throws Exception {
		int newId = super.next("metricId");
		metric.setId(newId);
		if(metric.getType()==null)
			throw new RuntimeException("测点的值类型不能为空，metric="+metric.toString());
		super.create(metric);
		return metric;
	}
	
	public Metric createMetric(Metric metric,Number value) throws Exception{		
		if(value instanceof Integer){
			metric.setType(Metric.ValueType.intValue);
		}else if(value instanceof Float){
			metric.setType(Metric.ValueType.floatValue);
		}else if(value instanceof Double){
			metric.setType(Metric.ValueType.floatValue);
		}else
			throw new RuntimeException("can not support value type,value is instanceof "
					+value.getClass().getSimpleName());
		this.createMetric(metric);
		return metric;
	}
	
	public List<Metric> findMetrics(String name, Map<String, String> tags) {
		List<Bson> filters = new ArrayList<Bson>();
		filters.add(Filters.eq("name",name));
		
		if(tags!=null && tags.size()>0) {
			Iterator<Entry<String,String>> iter = tags.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<String,String> e = iter.next();
				filters.add(Filters.eq("tags."+e.getKey(), e.getValue()));
			}			
		}
		return this.findList(Filters.and(filters));
	}

	public void delete(String name, Map<String, String> tags) {
		List<Bson> filters = new ArrayList<Bson>();
		filters.add(Filters.eq("name",name));
		
		if(tags!=null && tags.size()>0) {
			Iterator<Entry<String,String>> iter = tags.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<String,String> e = iter.next();
				filters.add(Filters.eq("tags."+e.getKey(), e.getValue()));
			}			
		}
		super.remove(filters);
	}

	
	
}
