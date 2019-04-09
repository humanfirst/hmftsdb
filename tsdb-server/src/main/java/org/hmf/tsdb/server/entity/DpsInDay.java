package org.hmf.tsdb.server.entity;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import com.mongodb.BasicDBObject;

/**
 * 按天归档的时需数据
 */
public class DpsInDay extends Entity<Long>{
	private Long id;
	private int day;
	private Date timestamp;
	private Map<String,Object> data = null;
	private int metricId = -1;
	
	/**
	 * @param date所在日期0点的时间
	 * @param metricId
	 * @param data
	 */
	public DpsInDay(Date date,Integer metricId,Map<String,Object> data){
		this.day =(int) (date.getTime()/3600000);
		this.id = ((long)day<<32) + metricId;
		this.data =  data;
		this.metricId = metricId;
		this.timestamp = date;
	}
	
	public DpsInDay(long id,Map<String,Object> data){
		this.id = id;
		this.metricId = (int)(id & 0x00000000ffffffffL);
		this.day = (int)(id>>32);
		this.data = data;
		this.timestamp = new Date(day * 3600 * 1000);
	}
	
	
	public DpsInDay(){
	}
	
	public Long getId() {
		return id;
	}
	
	public void setId(Long id) {
		this.id = id;
	}
	
	public long getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public Map<String, Object> getData() {
		return data;
	}
	
	public void setData(Map<String, Object> data) {
		this.data = data;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public Document toDocument() {
		Document document = new Document();
		document.append("_id", id);
		document.append("timestamp", this.timestamp);
		document.append("metricId", this.metricId);
		Iterator<Entry<String,Object>> iter = data.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,Object> entry = iter.next();
			Object value = entry.getValue();
			String key = entry.getKey();
			if(value instanceof Summary) {
				Summary summary = (Summary)value;
				BasicDBObject subDoc = new BasicDBObject();
				subDoc.append("begin", summary.getBegin());
				subDoc.append("end", summary.getEnd());
				subDoc.append("max", summary.getMax());
				subDoc.append("min", summary.getMin());
				subDoc.append("avg", summary.getAvg());
				document.append(entry.getKey(), subDoc);
			}else {
				document.append(key, value);
			}
		}
		return document;
	}
	
	public void sub(long startHour,long endHour) {		
		
	}
	
}
