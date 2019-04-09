package org.hmf.tsdb.server.entity;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;
import org.hmf.tsdb.server.coder.DpsInHourCoder;
import org.hmf.tsdb.server.history.DataPointStream;
import org.hmf.tsdb.util.ByteUtils;

import com.mongodb.BasicDBObject;


/**
 * 按小时归档的时序数据
 */
public class DpsInHour extends Entity<Long>{
	private static DpsInHourCoder coder = new DpsInHourCoder();
	private Long id;
	private Metric metric ;
	private long hour = -1;
	private Summary summary;
	private Date timestamp;
	private byte[] data = null;
	LinkedHashMap<Integer,Number> dps = null;
	@Override
	public int hashCode() {
		return this.id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj==null)
			return false;
		if(obj instanceof DpsInHour)
			return this.id.equals(((DpsInHour)obj).getId());
		else
			return false;
	}
	
	private LinkedHashMap<Integer,Number> getDpsFromEncodedBytes(byte[] data,Metric.ValueType type){
		LinkedHashMap<Integer,Number> dps = null;
		dps = coder.decode(type, data);
		return dps;		
	}
	
	private LinkedHashMap<Integer,Number> getDpsFromOriginBytes(byte[] data,Metric.ValueType type){
		
		LinkedHashMap<Integer,Number> dps = null;		
		dps = new LinkedHashMap<Integer,Number>();
		for(int i=0;i<data.length;i+=6) {
			int second = ByteUtils.changeBytesToInt(data[i],data[i+1]);
			int value = ByteUtils.changeBytesToInt(data[i+2],data[i+3],data[i+4],data[i+5]);
			Number number = null;
			if(type.equals(Metric.ValueType.floatValue)) {
				number = Float.intBitsToFloat(value);
			}else if(type.equals(Metric.ValueType.intValue)){
				number = value;
			}else {
				throw new RuntimeException("can not support this ValueType:"+type.toString());
			}
			dps.put(second, number);
			
		}
		return dps;
	}
	
	public DpsInHour(DataPointStream stream) {
		this.metric = stream.getMetric();
		this.hour =(long)stream.getCurrentHour();
		this.id = (this.hour<<32) + stream.getMetric().getId();
		
		this.timestamp = new Date(this.hour*3600000);
		this.data = stream.getData();
		this.dps  =  this.getDpsFromOriginBytes(data, stream.getMetric().getType());
	}
	
	public DpsInHour(Metric metric,long hour,byte[] data){
		this.metric = metric;
		this.id = (hour<<32) + metric.getId();
		this.hour = hour;
		this.timestamp = new Date(hour*3600000);
		this.data = data;
		this.dps  =  this.getDpsFromEncodedBytes(data, metric.getType());
	}
	
	public DpsInHour(Metric metric,long hour,Summary summary){
		this.metric = metric;
		this.id = (hour<<32) + metric.getId();
		this.hour = hour;
		this.timestamp = new Date(hour*3600000);
		this.summary = summary;
	}
	
	public DpsInHour(long id,Metric metric,byte[] data,Date timestamp){
		this.metric = metric;
		this.id = id;
		this.hour = timestamp.getTime()/3600000;
		this.timestamp = timestamp;
		this.data = data;
		this.dps  =  this.getDpsFromEncodedBytes(data, metric.getType());
	}
	
	
	@Override
	public Long getId() {
		return id;
	}
	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	
	
	public Metric getMetric() {
		return metric;
	}

	public void setMetric(Metric metric) {
		this.metric = metric;
	}

	public long getHour() {
		return hour;
	}

	public void setHour(long hour) {
		this.hour = hour;
	}

	public void setId(Long id) {
		this.id = id;
	}
	
	public Summary getSummary() { 
		return summary;
	}

	public void setSummary(Summary summary) {
		this.summary = summary;
	}
	
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	@Override
	public Document toDocument() throws Exception {
		Document document = new Document();
		document.append("_id", id);
		document.append("timestamp", this.timestamp);
		document.append("metricId", this.metric.getId());
		if(this.summary==null) {
			this.doSummary();
			BasicDBObject summary = new BasicDBObject();			
			summary.append("begin", this.summary.getBegin());
			summary.append("end", this.summary.getEnd());
			summary.append("max", this.summary.getMax());
			summary.append("min", this.summary.getMin());
			summary.append("avg", this.summary.getAvg());
			document.append("summary", summary);
		}
		byte[] encoded = coder.encode(metric.getType(), this.dps);
		document.append("dps", encoded);
		//document.append("dps", this.data); 非压缩情况下直接用data
		return document;
	}
	
	public DpsInHour doSummary(){
		if(data==null || data.length==0)
			return null;
		Number firstValue = dps.entrySet().iterator().next().getValue();
		if(firstValue instanceof Float ==false)
			return this;
		Float avg =null;
		Float max =null;
		Float min =null;
		Float begin =null;
		Float end = null;
		Float sum = 0F;
		int count =0;
		Set<Entry<Integer,Number>> set = dps.entrySet();
		for(Entry<Integer,Number> entry:set){
			Float value = (Float)entry.getValue();
			sum = sum+value;
			if(count==0){
				max = value;
				min = value;
				begin = value;
				end = value;
			}else{
				if(value>max)
					max = value;
				if(value<min)
					min = value;
				end = value;
			}
			count ++;
		}
		avg = sum/count;
		summary = new Summary();
		summary.setAvg(avg);
		summary.setBegin(begin);
		summary.setEnd(end);
		summary.setMax(max);
		summary.setMin(min);
		return this;
	}
	
	public void subDps(Date start,Date end) {		
		long startL,endL;
		startL = start.getTime();
		endL = end!=null?end.getTime():Long.MAX_VALUE;
		if(this.dps!=null && this.dps.size()>0){
			Iterator<Entry<Integer,Number>> iter = this.dps.entrySet().iterator();
			while(iter.hasNext()){
				Entry<Integer,Number> entry = iter.next();
				Integer secondInHour = entry.getKey();
				long time =( this.hour*3600+secondInHour)*1000;
				if(time<startL || time>endL)
					iter.remove();
			}
		}
	}

	public LinkedHashMap<Integer, Number> getDps() {
		return dps;
	}

	public void setDps(LinkedHashMap<Integer, Number> dps) {
		this.dps = dps;
	}
}
