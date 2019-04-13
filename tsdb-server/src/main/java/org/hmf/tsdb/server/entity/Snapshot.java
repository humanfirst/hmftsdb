package org.hmf.tsdb.server.entity;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.exception.TimeBoundException;
import org.hmf.tsdb.server.history.DataPointStream;
import org.hmf.tsdb.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 快照内byte数组的格式如下:[metricId(4个byte),len(2个字节),data(len长度的字节数组)...]
 */
public class Snapshot extends Entity<ObjectId>{
	private static Logger logger = LoggerFactory.getLogger(Snapshot.class);
	final static int maxSize = 1024*1024*12;
	public enum Type{
		full,
		addition,
		part;
	}
	private ObjectId id;
	private Date time;
	private int index;
	private ObjectId parent;
	private Type type;
	private byte[] data;
	private ByteArrayOutputStream output = null;
	private int groupIdx;
	
	
	@Override
	public Document toDocument() {
		Document doc = new Document();
		doc.append("type", type.toString());
		doc.append("data", this.getData());
		doc.append("groupIdx", groupIdx);
		doc.append("time", this.time);
		if(type.equals(Type.part) ) {
			doc.append("index", index);
			doc.append("parent", parent);
		}
		return doc;
	}

	@Override
	public ObjectId getId() {
		return id;
	}

	@Override
	public void setId(ObjectId t) {
		this.id = t;
	}

	

	public int getGroupIdx() {
		return groupIdx;
	}

	public void setGroupIdx(int groupIdx) {
		this.groupIdx = groupIdx;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
	

	public ObjectId getParent() {
		return parent;
	}

	public void setParent(ObjectId parent) {
		this.parent = parent;
	}
	
	public int restSize() {
		if(this.output==null)
			return maxSize;
		return maxSize - this.output.size();
	}
	
	private void writeData(byte[] bytes,int off ,int len){
		if(this.output==null)
			this.output = new ByteArrayOutputStream(maxSize/4);
		if(maxSize>this.output.size())
			this.output.write(bytes, off, len);
		else
			throw new RuntimeException("over the bound of snapshot");
	}
	
	
	
	public void writeFullData(DataPointStream stream) {
		this.writeData(ByteUtils.changeIntTo4Bytes(stream.getMetric().getId()), 0, 4);
		this.writeData(ByteUtils.changeIntTo2Bytes(stream.getPointer()), 0, 2);
		this.writeData(stream.getBytes(),0,stream.getPointer());
		stream.setPreSnapshotAt(stream.getPointer());
		if(stream.getMetric().getId().intValue()==46001) {
			logger.info("full snapshot data:{}",ByteUtils.changeBytesToHexStr(stream.getBytes(),0,stream.getPointer()));
		}
	}
	
	public void writeAdditionData(DataPointStream stream){
		this.writeData(ByteUtils.changeIntTo4Bytes(stream.getMetric().getId()), 0, 4);
		this.writeData(ByteUtils.changeIntTo2Bytes(stream.getPointer()- stream.getPreSnapshotAt()), 0, 2);
		this.writeData(stream.getBytes(), stream.getPreSnapshotAt(), stream.getPointer()- stream.getPreSnapshotAt());		
		stream.setPreSnapshotAt(stream.getPointer());
	}
	
	

	public byte[] getData() {
		if(data==null) {
			if(output!=null)
				data = output.toByteArray();
		}
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	private int offset = 0;
	public Fragment nextFramgment(){
		if(data.length>offset) {
			Fragment f = new Fragment();
			int metricId = ByteUtils.changeBytesToInt(this.data[offset++],data[offset++],data[offset++],data[offset++]);
			int len = ByteUtils.changeBytesToInt(data[offset++],data[offset++]);
			byte[] bytes = new byte[len];
			System.arraycopy(data, offset, bytes, 0, len);
			offset+=len;
			f.metricId = metricId;
			f.data = bytes;
			return f;
		}else
			return null;
	}

	@Override
	public String toString() {
		return "Snapshot [id=" + id + ", time=" + time + ", index=" + index + ", parent=" + parent + ", type=" + type
				+ ", groupIdx=" + groupIdx + "]";
	}

	
	public static void main(String[] args) {
		Metric m = new Metric();
		m.setId( 1);
		m.setName("温度");
		Snapshot ss = new Snapshot();
		DataPointStream stream = new DataPointStream(m,3600);
		long now = System.currentTimeMillis();
		for(int i = 0;i<10;i++) {
			DataPoint dp = new DataPoint();
			dp.setMetric("温度");
			dp.setTime(now++);
			dp.setValue(i);
			
			try {
				stream.write(dp);
			} catch (TimeBoundException e) {
				e.printStackTrace();
			} 
		
		}
		System.out.println(stream.getPointer());
		
		ss.writeFullData(stream);
		System.out.println(stream.getPreSnapshotAt());
		
		DataPoint dp = new DataPoint();
		dp.setMetric("温度");
		dp.setTime(now++);
		dp.setValue(11);
		
		try {
			stream.write(dp);
		} catch (TimeBoundException e) {
			e.printStackTrace();
		} 
		ss.writeAdditionData(stream);
		System.out.println(stream.getPointer());
		System.out.println(stream.getPreSnapshotAt());
	}
	
}
