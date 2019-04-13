package org.hmf.tsdb.server.history;

import java.util.Arrays;
import java.util.Date;

import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.entity.Fragment;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.exception.TimeBoundException;
import org.hmf.tsdb.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 某个测点的时序数据缓存器,缓存格式： [采样时间(2 bytes)，采样值(4bytes)......]
 */
public class DataPointStream {	
	private Date now = null;
	private static Logger logger = LoggerFactory.getLogger(DataPointStream.class);
	private static ObjectMapper mapper = new ObjectMapper();
	public enum Status{
		working,
		finished;
	}
	/**
	 * 测点
	 */
	private Metric metric;
	
	/**
	 * 主缓存
	 */
	private byte[] bytes;
	
	/**
	 * 当前位置
	 */
	private int pointer=0;
	
	
	private Status status;
	
	/**
	 * 上次快照的位置
	 */
	private int preSnapshotAt  = 0;
	
	private long lastSampleTime = 0;
	
	public int getPreSnapshotAt() {
		return preSnapshotAt;
	}

	public void setPreSnapshotAt(int preSnapshotAt) {
		this.preSnapshotAt = preSnapshotAt;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
	
	public int getPointer() {
		return pointer;
	}

	public void setPointer(int pointer) {
		this.pointer = pointer;
	}

	public Metric getMetric() {
		return metric;
	}

	public void setMetric(Metric metric) {
		this.metric = metric;
	}

	public byte[] getBytes() {
		return bytes;
	}
	
	public byte[] getData() {
		byte[] result = new byte[pointer-4];
		System.arraycopy(bytes, 4, result, 0, pointer-4);
		return result;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

	

	public DataPointStream(Metric metric,int size) {
		this.metric = metric;
		this.bytes= new byte[size*6];
		this.status = Status.working;
	}
	
	public void reset() {
		pointer = 0;
		this.preSnapshotAt = 0;
		Arrays.fill(bytes,(byte)0);
		this.status = Status.working;
	}
	
	public void writeHour(long hour) {
		byte[] hourBytes =  ByteUtils.changeIntTo4Bytes((int)hour);
		bytes[pointer++] = hourBytes[0];
		bytes[pointer++] = hourBytes[1];
		bytes[pointer++] = hourBytes[2];
		bytes[pointer++] = hourBytes[3];
	}
	public long readHour() {
		long hour = (long)ByteUtils.changeBytesToInt(bytes[0],bytes[1],bytes[2],bytes[3]);
		return hour;
	}
	public long getCurrentHour() {
		return this.readHour();
	}
	
	public void write(Fragment fragment) {	
		try {
		System.arraycopy(fragment.data, 0, this.bytes, this.pointer, fragment.data.length);
		}catch(Exception e) {
			e.printStackTrace();
		}
		this.pointer = this.pointer+fragment.data.length;
		this.preSnapshotAt = this.pointer;
		if((this.pointer-4)%6>0) {
			System.out.println();
		}
	}
	public void write(DataPoint dp) throws TimeBoundException {	
		if(dp.getTime()<=this.lastSampleTime) {
			return ;
		}
		long sampleHour = dp.getTime()/(3600*1000);
		if(pointer==0) {
			this.writeHour(sampleHour);
		}else {
			long currentHour = this.readHour();
			if(pointer>=this.bytes.length) {
				logger.error("DataPointStream已达存储上限!当前采样时间:{},前一个数据的采样时间:{}",new Date(dp.getTime()),new  Date(this.lastSampleTime)) ;
			}
			if(sampleHour>currentHour) {
				throw new TimeBoundException("新的DataPoint属于下一个小时");
			}else if(sampleHour<currentHour) {
				//throw new RuntimeException("新的DataPoint属于上一个小时");
			}
		}
		
		long second = (dp.getTime() % 3600000)/1000;
		
		
		//write sample time
		byte[] secondBytes = ByteUtils.changeIntTo2Bytes((int)second);		
		bytes[pointer++] = secondBytes[0];
		bytes[pointer++] = secondBytes[1];
		
		
		int value = changeNumberToInt(dp.getValue());
		byte[] valueBytes = ByteUtils.changeIntTo4Bytes(value);
		bytes[pointer++] = valueBytes[0];
		bytes[pointer++] = valueBytes[1];
		bytes[pointer++] = valueBytes[2];
		bytes[pointer++] = valueBytes[3];
		
	}
	
	private int changeNumberToInt(Number number) {
		if(number instanceof Float) {
			Float f = (Float)number;
			return Float.floatToIntBits(f);
		}else if(number instanceof Integer) {
			return (Integer)number;
		}else 
			throw new IllegalArgumentException("can change number to int! current number is a "+number.getClass().getSimpleName());
	}
	
}
