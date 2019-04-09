package org.hmf.tsdb.server.coder;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.util.ByteUtils;

/**
 * 对“按小时归档的时序数据”进行压缩的编解码器
 */
public class DpsInHourCoder extends SerialDataCoder{
	
	private List<Integer> changeNumberListToIntList(List<Number> inList){
		List<Integer> outList = new ArrayList<Integer>(inList.size());
		for(Number n :inList) {
			outList.add((Integer)n);
		}
		return outList;
	}
	
	private List<Float> changeNumberListToFloatList(List<Number> inList){
		List<Float> outList = new ArrayList<Float>(inList.size());
		for(Number n :inList) {
			outList.add((Float)n);
		}
		return outList;
	}
	
	public byte[] encode(Metric.ValueType type,Map<Integer,Number> dps) {
		List<Integer> secondSerial =  new ArrayList<Integer>();
		List<Number> valueSerial = new ArrayList<Number>();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		
		
		Set<Entry<Integer, Number>> set = dps.entrySet();
		for(Entry<Integer,Number> one:set) {
			secondSerial.add(one.getKey());
			valueSerial.add(one.getValue());
		}
		
		BitOutputStream stream1 = super.encodeInts(secondSerial);
		BitOutputStream stream2 = null;
		if(type.equals(Metric.ValueType.intValue)){
			stream2 = super.encodeInts(this.changeNumberListToIntList(valueSerial));
		}else {
			stream2 = super.encodeFloats(this.changeNumberListToFloatList(valueSerial));
		}
		byte[] data1  = stream1.getData();
		try {
			byteStream.write(ByteUtils.changeIntTo2Bytes(data1.length));
			byteStream.write(data1);
			byteStream.write(stream2.getData());
		}catch(Exception e) {
			e.printStackTrace();
		}
		return byteStream.toByteArray();
	}
	
	public LinkedHashMap<Integer,Number> decode(Metric.ValueType type,byte[] data) {
		int len = ByteUtils.changeBytesToInt(data[0],data[1]);
		byte[] secondData  = new byte[len];
		System.arraycopy(data, 2, secondData, 0, len);
		byte[] valueData = new byte[data.length-2-len];
		System.arraycopy(data, 2+len, valueData, 0, valueData.length);
		
		BitInputStream stream = null;
		stream = new BitInputStream(secondData);
		List<Integer> seconds = super.decode(stream);
		
		stream = new BitInputStream(valueData);
		List<Number> values = new ArrayList<Number>();
		if(type.equals(Metric.ValueType.floatValue)) {
			values.addAll( super.decodeToFloats(stream));
		}else
			values.addAll(super.decode(stream));
		
		LinkedHashMap<Integer,Number> result = new LinkedHashMap<Integer,Number>();
		Iterator<Number> iter = values.iterator();
		for(Integer second:seconds) {
			result.put(second, iter.next());
		}
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		
	}
	
	static float r = 2.0f;
	static float midV = 1000.0f;
	public static float genValue(int i) {		
		float f =  0.0f;
		double angle = ((double)i);
		f = midV + r * (float)Math.sin(Math.toRadians(angle));
		return f;
	}
	
	
}


