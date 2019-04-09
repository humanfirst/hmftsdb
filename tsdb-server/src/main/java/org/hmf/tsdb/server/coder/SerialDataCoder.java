package org.hmf.tsdb.server.coder;
import java.util.ArrayList;
import java.util.List;


/**
 * 对 int集合或者float集合进行压缩的编解码器
 */
public class SerialDataCoder {
	public BitOutputStream encodeFloats(List<Float> list) {
		List<Integer> intList = changeFloatListToIntList(list);
		return encodeInts(intList);
	}
	
	private int getDeltaOfDeltaFromStream(int valueBitsLen,BitInputStream stream) {
		int symbol = stream.readBit();
		symbol = symbol==1?-1:1;
		int value = stream.readBit(valueBitsLen);
		return symbol * value;
	}
	
	public List<Float> decodeToFloats(BitInputStream stream){
		List<Integer> intList = this.decode(stream);
		return changeIntListToFloatList(intList);
	}
	public List<Integer> decode(BitInputStream stream){
		int firstValue = stream.readBit(32);
		int firstDelta = stream.readBit(32);
		int deltaOfDeltaCount = stream.readBit(16);
		List<Integer> deltas = new ArrayList<Integer>();
		deltas.add(firstDelta);
		int bit = 0;
		int preDelta = firstDelta;
		int deltaOfDelta=0,delta=0;
		while((bit=stream.readBit())>-1) {
			if(bit==0) {
				deltaOfDelta = 0;
				deltas.add(preDelta);
			}else {
				int oneByte = bit;
				while((bit=stream.readBit())==1) {
					oneByte = oneByte<<1 | bit;
				}
				oneByte = oneByte<<1;
				switch(oneByte) {
				case 2:
					deltaOfDelta = this.getDeltaOfDeltaFromStream(5, stream);
					break;
				case 6:
					deltaOfDelta = this.getDeltaOfDeltaFromStream(8, stream);
					break;
				case 14:
					deltaOfDelta = this.getDeltaOfDeltaFromStream(12, stream);
					break;
				case 30:
					deltaOfDelta = this.getDeltaOfDeltaFromStream(19, stream);
					break;
				case 62:
					deltaOfDelta = this.getDeltaOfDeltaFromStream(32, stream);
					break;
				}
				
				delta = preDelta + deltaOfDelta;
				deltas.add(delta);
				preDelta= delta;
			}
			if(deltas.size()>=deltaOfDeltaCount+1) {
				break;
			}			
		}
		List<Integer> values = new ArrayList<Integer>();
		values.add(firstValue);
		int currentValue = 0,preValue = firstValue;
		for(Integer one:deltas) {
			currentValue = preValue+one;
			values.add(currentValue);
			preValue = currentValue;
		}
		return values;
	}
	
	public BitOutputStream encodeInts(List<Integer> list) {
		List<Integer> deltas1 = new ArrayList<Integer>();
		
		for(int i=1;i<list.size();i++) {
			long current = 0x00000000FFFFFFFFL & list.get(i);
			long pre = 0x00000000FFFFFFFFL & list.get(i-1);
			Integer delta =(int)( current - pre);
			deltas1.add(delta);
		}
		
		List<Integer> deltas2 = new ArrayList<Integer>();
		for(int i=1;i<deltas1.size();i++) {
			Integer delta = deltas1.get(i) - deltas1.get(i-1);
			deltas2.add(delta);
		}
		
		BitOutputStream stream = new  BitOutputStream(1024);
		stream.write32Bits(list.get(0));
		stream.write32Bits(deltas1.get(0));
		stream.write16Bits(deltas2.size());
		
		for(Integer delta:deltas2) {
			int deltaValue = delta.intValue();
			if(0==deltaValue) {
				stream.writeOneBit(0);
			}else if(deltaValue>=-31 && deltaValue<=31){
				stream.writeBits(1,0,deltaValue>=0?0:1);
				int v = Math.abs(deltaValue);
				stream.writeBitsFromByteRight(v, 5);
			}else if(deltaValue>=-255 && deltaValue<=255) {				
				stream.writeBits(1,1,0,deltaValue>=0?0:1);
				int v = Math.abs(deltaValue);
				stream.writeBitsFromIntRight(v, 8);
			}else if(deltaValue>=-4095 && deltaValue<=4095) {
				stream.writeBits(1,1,1,0,deltaValue>=0?0:1);
				int v = Math.abs(deltaValue);
				stream.writeBitsFromIntRight(v, 12);
			}else if(deltaValue>=-524287 && deltaValue<=524287) {
				stream.writeBits(1,1,1,1,0,deltaValue>=0?0:1);
				int v = Math.abs(deltaValue);
				stream.writeBitsFromIntRight(v, 19);
			}else{
				stream.writeBits(1,1,1,1,1,0,deltaValue>=0?0:1);
				int v = Math.abs(deltaValue);
				stream.writeBitsFromIntRight(v, 32);
			}
		}
		return stream;
	}
	
	public static String formatIntListToHexString(List<Integer> list) {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(Integer one:list) {
			String s = Integer.toHexString(one);
			s = insertZero(s);
			sb.append(s).append(" ");
			count++;
			if(count%10==9)
				sb.append("\r\n");
		}
		return sb.toString();
	}
	
	public static List<Integer> changeFloatListToIntList(List<Float> list){
		List<Integer> result = new ArrayList<Integer>();
		for(Float f:list) {
			result.add(Float.floatToIntBits(f));
		}
		return result;
	}
	
	public static List<Float> changeIntListToFloatList(List<Integer> list){
		List<Float> result = new ArrayList<Float>();
		for(Integer f:list) {
			result.add(Float.intBitsToFloat(f));
		}
		return result;
	}
	
	public static String formatFloatListToHexString(List<Float> list) {		
		List<Integer> intList = changeFloatListToIntList(list);		
		return formatIntListToHexString(intList);
	}
	
	public static String formatFloatListToDecString(List<Float> list) {		
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(Float one:list) {
			sb.append(one).append(" ");
			count++;
			if(count%10==9)
				sb.append("\r\n");
		}
		return sb.toString();
	}
	
	public static String insertZero(String hexStr) {
		if(hexStr.length()==8)
			return hexStr;
		
		StringBuilder sb = new StringBuilder();
		sb.append(hexStr);
		while(sb.length()<8) {
			sb.insert(0, "0");
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception {
		SerialDataCoder coder = new  SerialDataCoder();
		
		List<Integer> seconds = new ArrayList<Integer>();
		List<Float> serial = new  ArrayList<Float>();
		for(int i=0;i<3600;i+=1) {
			float v = genValue(i);
			serial.add(v);
			seconds.add(i);
		}
		System.out.println("压缩前最后一个整数："+seconds.get(seconds.size()-1));
		BitOutputStream stream = coder.encodeInts(seconds);
		byte[] encoded = stream.getBytes();
		BitInputStream inStream = new BitInputStream(encoded);
		List<Integer> decodedIntList = coder.decode(inStream);
		System.out.println("解压后最后一个整数："+decodedIntList.get(decodedIntList.size()-1));
		
		System.out.println("原始字节长度："+seconds.size());
		System.out.println("压缩后bit长度："+stream.size()/8);
		
		
		
		System.out.println("压缩前最后一个浮点数："+serial.get(serial.size()-1));
		stream = coder.encodeFloats(serial);
		encoded = stream.getBytes();
		inStream = new BitInputStream(encoded);
		List<Float> decoded = coder.decodeToFloats(inStream);
		System.out.println("解压后最后一个浮点数："+decoded.get(decoded.size()-1));
		
		System.out.println("原始字节长度："+serial.size()*4);
		System.out.println("压缩后字节长度："+stream.size() /8);
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


