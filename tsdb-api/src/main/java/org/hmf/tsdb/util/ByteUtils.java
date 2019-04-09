package org.hmf.tsdb.util;

import java.util.Date;

public class ByteUtils {
	public static String changeBytesToHexStr(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		for(byte b:bytes) {
			String hex = Integer.toHexString(b & 0xFF);
			hex = hex.length()==1?"0"+hex:hex;
			sb.append(hex).append(" ");
		}
		if(sb.length()>0) {
			sb.deleteCharAt(sb.length()-1);
		}
		return sb.toString();
	}
	
	public static String changeBytesToDecStr(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for(byte b:bytes) {
			sb.append( b & 0xFF).append(" ");
			i++;
			if(i%100==0) {
				sb.append("\r\n");
			}
		}
		if(sb.length()>0) {
			sb.deleteCharAt(sb.length()-1);
		}
		return sb.toString();
	}
	
	public static String changeBytesToHexStr(byte[] bytes,int from ,int len) {
		StringBuffer sb = new StringBuffer();		
		int to = from+len;
		for(int i=from;i<to;i++) {
			byte b = bytes[i];
			String hex = Integer.toHexString(b & 0xFF);
			hex = hex.length()==1?"0"+hex:hex;
			sb.append( hex).append(" ");
		}
		if(sb.length()>0) {
			sb.deleteCharAt(sb.length()-1);
		}
		return sb.toString();
	}
	
	
	public static byte[] changeIntTo2Bytes(int i) {
		byte[] bytes = new byte[2]; 
		int h = (i & 0x0000FF00) >> 8;
		int l = i & 0x00FF;
		bytes[0] = (byte)h;
		bytes[1] = (byte)l;
		return bytes;
	}
	
	public static byte[] changeIntTo4Bytes(int i) {
		byte[] bytes = new byte[4]; 
		bytes[0] = (byte)((i & 0xFF000000)>>24);
		bytes[1] = (byte)((i & 0xFF0000)>>16);
		bytes[2] = (byte)((i & 0xFF00)>>8);
		bytes[3] = (byte)(i & 0x00FF);
		return bytes;
	}
	
	public static byte[] changeLongTo8Bytes(long l) {
		byte[] bytes = new byte[8]; 
		bytes[0] = (byte)((l & 0xFF00000000000000L)>>56);
		bytes[1] = (byte)((l & 0xFF000000000000L)>>48);
		bytes[2] = (byte)((l & 0xFF0000000000L)>>40);
		bytes[3] = (byte)((l & 0xFF00000000L)>>32);
		bytes[4] = (byte)((l & 0xFF000000)>>24);
		bytes[5] = (byte)((l & 0xFF0000)>>16);
		bytes[6] = (byte)((l & 0xFF00)>>8);
		bytes[7] = (byte)(l & 0x00FF);
		return bytes;
	}
	
	public static int changeBytesToInt(byte... b) {
		if(b.length==4)
			return b[3] & 0xFF |  
				(b[2] & 0xFF) << 8 |  
				(b[1] & 0xFF) << 16 |  
				b[0]  << 24;  
		else if (b.length==2)
			return (b[1] & 0xFF) | b[0] << 8;  
		else 
			throw new IllegalArgumentException("转换为整形数的字节数组长度必须等于2或者4");
	}
	
	public static long changeBytesToLong(byte... b) {
		if(b.length!=8)
			throw new IllegalArgumentException("转换为长整数的字节数组长度必须等于8");
		
		return 
			b[7] & 0xFFL |  
			(b[6] & 0xFFL) << 8 |  
			(b[5] & 0xFFL) << 16 |  
			(b[4] & 0xFFL) << 24 | 
			(b[3] & 0xFFL) << 32 | 
			(b[2] & 0xFFL) << 40 | 
			(b[1] & 0xFFL) << 48 | 
			(b[0] & 0xFFL) << 56 ;
	}
	
	public static void main(String[] args) {
		byte[] bytes = ByteUtils.changeIntTo2Bytes(-532);
		System.out.println(ByteUtils.changeBytesToDecStr(bytes));
		
		int i = ByteUtils.changeBytesToInt(bytes);
		System.out.println(i);
		
		Date t = new Date();
		System.out.println(t.getTime());
		
		
		float a = 1.1f,b= -1.2f;
		bytes =  ByteUtils.changeIntTo4Bytes(Float.floatToIntBits(a));
		System.out.println(ByteUtils.changeBytesToHexStr(bytes));
		bytes =  ByteUtils.changeIntTo4Bytes(Float.floatToIntBits(b));
		System.out.println(ByteUtils.changeBytesToHexStr(bytes));
	}

}
