package org.hmf.tsdb.server.coder;

/**
 * for write bit to byte array
 */
public  class BitOutputStream{
	byte[] bytes = null;
	int pointer = 0;
	
	public BitOutputStream(int size) {
		bytes = new byte[size];
	}
	
	public byte[] getData() {
		int byteSize = this.pointer/8;
		int rest = this.pointer%8;
		if(rest>0)
			byteSize++;
		
		byte[] data = new byte[byteSize];
		System.arraycopy(bytes, 0, data, 0, byteSize);
		return data;
	}
	
	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}
	
	/**
	 * 扩容
	 */
	private void increaseArray() {
		int oldLen = bytes.length;
		int newLen = (int)(oldLen * 1.5);
		byte[] newBytes = new byte[newLen];
		System.arraycopy(bytes, 0, newBytes, 0, pointer);
		this.bytes = newBytes;
	}
	
	/**
	 * 写入32个bit
	 * @param v 以int形式存在的32个bit
	 */
	public void write32Bits(int v) {
		if(pointer+32>bytes.length) {
			this.increaseArray();
		}
		this.writeBitsFromIntRight(v, 32);
	}
	
	/**
	 * 写入16个bit
	 * @param v 以int形式存在的16个bit
	 */
	public void write16Bits(int v) {
		if(pointer+16>bytes.length) {
			this.increaseArray();
		}
		this.writeBitsFromIntRight(v, 16);
	}
	
	/**
	 * 写入8个bit
	 * @param b 以byte形式存在8个bit
	 */
	public void write08Bits(byte b){	
		if(this.pointer+8>this.bytes.length) {
			this.increaseArray();
		}
		int byteValue = 0xFF & b;
		int bitLenInLastByte = pointer % 8;
		int emptyLen = 8 -bitLenInLastByte;
		if(bitLenInLastByte==0) {
			this.bytes[pointer/8] = (byte)b;
		}else {
			int leftValue = 0xFF & (byteValue>>bitLenInLastByte);
			int rightValue = 0xFF & (byteValue<<emptyLen);
			int leftOld = 0xFF & this.bytes[pointer/8];
			int leftNew = leftOld | leftValue;
			this.bytes[pointer/8] = (byte)leftNew;
			this.bytes[pointer/8+1] = (byte)rightValue;
		}
		pointer = pointer+8;
	}
	
	/**
	 * 写入1个bit
	 * @param bit 以int形式存在的bit，只能取值0或1
	 * @throws Exception
	 */
	public void writeOneBit(int bit){
		if(bit>1)
			throw new RuntimeException("bit的值只能等于0或者1");
		if(this.pointer+1>this.bytes.length) {
			this.increaseArray();
		}
		if(pointer%8==0) {
			int v = bit<<7;
			bytes[pointer/8] = (byte)v;pointer++;
		}else {
			int v = bit<< (7-pointer%8);
			int one = 0xFF & bytes[pointer/8];
			v = v | one;
			bytes[pointer/8] =(byte) v;pointer++;				
		}
	}
	
	public void writeBitsFromIntRight(int value,int lenFromRight) {
		if(this.pointer+lenFromRight>this.bytes.length) {
			this.increaseArray();
		}
		//需要的bit中包含了几个完整的byte
		int byteCount = lenFromRight/8;
		//除了n个完整byte之外的bit数量
		int restBitCount = lenFromRight % 8;
		if(byteCount>0) {
			for(int i=0;i<byteCount;i++) {
				int oneByte = 0xFF &(value>>((byteCount-1-i)*8+restBitCount));
				this.write08Bits((byte)oneByte);
			}
		}
		
		if(restBitCount>0) {
			long v = (long)value;
			int oneByte = 0xFF &(int) ((v<<(32-restBitCount)) >> (32-restBitCount));
			this.writeBitsFromByteRight(oneByte, lenFromRight-8*byteCount);
		}
	}
	
	/**
	 * 从int中截取一段bit
	 * @param origin
	 * @param from 从左向右数第几位
	 * @param end 从左向右数第几位
	 * @return
	 */
	public int subBits(int origin,int from ,int end) {
		int sub = 0;
		for (int i=from ;i<end;i++){
			sub = sub | (1<<(31-i));
		}
		sub = sub & origin;
		sub = sub >> (32-end);
		return sub;
	}
	
	/**
	 * 向stream写入byte右边部分的bit
	 * @param byteValue 
	 * @param len 右边部分的长度
	 */
	public void writeBitsFromByteRight(int byteValue,int len) {
		if(this.pointer+len>this.bytes.length) {
			this.increaseArray();
		}
		int bitLenInLastByte = pointer % 8;
		int emptyLen = 8 -bitLenInLastByte;
		if(bitLenInLastByte==0) {
			this.bytes[pointer/8] = (byte)(byteValue<<(8-len));
		}else {
			int leftOld = 0xFF & this.bytes[pointer/8];
			if(len<=emptyLen) {
				int value = (byteValue<<(8-len))>>bitLenInLastByte;
				int newValue = value | leftOld;
				this.bytes[pointer/8] = (byte)newValue;
			}else {
				int leftValue = 0xFF & ((byteValue<<(8-len))>>bitLenInLastByte);
				int rightValue = 0xFF & (byteValue<<(8-len+emptyLen));
				int leftNew =  leftOld | leftValue;
				this.bytes[pointer/8] = (byte)leftNew;
				this.bytes[pointer/8+1] = (byte)rightValue;
			}
		}
		pointer = pointer+len;
	};
	
	public void writeBits(int... bits) {
		if(bits.length>=32) {
			throw new RuntimeException("按位写入值时，不可一次写入32位！请改用write32Bit方法写入。");
		}else {
			if(this.pointer+bits.length>this.bytes.length) {
				this.increaseArray();
			}
			int v = 0;
			for(int i=0;i<bits.length;i++) {
				v = (v<<1) | bits[i];
			}
			this.writeBitsFromIntRight(v,bits.length);
		}
	}
	
	public int readBit(int index) {
		if(index>=this.pointer) {
			throw new RuntimeException("index is over bitstream bound!index="+index) ;
		}else {
			int one = 0xFF & bytes[index/8];
			int r = one >> (7-index%8) & 0x01;
			return r;
		}
	}
	
	public int size() {
		return pointer;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<this.pointer;i++) {
			sb.append(this.readBit(i));
			if(i%4==3) {
				sb.append(" ");
			}
		}
		return sb.toString();
	}
}