package org.hmf.tsdb.server.coder;

/**
 *  for read bit from byte array
 */
public class BitInputStream{
	byte[] bytes = null;
	int pointer = 0;
	
	public BitInputStream(byte[] in) {
		bytes = in;
	}
	/**
	 * 从bit流读取一个bit
	 * @return 0或者1，如果是-1说明流已经到末尾了
	 */
	public int readBit() {	
		if(pointer<bytes.length*8) {
			int one = 0xFF & bytes[pointer/8];
			int r = one >> (7-pointer%8) & 0x01;
			pointer++;
			return r;
		}else {
			return -1;
		}
	}
	
	public int readBit(int size) {
		if(size>32) {
			throw new RuntimeException("每次最多只能读取32个bit");
		}else {
			int v =  0;
			for(int i=0;i<size;i++) {
				int bit = this.readBit();
				v =  v | bit<<(size-i-1);
			}
			return v;
		}
	}
	
	
	public void reset() {
		pointer = 0;
	}		
}