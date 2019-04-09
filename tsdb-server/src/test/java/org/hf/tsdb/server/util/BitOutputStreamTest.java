package org.hf.tsdb.server.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.hmf.tsdb.server.coder.BitOutputStream;
import org.junit.Before;
import org.junit.Test;

public class BitOutputStreamTest {
	BitOutputStream stream = null;
	
	@Before
	public void setup() {
		stream = new BitOutputStream(1024);
	}

	@Test
	public void testWrite32Bits() {
		stream.write32Bits(512);
		assertEquals(32,stream.size());
		assertEquals("0000 0000 0000 0000 0000 0010 0000 0000",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.writeBits(1,0,0,0);
		stream.write32Bits(512);
		assertEquals(36,stream.size());
		assertEquals("1000 0000 0000 0000 0000 0000 0010 0000 0000",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.write08Bits((byte)8);
		stream.write32Bits(512);
		assertEquals(40,stream.size());
		assertEquals("0000 1000 0000 0000 0000 0000 0000 0010 0000 0000",stream.toString().trim());
	}

	@Test
	public void testWrite16Bits() {
		stream.write16Bits(512);
		assertEquals(16,stream.size());
		assertEquals("0000 0010 0000 0000",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.writeBits(1,0,0);
		stream.write16Bits(512);
		assertEquals(19,stream.size());
		assertEquals("1000 0000 0100 0000 000",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.write08Bits((byte)8);
		stream.write16Bits(512);
		assertEquals(24,stream.size());
		assertEquals("0000 1000 0000 0010 0000 0000",stream.toString().trim());
	}

	@Test
	public void testWrite08Bits() {
		stream.write08Bits((byte)255);
		assertEquals(8,stream.size());
		assertEquals("1111 1111",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.writeBits(1,0,0);
		stream.write08Bits((byte)255);
		assertEquals(11,stream.size());
		assertEquals("1001 1111 111",stream.toString().trim());
		
		stream = new BitOutputStream(1024);
		stream.writeBits(1,0,1,0,1,0,1,0);
		stream.write08Bits((byte)255);
		assertEquals(16,stream.size());
		assertEquals("1010 1010 1111 1111",stream.toString().trim());
	}

	@Test
	public void testWriteOneBit() {
		stream.writeOneBit(1);
		stream.writeOneBit(0);
		assertEquals(2,stream.size());
		
		stream.writeOneBit(1);
		stream.writeOneBit(0);
		stream.writeOneBit(1);
		stream.writeOneBit(0);
		stream.writeOneBit(1);
		stream.writeOneBit(0);
		assertEquals(8,stream.size());
		stream.writeOneBit(1);
		stream.writeOneBit(0);
		assertEquals(10,stream.size());
		assertEquals("1010 1010 10",stream.toString().trim());
	}
	
	@Test
	public void testWriteBitsFromByteRight() {
		stream.writeBitsFromByteRight(4, 3);
		assertEquals(3,stream.size());
		assertEquals("100",stream.toString().trim());
		
		stream.writeBitsFromByteRight(8, 4);
		assertEquals(7,stream.size());
		assertEquals("1001 000",stream.toString().trim());
		
		stream.writeBitsFromByteRight(9, 4);
		assertEquals(11,stream.size());
		assertEquals("1001 0001 001",stream.toString().trim());
	}

	@Test
	public void testWriteBitsFromIntRight() {
		stream.writeBitsFromIntRight(4, 3);
		assertEquals(3,stream.size());
		assertEquals("100",stream.toString().trim());
		
		stream.writeBitsFromIntRight(8, 4);
		assertEquals(7,stream.size());
		assertEquals("1001 000",stream.toString().trim());
		
		stream.writeBitsFromIntRight(9, 4);
		assertEquals(11,stream.size());
		assertEquals("1001 0001 001",stream.toString().trim());
	}

	

	@Test
	public void testWriteBits() {
		stream.writeBits(1,0,0,0);
		assertEquals(4,stream.size());
		assertEquals("1000",stream.toString().trim());
		stream.writeBits(1,0);
		assertEquals(6,stream.size());
		assertEquals("1000 10",stream.toString().trim());
		stream.writeBits(1,1,1,1,0,0,0,0);
		assertEquals(14,stream.size());
		assertEquals("1000 1011 1100 00",stream.toString().trim());
	}
	
	@Test
	public void testSubBits() {
		int sub = stream.subBits(96, 24, 28);
		assertEquals(6,sub);
		
		sub = stream.subBits(852, 22,30);
		System.out.println(sub);
		stream.write16Bits(sub);
		System.out.println(stream.toString());
		assertEquals(213,sub);
	}

}
