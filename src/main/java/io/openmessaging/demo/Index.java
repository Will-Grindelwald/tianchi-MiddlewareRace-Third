package io.openmessaging.demo;

/**
 * 索引文件结构：
 *  ------------------- 
 *  |offset|mesageSize|
 *  |long  |int       | 
 *  -------------------
 */
public class Index {

	public long offset;
	public int size;

	public Index(long offset, int size) {
		this.offset = offset;
		this.size = size;
	}

	public static long getOffset(byte[] index) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE))
			return Utils.getLong(index, Constants.OFFSET_POS);
		return -1;
	}

	public static int getSize(byte[] index) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE))
			return Utils.getInt(index, Constants.SIZE_POS);
		return -1;
	}

	public static boolean setOffset(byte[] index, long offset) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE)) {
			Utils.longToByteArray(offset, index, Constants.OFFSET_POS);
			return true;
		}
		return false;
	}

	public static boolean setSize(byte[] index, int size) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE)) {
			Utils.intToByteArray(size, index, Constants.SIZE_POS);
			return true;
		}
		return false;
	}

}
