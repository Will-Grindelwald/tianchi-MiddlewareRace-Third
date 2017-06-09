package io.openmessaging.demo;

/**
 * 索引文件结构：
 *  -------------------------- 
 *  |fileID|offset|mesageSize|
 *  |int   |int   |int       | 
 *  --------------------------
 */
public class Index {

	public int fileID;
	public int offset;
	public int size;

	public Index(int fileID, int offset, int size) {
		this.fileID = fileID;
		this.offset = offset;
		this.size = size;
	}

	public static int getFileID(byte[] index) {
		return getElement(index, Constants.FILEID_POS);
	}

	public static int getOffset(byte[] index) {
		return getElement(index, Constants.OFFSET_POS);
	}

	public static int getSize(byte[] index) {
		return getElement(index, Constants.SIZE_POS);
	}

	public static int getElement(byte[] index, int pos) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE))
			return Utils.getInt(index, pos);
		return -1;
	}

	public static boolean setFileID(byte[] index, int fileID) {
		return setElement(index, fileID, Constants.FILEID_POS);
	}

	public static boolean setOffset(byte[] index, int offset) {
		return setElement(index, offset, Constants.OFFSET_POS);
	}

	public static boolean setSize(byte[] index, int size) {
		return setElement(index, size, Constants.SIZE_POS);
	}

	public static boolean setElement(byte[] index, int value, int pos) {
		if (Utils.checkBoard(index, 0, Constants.INDEX_SIZE)) {
			Utils.intToByteArray(value, index, pos);
			return true;
		}
		return false;
	}
}
