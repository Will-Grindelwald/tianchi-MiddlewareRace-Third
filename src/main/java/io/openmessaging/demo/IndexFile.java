package io.openmessaging.demo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 索引文件结构：
 * --------------------------
 * |fileID|offset|mesagesize|
 * |000000|long  |int       |
 * --------------------------
 */
public class IndexFile extends MappedFile {
	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();
	private long offset;
	private int indexSize = 30;

	private static final int ONEINDEXSIZE = 30 * 1024;
	private static final String NAMEFIRST = "LOG";

	private MappedByteBuffer readMappedByteBuffer, writeMappedByteBuffer;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(ONEINDEXSIZE);

	public IndexFile(String path, String fileName) {
		super(path, fileName);
		init();
	}
	
	public void init(){
		FileChannel tmpFileChannel=super.getFileChannel();
		try {
			writeMappedByteBuffer=tmpFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, tmpFileChannel.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void appendIndex(int size) {
		String fileName = null;
		fileWriteLock.lock();

		if (byteBuffer.remaining() == ONEINDEXSIZE) {
			fileName = NAMEFIRST + "000000";
			offset = 0L;
		} else {
			byte[] tmpfirst=new byte[3];
			byte[] tmplast=new byte[6];
			byteBuffer.get(tmpfirst);
			byteBuffer.get(tmplast);
			offset=byteBuffer.getLong();
			int lastSize=byteBuffer.getInt();
			offset+=lastSize;
			String namefirst=new String(tmpfirst);
			if(!namefirst.equals(NAMEFIRST)){
				System.out.println("校验出现问题，code:1");
				namefirst=NAMEFIRST;
			}
			int namelast=Integer.valueOf(new String(tmplast));
			fileName=namefirst+String.format("%06d", namelast+1);
			
			
		}
		byteBuffer.put(fileName.getBytes());
		byteBuffer.putLong(offset);
		byteBuffer.putInt(size);
		byteBuffer.flip();
		
		
		fileWriteLock.unlock();
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	@Override
	public void flush() {

	}

}
