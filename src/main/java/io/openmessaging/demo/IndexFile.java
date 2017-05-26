package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 索引文件结构：
 *  -------------------------- 
 *  |fileID|offset|mesagesize|
 *  |000000|long|int | 
 *  --------------------------
 */
// TODO 将 offset 改为 int ?
public class IndexFile {
	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();

	private String path;
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;
	private MappedByteBuffer writeMappedByteBuffer;
	private ByteBuffer lastIndex = ByteBuffer.allocate(Constants.INDEX_SIZE);
	private static AtomicInteger count = new AtomicInteger(0);

	public IndexFile(String path, String fileName) {
		this.path = path;
		this.fileName = fileName;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			this.fileChannel = this.file.getChannel();
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,
					Constants.INDEX_WRITE_BUFFER_SIZE);
		} catch (IOException e) {
			throw new ClientOMSException("IndexFile create failure", e);
		}
	}

	// for Producer
	public String appendIndex(int size) {
		fileWriteLock.lock();
		String fileID;
		byte[] previousMessageFileID = new byte[6];
		int Offset;
		int previousMessageSize;
		if (lastIndex.remaining() == Constants.INDEX_SIZE) {
			fileID = "000000";
			Offset = 0;
			previousMessageSize = 0;
		} else {

			lastIndex.flip();
			lastIndex.get(previousMessageFileID);
			Offset = lastIndex.getInt();
			previousMessageSize = lastIndex.getInt();
			int name = Integer.valueOf(new String(previousMessageFileID));
			Offset += previousMessageSize;
			if (Offset + size > Constants.LOG_FILE_SIZE) {
				fileID = String.format("%06d", name + 1);
				Offset = 0;
			} else {
				fileID = String.format("%06d", name);
			}
			lastIndex.clear();
		}
		lastIndex.put(fileID.getBytes());
		lastIndex.putInt(Offset);
		lastIndex.putInt(size);
		lastIndex.flip();
		if (writeMappedByteBuffer.remaining() < lastIndex.limit()) {
			flush();
		}
		writeMappedByteBuffer.put(lastIndex);
		fileWriteLock.unlock();
		return fileID + ":" + Offset;
	}

	// for Producer
	public String getFileName() {
		return this.fileName;
	}

	// for Consumer
	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

	// for Producer
	public void flush() {
		// writeMappedByteBuffer.flip();
		writeMappedByteBuffer.force();
		writeMappedByteBuffer.clear();
		try {
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
					count.incrementAndGet() * Constants.INDEX_WRITE_BUFFER_SIZE, Constants.INDEX_WRITE_BUFFER_SIZE);
		} catch (IOException e) {
			System.out.println("MappedByteBuffer Exception");
			e.printStackTrace();
		}
	}

}
