package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * READ WRITE MappedByteBuffer Wrapper for Producer. 写缓存系统.
 */
public class WriteBuffer {

	private static final int INDEX_BUFFER_SIZE = Constants.INDEX_BUFFER_SIZE;
	private FileChannel indexMappedFileChannel = null;
	private MappedByteBuffer indexBuffer;
//	private int blockNumberForIndex; // 当前映射块在整个 topic index 中的块号

	private static final int LOG_BUFFER_SIZE = Constants.LOG_BUFFER_SIZE;
	private FileChannel logMappedFileChannel = null;
	private MappedByteBuffer logBuffer;
	private int blockNumberForLog; // 当前映射块在整个 topic log 中的块号

	private byte[] bufferL2 = new byte[10 * 1024 * 1024];
	private int count = 0;
	private volatile int nextMessageOffset;

	private boolean open = false; // for init

	public WriteBuffer(PersistenceFile logFile, PersistenceFile indexFile) {
		indexMappedFileChannel = indexFile.getFileChannel();
		logMappedFileChannel = logFile.getFileChannel();
	}

	public void init() {
		try {
//			if (indexMappedFileChannel.size() == 0) { // 第一次启动生产者
//				blockNumberForIndex = 0;
				indexBuffer = indexMappedFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_BUFFER_SIZE);
				nextMessageOffset = 0;
				indexBuffer.putInt(0);
				blockNumberForLog = 0;
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, LOG_BUFFER_SIZE);
//			} else {
//				blockNumberForIndex = (int) indexMappedFileChannel.size() / INDEX_BUFFER_SIZE;
//				indexBuffer = indexMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						blockNumberForIndex * INDEX_BUFFER_SIZE, INDEX_BUFFER_SIZE);
//				indexBuffer.position((int) indexMappedFileChannel.size() - Constants.INDEX_SIZE);
//				nextMessageOffset = indexBuffer.getInt();
//				blockNumberForLog = nextMessageOffset / LOG_BUFFER_SIZE;
//				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						blockNumberForLog * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
//				logBuffer.position((int) (nextMessageOffset % LOG_BUFFER_SIZE));
//			}
			open = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//	public synchronized boolean write(byte[] bytes) throws InterruptedException {
//		if (!open)
//			init();
//		try {
//			nextMessageOffset += bytes.length;
//			indexBuffer.putInt(nextMessageOffset);
//			if (logBuffer.remaining() < bytes.length) {
//				int size = logBuffer.remaining();
//				logBuffer.put(bytes, 0, size);
//				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
//				logBuffer.put(bytes, size, bytes.length - size);
//			} else {
//				logBuffer.put(bytes);
//			}
//			if (indexBuffer.remaining() == 0) {
//				indexBuffer = indexMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						(++blockNumberForIndex) * INDEX_BUFFER_SIZE, INDEX_BUFFER_SIZE);
//			}
//			if (logBuffer.remaining() == 0) {
//				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
//			}
//			return true;
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return false;
//	}

	public synchronized boolean write(byte[] bytes) throws InterruptedException {
		if (!open)
			init();
		try {
			nextMessageOffset += bytes.length;
			indexBuffer.putInt(nextMessageOffset);
			if (count + bytes.length > bufferL2.length) {
				int size = bufferL2.length - count;
				System.arraycopy(bytes, 0, bufferL2, count, size);
				logBuffer.put(bufferL2);
				count = bytes.length - size;
				System.arraycopy(bytes, size, bufferL2, 0, count);
			} else {
				System.arraycopy(bytes, 0, bufferL2, count, bytes.length);
				count += bytes.length;
			}
//			if (indexBuffer.remaining() == 0) {
//				indexBuffer = indexMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
//						(++blockNumberForIndex) * INDEX_BUFFER_SIZE, INDEX_BUFFER_SIZE);
//			}
			if (logBuffer.remaining() == 0) {
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void flush() {
		logBuffer.put(bufferL2, 0, count);
	}
}
