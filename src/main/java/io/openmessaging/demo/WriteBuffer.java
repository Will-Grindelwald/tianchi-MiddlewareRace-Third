package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * READ WRITE MappedByteBuffer Wrapper for Producer. 写缓存系统.
 */
public class WriteBuffer {

	private static final int LOG_BUFFER_SIZE = Constants.LOG_BUFFER_SIZE;
	private FileChannel logMappedFileChannel = null;
	private MappedByteBuffer logBuffer;
	private int blockNumberForLog; // 当前映射块在整个 topic log 中的块号

	private volatile boolean open = false; // for init

	private ReentrantLock lock = new ReentrantLock();

	public WriteBuffer(PersistenceFile logFile) {
		logMappedFileChannel = logFile.getFileChannel();
	}

	public void init() {
		try {
			blockNumberForLog = 0;
			logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, LOG_BUFFER_SIZE);
			open = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean write(byte[] bytes) throws InterruptedException {
		// lock
		lock.lock();
		if (!open)
			init();
		try {
			if (logBuffer.remaining() < bytes.length) {
				int size = logBuffer.remaining();
				logBuffer.put(bytes, 0, size);
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
				logBuffer.put(bytes, size, bytes.length - size);
			} else {
				logBuffer.put(bytes);
			}
			if (logBuffer.remaining() == 0) {
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// unlock
			lock.unlock();
		}
		return false;
	}

}
