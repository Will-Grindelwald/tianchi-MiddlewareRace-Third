package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

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

	private byte[] compressedBuffer = new byte[2 * 1024 * 1024];
	
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

	public boolean write(ConcurrentLinkedQueue<byte[]> cacheMessageQueue) throws InterruptedException {
		// lock
		lock.lock();
		if (!open)
			init();
		try {
			// 压缩
			byte[] bytes = gZip(cacheMessageQueue);
			// 写入 length
			if (logBuffer.remaining() < 4) {
				int size = logBuffer.remaining();
				byte[] intByte = Utils.intToByteArray(bytes.length);
				logBuffer.put(intByte, 0, size);
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
				logBuffer.put(intByte, size, 4 - size);
			} else {
				logBuffer.putInt(bytes.length);
			}
			// 写入 bytes
			if (logBuffer.remaining() < bytes.length) {
				int size = logBuffer.remaining();
				logBuffer.put(bytes, 0, size);
				logBuffer = logMappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
						(++blockNumberForLog) * LOG_BUFFER_SIZE, LOG_BUFFER_SIZE);
				logBuffer.put(bytes, size, bytes.length - size);
			} else {
				logBuffer.put(bytes);
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

	public byte[] gZip(ConcurrentLinkedQueue<byte[]> cacheMessageQueue) {
		byte[] bytes = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			for (int i = 0; i < Constants.CACHED_MESSAGE_NUMBER; i++) {
				bytes = cacheMessageQueue.poll();
				if (bytes != null) {
					gzip.write(bytes);
				}
			}
			gzip.finish();
			gzip.flush();
			gzip.close();
			bytes = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return bytes;
	}

}
