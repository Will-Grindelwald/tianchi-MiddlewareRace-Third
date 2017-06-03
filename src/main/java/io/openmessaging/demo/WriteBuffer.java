package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * READ WRITE MappedByteBuffer Wrapper for Producer. 写缓存系统.
 */
public class WriteBuffer {

	private static final int bufferSize = Constants.LOG_BUFFER_SIZE;

	private final PersistenceFile logFile;
	private final LastFile lastFile;
	private FileChannel mappedFileChannel = null;
	private MappedByteBuffer buffer;
	private int blockNumber; // 当前映射块在整个 topic 中的 块号

	private boolean open = false; // for init
	private boolean close = false; // for flush

	public WriteBuffer(PersistenceFile logFile, LastFile lastFile) {
		this.logFile = logFile;
		this.lastFile = lastFile;
		blockNumber = lastFile.nextMessageOffset / bufferSize;
	}

	public void init() {
		// get FileChannel
		mappedFileChannel = logFile.getFileChannel();
		if (mappedFileChannel == null) {
			System.err.println("ERROR PersistenceFile 丢失");
			new Exception().printStackTrace();
			System.exit(0);
		}
		// init mappedBuffer
		try {
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, blockNumber * bufferSize, bufferSize);
			buffer.position((int) (lastFile.nextMessageOffset % bufferSize));
			open = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized boolean write(byte[] bytes) throws InterruptedException {
		if (!open)
			init();
		try {
			lastFile.nextMessageOffset += bytes.length;
			if (buffer.remaining() < bytes.length) {
				int size = buffer.remaining();
				buffer.put(bytes, 0, size);
				buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, (++blockNumber) * bufferSize,
						bufferSize);
				buffer.put(bytes, size, bytes.length - size);
			} else {
				buffer.put(bytes);
			}
			if (buffer.remaining() == 0) {
				buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, (++blockNumber) * bufferSize,
						bufferSize);
			} else if (close) {
				lastFile.flush();
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	// 由 GlobalResource.flush 而来, 只会调用一次
	public void flush() {
		if (!close)
			close = true;
		lastFile.flush();
	}

}
