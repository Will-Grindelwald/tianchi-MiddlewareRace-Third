package io.openmessaging.demo;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {

	private final String path;
	public final String bucket;

	private final PersistenceFile logFile; // Log File
	private final WriteBuffer writeBuffer; // Write Buffer

	private ConcurrentLinkedQueue<byte[]> cacheMessageQueue = new ConcurrentLinkedQueue<byte[]>();
	private AtomicInteger count = new AtomicInteger(0);

	public Topic(String bucket) {
		this.bucket = bucket;
		path = System.getProperty("path") + "/" + bucket;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + "不是一个目录");
		} else {
			file.mkdirs();
		}
		logFile = new PersistenceFile(path, Constants.LOG_FILE_NAME);
		writeBuffer = new WriteBuffer(logFile);
	}

	// for Producer
	public WriteBuffer getWriteBuffer() {
		return writeBuffer;
	}

	public void putMessage(byte[] messageByte) throws InterruptedException {
		cacheMessageQueue.offer(messageByte);
		if (count.incrementAndGet() % Constants.CACHED_MESSAGE_NUMBER == 0) {
			GlobalResource.putWriteTask(new WriteTask(writeBuffer, cacheMessageQueue));
		}
	}

	// for Consumer
	public PersistenceFile getLogFile() {
		return logFile;
	}

	public void flush() throws InterruptedException {
		if (!cacheMessageQueue.isEmpty()) {
			GlobalResource.putWriteTask(new WriteTask(writeBuffer, cacheMessageQueue));
		}
	}

}
