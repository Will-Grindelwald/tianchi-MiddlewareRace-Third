package io.openmessaging.demo;

import java.util.concurrent.ConcurrentLinkedQueue;

public class WriteTask {
	public final WriteBuffer writeBuffer;
	public final ConcurrentLinkedQueue<byte[]> cacheMessageQueue;

	public WriteTask(WriteBuffer writeBuffer, ConcurrentLinkedQueue<byte[]> cacheMessageQueue) {
		this.writeBuffer = writeBuffer;
		this.cacheMessageQueue = cacheMessageQueue;
	}
}
