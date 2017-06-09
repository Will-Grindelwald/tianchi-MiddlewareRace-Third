package io.openmessaging.demo;

public class WriteTask {
	public final byte[] messageBytes;
	public final String bucket;
	public final long offset;

	public WriteTask(byte[] messageBytes, String bucket, long offset) {
		this.messageBytes = messageBytes;
		this.bucket = bucket;
		this.offset = offset;
	}
}
