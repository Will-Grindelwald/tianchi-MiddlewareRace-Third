package io.openmessaging.demo;

public class WriteTask {
	public final byte[] messageBytes;
	public final WriteBuffer3 WriteBuffer;
	public final long offset;

	public WriteTask(byte[] messageBytes, WriteBuffer3 WriteBuffer, long offset) {
		this.messageBytes = messageBytes;
		this.WriteBuffer = WriteBuffer;
		this.offset = offset;
	}
}
