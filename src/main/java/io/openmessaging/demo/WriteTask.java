package io.openmessaging.demo;

public class WriteTask {
	public byte[] messageBytes;
	public int intValue;
	public final WriteBuffer3 WriteBuffer;
	public final long offset;

	public WriteTask(WriteBuffer3 WriteBuffer, byte[] messageBytes, long offset) {
		this.WriteBuffer = WriteBuffer;
		this.messageBytes = messageBytes;
		this.offset = offset;
	}

	public WriteTask(WriteBuffer3 WriteBuffer, int intValue, long offset) {
		this.WriteBuffer = WriteBuffer;
		this.intValue = intValue;
		this.offset = offset;
	}

}
