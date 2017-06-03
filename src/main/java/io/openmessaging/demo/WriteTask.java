package io.openmessaging.demo;

public class WriteTask {
	public final byte[] messageByte;
	public final WriteBuffer WriteBuffer;

	public WriteTask(WriteBuffer WriteBuffer, byte[] messageByte) {
		this.WriteBuffer = WriteBuffer;
		this.messageByte = messageByte;
	}
}
