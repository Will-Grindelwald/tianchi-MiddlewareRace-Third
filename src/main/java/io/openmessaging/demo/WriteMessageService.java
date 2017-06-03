package io.openmessaging.demo;

public class WriteMessageService implements Runnable {

	private final int ID;

	public WriteMessageService(int ID) {
		this.ID = ID;
	}

	@Override
	public void run() {
		WriteTask task = null;
		try {
			while (true) {
				task = GlobalResource.takeWriteTask(ID);
				task.WriteBuffer.write(task.messageByte);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
