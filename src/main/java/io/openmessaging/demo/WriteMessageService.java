package io.openmessaging.demo;

public class WriteMessageService implements Runnable {

	@Override
	public void run() {
		WriteTask task = null;
		try {
			while (true) {
				task = GlobalResource.takeWriteTask();
				task.writeBuffer.write(task.cacheMessageQueue);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
