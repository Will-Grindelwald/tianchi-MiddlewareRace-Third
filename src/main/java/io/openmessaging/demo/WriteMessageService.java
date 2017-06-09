package io.openmessaging.demo;

public class WriteMessageService implements Runnable {

	@Override
	public void run() {
		WriteTask task = null;
		while (true) {
			try {
				// 1
				if (task == null) {
					task = GlobalResource.takeWriteTask();
				}
				// 2
				task.WriteBuffer.write(task.messageBytes, task.offset);
				task = null;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
