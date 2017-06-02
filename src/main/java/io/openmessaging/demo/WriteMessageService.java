package io.openmessaging.demo;

public class WriteMessageService implements Runnable {

	@Override
	public void run() {
		WriteTask task = null;
		try {
			while (true) {
				// 1
				if (task == null) {
					task = GlobalResource.takeWriteTask();
				}
				// 2
				if (task.messageBytes != null) {
					task.WriteBuffer.write(task.messageBytes, task.offset);
				} else {
					// task.WriteBuffer.write(task.intValue, task.offset);
				}
				task = null;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
