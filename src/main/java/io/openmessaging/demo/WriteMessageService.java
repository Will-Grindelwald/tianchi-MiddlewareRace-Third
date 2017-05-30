package io.openmessaging.demo;

import java.util.HashMap;

public class WriteMessageService implements Runnable {

	private final HashMap<String, Topic> topicCache = new HashMap<>();
	private WriteTask task = null;
	private Topic taskTopic;
	private int number;

	public WriteMessageService(int number) {
		this.number = number;
	}
	@Override
	public void run() {
		while (true) {
			try {
				// 1
				if (task == null) {
					task = GlobalResource.WriteTaskBlockQueue.take();
					System.out.println("1b:" + number);
				}
				// 2
				if ((taskTopic = topicCache.get(task.bucket)) == null) {
					taskTopic = GlobalResource.getTopicByName(task.bucket);
					topicCache.put(task.bucket, taskTopic);
				}
				// 3
				taskTopic.appendMessageBytes(task.messageBytes, task.offset);
				task = null;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
