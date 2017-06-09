package io.openmessaging.demo;

import java.util.HashMap;

public class WriteMessageService implements Runnable {

	private final HashMap<String, Topic> topicCache = new HashMap<>();

	@Override
	public void run() {
		WriteTask task = null;
		Topic taskTopic;
		while (true) {
			try {
				// 1
				if (task == null) {
					task = GlobalResource.takeWriteTask();
					System.out.println("WriteQueue 出队, 剩余 " + GlobalResource.getSizeOfWriteTaskBlockQueue()); //// test
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
