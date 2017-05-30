package io.openmessaging.demo;

import java.util.HashMap;

public class WriteMessageService implements Runnable {

	private final HashMap<String, Topic> topicCache = new HashMap<>();

	private int number; //// test

	public WriteMessageService(int number) {
		this.number = number; //// test
	}

	@Override
	public void run() {
		WriteTask task = null;
		Topic taskTopic;
		while (true) {
			try {
				// 1
				if (task == null) {
					task = GlobalResource.WriteTaskBlockQueue.take();
					System.out.println("WriteQueue1出" + GlobalResource.WriteTaskBlockQueue.size()); //// test
					System.out.println("1b:" + number); //// test
				}
				// 2
//				if ((taskTopic = topicCache.get(task.bucket)) == null) {
//					taskTopic = GlobalResource.getTopicByName(task.bucket);
//					topicCache.put(task.bucket, taskTopic);
//				}
				taskTopic = GlobalResource.getTopicByName(task.bucket);
				// 3
				taskTopic.appendMessageBytes(task.messageBytes, task.offset);
				task = null;
			} catch (Exception e) {
//			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
