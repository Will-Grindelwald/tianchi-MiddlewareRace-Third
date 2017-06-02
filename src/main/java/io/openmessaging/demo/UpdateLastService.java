package io.openmessaging.demo;

public class UpdateLastService implements Runnable {

	@Override
	public void run() {
		try {
			Topic topic;
			while (true) {
				while ((topic = GlobalResource.takeReadyTopic()) == null) {
				}
				topic.doAppendMessage();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
