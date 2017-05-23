package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

public class DefaultPullConsumer implements PullConsumer {
	private KeyValue properties;
	private String path;
	private MessageStore messageStore = new MessageStore(path);
	private String queue;
	
	private Set<String> buckets = new HashSet<>();
	private List<String> bucketList = new ArrayList<>();

	private int lastIndex = 0;

	public DefaultPullConsumer(KeyValue properties) {
		this.properties = properties;		
		this.path=this.properties.getString("STORE_PATH");
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public Message poll() {
		// if (bucketList.size() == 0 || queue == null) {
		// return null;
		// }
		// // use Round Robin
		// int checkNum = 0;
		// while (++checkNum <= bucketList.size()) {
		// String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
		// String path = properties().getString("STORE_PATH");
		// Message message = messageStore.pullMessage(queue, bucket, path);
		// if (message != null) {
		// return message;
		// }
		// }
		return null;
	}

	@Override
	public Message poll(KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public synchronized void attachQueue(String queueName, Collection<String> topics) {
		if (queue != null && !queue.equals(queueName)) {
			throw new ClientOMSException("You have alreadly attached to a queue " + queue);
		}
		queue = queueName;
		buckets.add(queueName);
		buckets.addAll(topics);
		bucketList.clear();
		bucketList.addAll(buckets);
	}

}
