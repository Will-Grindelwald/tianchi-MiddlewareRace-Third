package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

public class DefaultPullConsumer implements PullConsumer {
	private KeyValue properties;
	private MessageStore messageStore;

	private String queue;
	private Set<String> buckets = new HashSet<>(); // 存 queue name & topic name, set 去重
	private List<String> bucketList = new ArrayList<>(); // 内容同 buckets, list 随机读
	private HashMap<String, Long> offsets = new HashMap<>(); // 存 <bucket name, offset>

	private int lastIndex = 0;

	public DefaultPullConsumer(KeyValue properties) {
		this.properties = properties;
		messageStore = new MessageStore(properties.getString("STORE_PATH"));
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public Message poll() {
		if (bucketList.size() == 0 || queue == null) {
			return null;
		}

		String bucket;
		Message message;
		for (int checkNum = 0; checkNum < bucketList.size(); checkNum++) {
			bucket = bucketList.get((++lastIndex) % (bucketList.size()));
			message = messageStore.pullMessage(bucket, offsets.getOrDefault(bucket, new Long(0)));
			if (message != null) {
				// TODO 修改 offset
				offsets.put(bucket, null);
				return message;
			}
		}
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
