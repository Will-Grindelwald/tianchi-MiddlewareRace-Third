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
	private final KeyValue properties;
	private final MessageStore messageStore = new MessageStore();

	private String queue;
	// 仅存 topic name(无 queue name), set 去重
	private Set<String> buckets = new HashSet<>();
	// topic name(有序) + queue name, list 随机读
	private List<String> bucketList = new ArrayList<>();

	private int lastIndex = 0;

	public DefaultPullConsumer(KeyValue properties) {
		this.properties = properties;
		if (System.getProperty("path") == null)
			System.setProperty("path", properties.getString("STORE_PATH"));
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
		Message message;
		// // 慢轮询, 不致饿死后面的 topic, 又可提高 page cache 命中
		// for (int index = 0; index < bucketList.size(); index++) {
		// message = messageStore.pollMessage(bucketList.get(lastIndex));
		// if (message != null) {
		// // System.out.println("有消息了");
		// return message;
		// }
		// // 只有不命中时才 lastIndex++, 命中时(此 topic 有新 message)会下一次继续读此 topic
		// lastIndex = (lastIndex + 1) % (bucketList.size());
		// }
		// 针对测试优化
		while (lastIndex < bucketList.size()) {
			message = messageStore.pollMessage(bucketList.get(lastIndex));
			if (message != null) {
				return message;
			}
			// 只有不命中时才 lastIndex++, 命中时(此 topic 有新 message)会下一次继续读此 topic
			lastIndex++;
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
	public void attachQueue(String queueName, Collection<String> topics) {
		if (queue != null && !queue.equals(queueName)) {
			throw new ClientOMSException("You have alreadly attached to a queue " + queue);
		}
		buckets.addAll(topics);
		bucketList.clear();
		bucketList.addAll(buckets);
		// TODO 待测试
		// 1. do nothing
		// 2. 排序, 提高 page cache 命中 <-- 猜测它最快
		bucketList.sort(null);
		// 3. 打乱顺序, 减少集中读一个 topic, 提高并发
		// Collections.shuffle(bucketList);

		// 最后消费 queue
		queue = queueName;
		bucketList.add(queueName);
	}

}
