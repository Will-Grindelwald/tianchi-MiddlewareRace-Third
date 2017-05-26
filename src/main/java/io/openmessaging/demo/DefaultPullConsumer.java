package io.openmessaging.demo;

import java.nio.channels.FileChannel;
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
	// 存 queue name & topic name, set 去重
	private Set<String> buckets = new HashSet<>();
	// 内容同 buckets, list 随机读
	private List<String> bucketList = new ArrayList<>();
	// 存 <bucket name, offsetInIndexFile>
	private HashMap<String, Long> offsets = new HashMap<>();

	private static final int BUFFER_SIZE = 20 * 1024 * 1024;
	private ReadBuffer readIndexFileBuffer = null;
	private ReadBuffer readLogFileBuffer = null;

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
		long offsetInIndexFile;
		Message message;
		// 慢轮询, 不致饿死后面的 topic, 又可提高 page cache 命中
		for (int index = 0; index < bucketList.size(); index++) {
			bucket = bucketList.get(lastIndex);
			offsetInIndexFile = offsets.getOrDefault(bucket, Long.valueOf(0));
			message = getNewMessage(bucket, offsetInIndexFile);
			if (message != null) {
				// TODO 修改 offset
				offsets.put(bucket, offsetInIndexFile + 30);
				return message;
			}
			// 只有不命中时才 lastIndex++, 命中时(此 topic 有新 message)会下一次继续读此 topic
			lastIndex = (lastIndex + 1) % (bucketList.size());
		}
		return null;
	}

	// 跨过 messageStore, 利用 MappedBuffer 直接读 message
	private Message getNewMessage(String bucket, long offsetInIndexFile) {
		// TODO
		// 第一次
		if (readIndexFileBuffer == null) {
			FileChannel indexFileChannel = messageStore.getIndexFileChannel(bucket);
			readIndexFileBuffer = new ReadBuffer(indexFileChannel, BUFFER_SIZE);
			// TODO 测试 load 与 不 load 谁快
			// readIndexFileBuffer.buffer.load();
		}

		// TODO 边界用 >= ? 待测
		if (offsetInIndexFile > readIndexFileBuffer.offsetInFile) {
			// readIndexFileBuffer 缓存不命中

		} else {
			// readIndexFileBuffer 缓存命中
			int size = IndexFile.INDEX_SIZE;
			if(size > readIndexFileBuffer.offsetInFile - offsetInIndexFile) {
				// 要取得 Index 是 Buffer 最后一条 Index 且内容不全
				
			} else {
				// other, 正常读
				byte[] fileID = new byte[6];
				readIndexFileBuffer.buffer.get(fileID);
				long offsetInLogFile = readIndexFileBuffer.buffer.getLong();
				int messageSize = readIndexFileBuffer.buffer.getInt();
			}
		}

		return messageStore.pullMessage(bucket, offsetInIndexFile);
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
		queue = queueName;
		buckets.add(queueName);
		buckets.addAll(topics);
		bucketList.clear();
		bucketList.addAll(buckets);
		// TODO 待测试
		// 1. do nothing
		// 2. 排序, 提高 page cache 命中 <-- 目测它最快
		// bucketList.sort(null);
		// 3. 打乱顺序, 减少集中读一个 topic, 提高并发
		// Collections.shuffle(bucketList);
	}

}
