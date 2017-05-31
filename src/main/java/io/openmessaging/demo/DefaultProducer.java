package io.openmessaging.demo;

import java.util.concurrent.locks.ReentrantLock;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer implements Producer {
	private final KeyValue properties;
	private final MessageStore messageStore = new MessageStore();
	private final MessageFactory messageFactory = new DefaultMessageFactory();
	private static final ReentrantLock sendLock = new ReentrantLock();

	 private ReentrantLock putMessageNormalLock = new ReentrantLock(); 
	public DefaultProducer(KeyValue properties) {
		this.properties = properties;
		if (System.getProperty("path") == null)
			System.setProperty("path", properties.getString("STORE_PATH"));
	}

	@Override
	public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
		return messageFactory.createBytesMessageToTopic(topic, body);
	}

	@Override
	public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
		return messageFactory.createBytesMessageToQueue(queue, body);
	}

	@Override
	public void start() {

	}

	@Override
	public void shutdown() {

	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public void send(Message message) {
		sendLock.lock();
		if (message == null)
			throw new ClientOMSException("Message should not be null");
		String topic = message.headers().getString(MessageHeader.TOPIC);
		String queue = message.headers().getString(MessageHeader.QUEUE);
		if ((topic == null && queue == null) || (topic != null && queue != null)) {
			throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
		}
		messageStore.putMessage(topic != null ? topic : queue, message);
		sendLock.unlock();
	}

	@Override
	public void send(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void flush() {
		try {
			messageStore.flush();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
