package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

// 一个 Producer/Consumer 一个，不会有并发
public class MessageStore {

	private HashMap<String, Topic> topicCache = new HashMap<>();

	// for Producer
	private final ByteBuffer KVToBytesBuffer = ByteBuffer.allocate(2 * 1024 * 1024);

	// for Consumer
	// 存 <bucket name, offsetInIndexFile>
	private final HashMap<String, Long> offsets = new HashMap<>();
	private final ReadBuffer readIndexFileBuffer = new ReadBuffer();
	private final ReadBuffer readLogFileBuffer = new ReadBuffer();

	public MessageStore() {
	}

	// for Producer
	public void putMessage(String bucket, Message message) {
		if (message == null)
			return;
		byte[] messageByte = messageToBytes(message);

		// 放入阻塞队列
		Topic topic;
		if ((topic = topicCache.get(bucket)) == null) {
			topic = GlobalResource.getTopicByName(bucket);
			topicCache.put(bucket, topic);
		}
		topic.putMessageToQueue(messageByte);
	}

	// for Consumer
	// 利用 MappedBuffer 读 message
	public Message pollMessage(String bucket) {
		Topic topic;
		if ((topic = topicCache.get(bucket)) == null) {
			topic = GlobalResource.getTopicByName(bucket);
			topicCache.put(bucket, topic);
		}
		// Step 1: 读 Index
		long offsetInIndexFile = offsets.getOrDefault(bucket, Long.valueOf(0));
		FileChannel indexFileChannel = topic.getIndexFileChannelByOffset(offsetInIndexFile);
		byte[] index = readIndexFileBuffer.read(bucket, indexFileChannel, offsetInIndexFile, Constants.INDEX_SIZE);
		if (index == null)
			return null;

		// Step 2: 读 Message
		long offsetInLogFile = Index.getOffset(index);
		int messageSize = Index.getSize(index);
		FileChannel logFileChannel = topic.getLogFileChannelByOffset(offsetInLogFile);
		byte[] messageBytes = readLogFileBuffer.read(bucket, logFileChannel, offsetInLogFile, messageSize);
		if (messageBytes == null)
			return null; // ERROR 有 index 无 message

		// Step 3: 更新 Offset
		Message result = bytesToMessage(messageBytes);
		offsets.put(bucket, offsetInIndexFile + Constants.INDEX_SIZE);
		return result;
	}

	// for Producer
	/**
	 * message 结构
	 * ------------------------------------------------------------------------
	 * |body.length| body |headers.length|headers|properties.length|properties|
	 * |int        |byte[]|int           |byte[] |int              |byte[]    |
	 * ------------------------------------------------------------------------
	 */
	public byte[] messageToBytes(Message message) {
		byte[] byteHeaders = defaultKeyValueToBytes((DefaultKeyValue) (message.headers()));
		byte[] byteProperties = defaultKeyValueToBytes((DefaultKeyValue) message.properties());
		byte[] byteBody = ((BytesMessage) message).getBody();
		byte[] result = new byte[3 * 4 + byteBody.length + byteHeaders.length + byteProperties.length];
		int pos = 0;
		Utils.intToByteArray(byteBody.length, result, pos); // byteBody.length
		pos += 4;
		System.arraycopy(byteBody, 0, result, pos, byteBody.length); // byteBody
		pos += byteBody.length;
		Utils.intToByteArray(byteHeaders.length, result, pos); // byteHeaders.length
		pos += 4;
		System.arraycopy(byteHeaders, 0, result, pos, byteHeaders.length); // byteHeaders
		pos += byteHeaders.length;
		Utils.intToByteArray(byteProperties.length, result, pos); // byteProperties.length
		pos += 4;
		System.arraycopy(byteProperties, 0, result, pos, byteProperties.length); // byteProperties
		// TODO 添加数据压缩
		return result;
	}

	// for Producer
	// defaultKeyValueToBytes 的另一种解决方案
	public byte[] defaultKeyValueToBytes(DefaultKeyValue kv) {
		if (kv == null) {
			return new byte[0];
		}
		Object value;
		byte[] keyBytes, stringValueBytes;
		KVToBytesBuffer.clear();
		for (String key : kv.keySet()) {
			value = kv.get(key);
			if (value instanceof Integer) {
				KVToBytesBuffer.put((byte) 0);
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putInt((Integer) value);
			} else if (value instanceof Long) {
				KVToBytesBuffer.put((byte) 1);
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putLong((Long) value);
			} else if (value instanceof Double) {
				KVToBytesBuffer.put((byte) 2);
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putDouble((Double) value);
			} else {
				KVToBytesBuffer.put((byte) 3);
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				stringValueBytes = ((String) value).getBytes();
				KVToBytesBuffer.putInt(stringValueBytes.length);
				KVToBytesBuffer.put(stringValueBytes);
			}
		}
		KVToBytesBuffer.flip();
		byte[] result = new byte[KVToBytesBuffer.remaining()];
		KVToBytesBuffer.get(result);
		return result;
	}

	// for Consumer
	// message 结构见 messageToBytes
	public Message bytesToMessage(byte[] bytes) {
		// TODO 添加数据解压缩
		int length = Utils.getInt(bytes, 0), pos = 4; // byteBody.length
		byte[] body = new byte[length];
		System.arraycopy(bytes, pos, body, 0, length); // byteBody
		DefaultBytesMessage message = new DefaultBytesMessage(body);
		pos += length;
		length = Utils.getInt(bytes, pos); // byteHeaders.length
		pos += 4;
		if (length != 0) { // byteHeaders
			bytesToDefaultKeyValue((DefaultKeyValue) (message.headers()), bytes, pos, length);
			pos += length;
		}
		length = Utils.getInt(bytes, pos); // byteProperties.length
		pos += 4;
		if (length != 0) { // byteProperties
			bytesToDefaultKeyValue((DefaultKeyValue) (message.properties()), bytes, pos, length);
		}
		return message;
	}

	// for Consumer
	public DefaultKeyValue bytesToDefaultKeyValue(DefaultKeyValue kv, byte[] kvBytes, int offset, int length) {
		if (kv == null) {
			kv = new DefaultKeyValue();
		}
		int end = offset + length, intValue;
		long longValue;
		double doubleValue;
		String key, stringValue;
		while (offset < end) {
			switch (kvBytes[offset++]) {
			case 0: // for int
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				key = new String(kvBytes, offset, intValue);
				offset += intValue;
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				kv.put(key, intValue);
				break;
			case 1: // for long
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				key = new String(kvBytes, offset, intValue);
				offset += intValue;
				longValue = Utils.getLong(kvBytes, offset);
				offset += 8;
				kv.put(key, longValue);
				break;
			case 2: // for double
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				key = new String(kvBytes, offset, intValue);
				offset += intValue;
				doubleValue = Utils.getDouble(kvBytes, offset);
				offset += 8;
				kv.put(key, doubleValue);
				break;
			case 3: // for string
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				key = new String(kvBytes, offset, intValue);
				offset += intValue;
				intValue = Utils.getInt(kvBytes, offset);
				offset += 4;
				stringValue = new String(kvBytes, offset, intValue);
				offset += intValue;
				kv.put(key, stringValue);
				break;
			default:
				System.out.println("ERROR: bytesToDefaultKeyValue");
				break;
			}
		}
		return kv;
	}

	public void flush() throws InterruptedException {
		while (GlobalResource.getSizeOfWriteTaskBlockQueue() != 0) {
			// 全局的 WriteTaskQueue 非空
			Thread.sleep(1000);
		}
		Iterator<Map.Entry<String, Topic>> iterator = topicCache.entrySet().iterator();
		while (iterator.hasNext()) {
			iterator.next().getValue().flush();
		}
	}
}
