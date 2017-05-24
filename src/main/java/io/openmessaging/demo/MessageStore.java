package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

public class MessageStore {

	// private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();
	// private Map<String, HashMap<String, Integer>> queueOffsets = new
	// HashMap<>();

	// private boolean flag = false;

	private String path;

	public MessageStore(String path) {
		this.path = path;
	}

	public void putMessage(String bucket, Message message) {
		// if (!messageBuckets.containsKey(bucket)) {
		// messageBuckets.put(bucket, new ArrayList<>(1024));
		// }
		// ArrayList<Message> bucketList = messageBuckets.get(bucket);
		// bucketList.add(message);
		CommitLog cl = CommitLogHandler.getCommitLogByName(path, bucket);
		byte[] messages = messageToBytes(message);

		cl.appendMessage(messages);

		// TODO 分字段写入：headers,propersites,body等
		// ByteBuffer
		// bb=ByteBuffer.wrap(getObjectBytes(((DefaultKeyValue)(message.headers()))));

	}

	public byte[] getObjectBytes(DefaultKeyValue kv) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bout)) {
			out.writeObject(kv.getKVS());
			out.flush();
			byte[] bytes = bout.toByteArray();
			bout.close();
			return bytes;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public byte[] messageToBytes(Message message) {
		// TODO 添加数据压缩
		byte[] byteHeaders = getObjectBytes((DefaultKeyValue) (message.headers()));
		byte[] byteProperties = getObjectBytes((DefaultKeyValue) message.properties());
		byte[] byteBody = ((BytesMessage) message).getBody();
		byte[] bytes = Arrays.copyOf(byteHeaders, byteHeaders.length + byteProperties.length + byteBody.length);
		System.arraycopy(byteProperties, 0, bytes, byteHeaders.length, byteProperties.length);
		System.arraycopy(byteBody, 0, bytes, byteHeaders.length + byteProperties.length, byteBody.length);
		return bytes;
	}

	public Message bytesToMessage(byte[] bytes) {
		// TODO 添加数据解压缩
		return null;
	}

	public Message pullMessage(String bucket, long offsetInIndexFile) {
		CommitLog commitLog = CommitLogHandler.getCommitLogByName(path, bucket);
		return commitLog.getNewMessage(offsetInIndexFile);
	}

	public FileChannel getIndexFileChannel(String bucket) {
		return CommitLogHandler.getCommitLogByName(path, bucket).getIndexFileChannel();
	}

}
