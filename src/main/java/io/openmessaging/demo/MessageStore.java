package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

public class MessageStore {

	// private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();
	// private Map<String, HashMap<String, Integer>> queueOffsets = new
	// HashMap<>();

	// private boolean flag = false;

	private String path;

	// 存 <bucket name, offsetInIndexFile>
	private HashMap<String, Integer> offsets = new HashMap<>();

	private ReadBuffer readIndexFileBuffer;
	private ReadBuffer readLogFileBuffer;

	public MessageStore(String path) {
		this.path = path;
		readIndexFileBuffer = new ReadBuffer();
		readLogFileBuffer = new ReadBuffer();
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

	public static byte[] defaultKeyValueToBytes(DefaultKeyValue kv) {
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

	public static byte[] messageToBytes(Message message) {
		// TODO 添加数据压缩
		byte[] byteHeaders = defaultKeyValueToBytes((DefaultKeyValue) (message.headers()));
		byte[] byteProperties = defaultKeyValueToBytes((DefaultKeyValue) message.properties());
		byte[] byteBody = ((BytesMessage) message).getBody();
		byte[] bytes = Arrays.copyOf(byteHeaders, byteHeaders.length + byteProperties.length + byteBody.length);
		System.arraycopy(byteProperties, 0, bytes, byteHeaders.length, byteProperties.length);
		System.arraycopy(byteBody, 0, bytes, byteHeaders.length + byteProperties.length, byteBody.length);
		return bytes;
	}

	public static Message bytesToMessage(byte[] bytes) {
		// TODO 添加数据解压缩
		return null;
	}

	// 利用 MappedBuffer 读 message
	public Message pullMessage(String bucket) {
		// Step 1: 读 Index
		int offsetInIndexFile = offsets.getOrDefault(bucket, Integer.valueOf(0));
		FileChannel indexFileChannel = getIndexFileChannel(bucket);
		byte[] index = readIndexFileBuffer.read(bucket, indexFileChannel, offsetInIndexFile, Constants.INDEX_SIZE);

		// Step 2: 读 Message
		int offsetInLogFile = Utils.getInt(index, 6), messageSize = Utils.getInt(index, 10);
		FileChannel logFileChannel = getLogFileChannelByFileID(bucket, new String(index, 0, 6));
		byte[] message = readLogFileBuffer.read(bucket, logFileChannel, offsetInLogFile, messageSize);

		// TODO 修改 offset
		offsets.put(bucket, offsetInIndexFile + 30);
		return bytesToMessage(message);
	}

	public FileChannel getIndexFileChannel(String bucket) {
		return CommitLogHandler.getCommitLogByName(path, bucket).getIndexFileChannel();
	}

	public FileChannel getLogFileChannelByFileID(String bucket, String fileID) {
		return CommitLogHandler.getCommitLogByName(path, bucket).getLogFileChannelByFileID(fileID);
	}

}
