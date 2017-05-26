package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

public class MessageStore {

	private String path;

	// for Producer
	private ByteBuffer KVToBytesBuffer = ByteBuffer.allocate(2 * 1024 * 1024);

	// for Consumer
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

	// defaultKeyValueToBytes 的另一种解决方案
	public byte[] defaultKeyValueToBytes0(DefaultKeyValue kv) {
		if (kv == null) {
			return new byte[0];
		}
		Object value;
		byte[] keyBytes, stringValueBytes;
		for (String key : kv.keySet()) {
			value = kv.get(key);
			if (value instanceof Integer) {
				KVToBytesBuffer.putChar('i');
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putInt((Integer) value);
			} else if (value instanceof Long) {
				KVToBytesBuffer.putChar('l');
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putLong((Long) value);
			} else if (value instanceof Double) {
				KVToBytesBuffer.putChar('d');
				keyBytes = key.getBytes();
				KVToBytesBuffer.putInt(keyBytes.length);
				KVToBytesBuffer.put(keyBytes);
				KVToBytesBuffer.putDouble((Double) value);
			} else {
				KVToBytesBuffer.putChar('s');
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

	public static byte[] defaultKeyValueToBytes(DefaultKeyValue kv) {
		if (kv == null) {
			return new byte[0];
		}
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
		// byteBody.length
		byte[] bytes = Arrays.copyOf(Utils.intToByteArray(byteBody.length),
				3 * 4 + byteBody.length + byteHeaders.length + byteProperties.length);
		// byteBody
		System.arraycopy(byteBody, 0, bytes, 4, byteBody.length);
		// byteHeaders.length
		Utils.intToByteArray(byteHeaders.length, bytes, 4 + byteBody.length);
		// byteHeaders
		System.arraycopy(byteHeaders, 0, bytes, 2 * 4 + byteBody.length, byteHeaders.length);
		// byteProperties.length
		Utils.intToByteArray(byteProperties.length, bytes, 2 * 4 + byteBody.length + byteHeaders.length);
		// byteProperties
		System.arraycopy(byteProperties, 0, bytes, 3 * 4 + byteBody.length + byteHeaders.length, byteProperties.length);
		return bytes;
	}

	public static DefaultKeyValue bytesToDefaultKeyValue(DefaultKeyValue kv, byte[] kvBytes, int offset, int length) {
		return kv;
	}

	public static Message bytesToMessage(byte[] bytes) {
		// TODO 添加数据解压缩
		// byteBody.length
		int length = Utils.getInt(bytes, 0), pos = 4;
		// byteBody
		byte[] body = new byte[length];
		System.arraycopy(bytes, pos, body, 0, length);
		DefaultBytesMessage message = new DefaultBytesMessage(body);
		pos += length;
		// byteHeaders.length
		length = Utils.getInt(bytes, pos);
		pos += 4;
		// byteHeaders
		if (length != 0) {
			bytesToDefaultKeyValue((DefaultKeyValue) (message.headers()), bytes, pos, length);
		}
		pos += length;
		// byteProperties.length
		length = Utils.getInt(bytes, 4 + length);
		pos += 4;
		// byteProperties
		if (length != 0) {
			bytesToDefaultKeyValue((DefaultKeyValue) (message.properties()), bytes, pos, length);
		}
		return null;
	}

	// 利用 MappedBuffer 读 message
	public Message pullMessage(String bucket) {
		// Step 1: 读 Index
		int offsetInIndexFile = offsets.getOrDefault(bucket, Integer.valueOf(0));
		FileChannel indexFileChannel = getIndexFileChannel(bucket);
		byte[] index = readIndexFileBuffer.read(bucket, indexFileChannel, offsetInIndexFile, Constants.INDEX_SIZE);

		// Step 2: 读 Message
		int offsetInLogFile = Utils.getInt(index, Constants.OFFSET_POS),
				messageSize = Utils.getInt(index, Constants.SIZE_POS);
		FileChannel logFileChannel = getLogFileChannelByFileID(bucket,
				new String(index, Constants.FILEID_POS, Constants.OFFSET_POS - Constants.FILEID_POS));
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
