package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * READ ONLY MappedByteBuffer Wrapper for Consumer
 */
// 仅用作 Consumer 的私有属性, 且对文件只读, 不会有竞争
public class ReadBuffer {

	private final int type; // 0 for index, 1 for log
	private final int bufferSize;

	private Topic topic = null;
	private MappedByteBuffer buffer;
	private long offsetInFile; // 映射区的末尾在源文件中的 offset
	private int size;

	public ReadBuffer(int type) {
		this.type = type;
		if (type == Constants.INDEX_TYPE) { // index
			bufferSize = Constants.INDEX_BUFFER_SIZE;
		} else { // log
			bufferSize = Constants.LOG_BUFFER_SIZE;
		}
	}

	/**
	 * return false when no more file content to map.
	 */
	public boolean reMap(Topic topic, long offset) {
		// get FileChannel
		FileChannel tmpFileChannel;
		if (type == Constants.INDEX_TYPE) { // index
			tmpFileChannel = topic.getIndexFile().getFileChannel();
		} else {
			tmpFileChannel = topic.getLogFile().getFileChannel();
		}
		try {
			int remain = (int) (tmpFileChannel.size() - offset);
			int size = remain < bufferSize ? remain : bufferSize;
			if (size != 0) {
				// TODO 释放更快？待测
				// if (buffer != null)
				// BufferUtils.clean(buffer);
				buffer = tmpFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size);
				offsetInFile = offset + size;
				this.size = size;
				this.topic = topic; // 最后更新它
				return true;
			} else {
				// 只有在 offset 映射到最后一个文件, 且文件不足 Constants.FILE_SIZE, 且
				// tmpFileChannel.size() = offset 时, 才会 size = 0
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean reMap() {
		return reMap(topic, offsetInFile);
	}

	public boolean reMap(long offset) {
		return reMap(topic, offset);
	}

	/**
	 * @return null when error
	 */
	public byte[] read(Topic topic, long offset, int length) {
		// readIndexFileBuffer 缓存不命中
		if (this.topic != topic) {
			// 1. 不是同一个 topic
			if (!reMap(topic, offset))
				return null; // no more to map == no more new record
		} else if (offset >= offsetInFile || offset < offsetInFile - size) {
			// 2. 超出映射范围
			if (!reMap(offset))
				return null; // no more to map == no more new record
		}

		byte[] result = new byte[length];
		if (length > offsetInFile - offset) { // 读两段
			int size1 = (int) (offsetInFile - offset);
			buffer.get(result, 0, size1);
			if (!reMap())
				return null; // ERROR 文件损坏？
			buffer.get(result, size1, length - size1);
		} else { // other, 正常读
			if (buffer.remaining() < length)
				return null; // ERROR 文件损坏？
			buffer.get(result);
		}
		return result;
	}

	// read index file, read a int value(offset)
	public int read(Topic topic, long offset) {
		// readIndexFileBuffer 缓存不命中
		if (this.topic != topic) {
			// 1. 不是同一个 topic
			if (!reMap(topic, offset))
				return 0; // no more to map == no more new record
		} else if (offset >= offsetInFile || offset < offsetInFile - size) {
			// 2. 超出映射范围
			if (!reMap(offset))
				return 0; // no more to map == no more new record
		}
		return buffer.getInt();
	}

}
