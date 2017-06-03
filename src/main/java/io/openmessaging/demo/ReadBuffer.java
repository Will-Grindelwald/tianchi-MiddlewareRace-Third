//package io.openmessaging.demo;
//
//import java.io.IOException;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.List;
//
///**
// * READ ONLY MappedByteBuffer Wrapper for Consumer
// */
//// 仅用作 Consumer 的私有属性, 且对文件只读, 不会有竞争
//public class ReadBuffer {
//
//	private final int type; // 0 for index, 1 for log
//	private final int bufferSize;
//	private final int FileSize;
//
//	private Topic topic = null;
//	private MappedByteBuffer buffer;
//	private long offsetInFile; // 映射区的末尾在源文件中的 offset
//	private int size;
//
//	public ReadBuffer(int type) {
//		this.type = type;
//		if (type == 0) { // index
//			bufferSize = Constants.INDEX_BUFFER_SIZE;
//			FileSize = Constants.INDEX_FILE_SIZE;
//		} else { // log
//			bufferSize = Constants.LOG_BUFFER_SIZE;
//			FileSize = Constants.LOG_FILE_SIZE;
//		}
//	}
//
//	/**
//	 * return false when no more file content to map.
//	 */
//	public boolean reMap(Topic topic, long offset) {
//		// get FileChannel
//		int fileID = (int) (offset / FileSize);
////		List<PersistenceFile> tmpFileList = type == 0 ? topic.getIndexFileList() : topic.getLogFileList();
//		List<PersistenceFile> tmpFileList = topic.getLogFileList();
//		FileChannel tmpFileChannel = null;
//		for (PersistenceFile file : tmpFileList) {
//			if (file.fileID == fileID) {
//				tmpFileChannel = file.getFileChannel();
//				break;
//			}
//		}
//		if (tmpFileChannel == null) {
//			System.err.println("ERROR PersistenceFile 丢失");
//			System.err.println("offset=" + offset); /// test
//			System.err.println("fileID=" + fileID); /// test
//			new Exception().printStackTrace();
//			System.exit(0);
//		}
//		try {
//			int remain = (int) (tmpFileChannel.size() - offset % FileSize);
//			int size = remain < bufferSize ? remain : bufferSize;
//			if (size != 0) {
//				// TODO 释放更快？待测
//				// if (buffer != null)
//				// BufferUtils.clean(buffer);
//				buffer = tmpFileChannel.map(FileChannel.MapMode.READ_ONLY, offset % FileSize, size);
//				offsetInFile = offset + size;
//				this.size = size;
//				this.topic = topic; // 最后更新它
//				return true;
//			} else {
//				// 只有在 offset 映射到最后一个文件, 且文件不足 Constants.FILE_SIZE, 且
//				// tmpFileChannel.size() = offset 时, 才会 size = 0
//				return false;
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return false;
//	}
//
//	public boolean reMap() {
//		return reMap(topic, offsetInFile);
//	}
//
//	public boolean reMap(long offset) {
//		return reMap(topic, offset);
//	}
//
//	/**
//	 * @return null when no more new record
//	 */
//	public byte[] read(Topic topic, long offset, int length) {
//		// readIndexFileBuffer 缓存不命中
//		if (this.topic != topic) {
//			// 1. 不是同一个 topic
//			if (!reMap(topic, offset))
//				return null; // no more to map == no more new record
//			// buffer.load(); // TODO 测试 load 与 不 load 谁快
//		} else if (offset >= offsetInFile || offset < offsetInFile - size) {
//			// 2. 超出映射范围
//			if (!reMap(offset))
//				return null; // no more to map == no more new record
//			// buffer.load();
//		}
//		if (type == 0) {
//			if (offset >= topic.getNextIndexOffset())
//				return null;
//		}
//
//		byte[] result = new byte[length];
//		if (length > offsetInFile - offset) { // 读两段
//			int size1 = (int) (offsetInFile - offset);
//			buffer.get(result, 0, size1);
//			if (!reMap())
//				return null; // ERROR 文件损坏？
//			buffer.get(result, size1, length - size1);
//		} else { // other, 正常读
//			if (buffer.remaining() < length)
//				return null; // ERROR 文件损坏？
//			buffer.get(result);
//		}
//		return result;
//	}
//
//}
