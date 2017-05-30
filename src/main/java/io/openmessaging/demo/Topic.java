package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

// 一个 buchet 一个, 全局唯一, 小心并发
public class Topic {

	public final String bucket;
	private final String path;

	// Last file
	private final LastFile lastFile;
	// close flag for lastFile
	private boolean close = false;

	// IndexFile
	private final CopyOnWriteArrayList<PersistenceFile> indexFileList = new CopyOnWriteArrayList<>();
	private WriteBuffer writeIndexFileBuffer;

	// LogFiles
	private final CopyOnWriteArrayList<PersistenceFile> logFileList = new CopyOnWriteArrayList<>();
	private WriteBuffer writeLogFileBuffer;

	private final LinkedBlockingQueue<byte[]> messageBlockQueue = new LinkedBlockingQueue<>();
	// 伴生线程
	private Thread registerIndexService;

	public Topic(String bucket) {
		this.bucket = bucket;
		path = System.getProperty("path") + "/" + bucket;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		// Last file
		lastFile = new LastFile(path);
		// indexFile
		int tmpFileID;
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.INDEX_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.INDEX_FILE_PREFIX.length()));
			} catch (NumberFormatException e) {
				System.err.println("indexFile name 错误");
				continue;
			}
			indexFileList.add(new PersistenceFile(path, tmpFileID, Constants.INDEX_FILE_PREFIX));
		}
		if (indexFileList.isEmpty()) {
			indexFileList.add(new PersistenceFile(path, 0, Constants.INDEX_FILE_PREFIX));
		}
		writeIndexFileBuffer = new WriteBuffer(Constants.INDEX_FILE_PREFIX, indexFileList, lastFile.nextIndexOffset, 0);
		// LogFiles
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.LOG_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.LOG_FILE_PREFIX.length()));
			} catch (NumberFormatException e) {
				System.err.println("logFile name 错误");
				continue;
			}
			logFileList.add(new PersistenceFile(path, tmpFileID, Constants.LOG_FILE_PREFIX));
		}
		if (logFileList.isEmpty()) {
			logFileList.add(new PersistenceFile(path, 0, Constants.LOG_FILE_PREFIX));
		}
		writeLogFileBuffer = new WriteBuffer(Constants.LOG_FILE_PREFIX, logFileList, lastFile.nextMessageOffset,
				Constants.BUFFER_SIZE);
		registerIndexService = new Thread(new RegisterIndexService(this));
		registerIndexService.start();
	}

	// for Producer
	public void putMessageToQueue(byte[] byteMessage) {
		try {
			// 因为使用的是无界阻塞队列, 理应不会阻塞太久
			messageBlockQueue.put(byteMessage);
//			if (registerIndexService == null) {
//				// 没有并发
//				registerIndexService = new Thread(new RegisterIndexService(this));
//				registerIndexService.start();
//			}
			System.out.println("messageQueue入" + messageBlockQueue.size()); //// test
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// for WriteMessageService
	public void appendMessageBytes(byte[] bytes, long offset) throws InterruptedException {
		writeLogFileBuffer.write(bytes, offset);
	}

	// for RegisterIndexService
	public byte[] takeMessageFromQueue() throws InterruptedException {
		System.out.println("messageQueue出" + (messageBlockQueue.size() - 1)); //// test
		return messageBlockQueue.take();
	}

	// for RegisterIndexService
	public long appendIndex(int size) throws InterruptedException {
		long newOffset = lastFile.nextMessageOffset;
		Index.setOffset(lastFile.lastIndexByte, lastFile.nextMessageOffset);
		Index.setSize(lastFile.lastIndexByte, size);
		lastFile.nextIndexOffset += writeIndexFileBuffer.write(lastFile.lastIndexByte);
		lastFile.nextMessageOffset += size;
		if (close) {
			lastFile.flush();
		}
		return newOffset;
	}

	// for Consumer
	public FileChannel getIndexFileChannelByOffset(long offset) {
		int fileID = (int) (offset % Constants.FILE_SIZE);
		for (PersistenceFile indexFile : indexFileList) {
			if (indexFile.fileID == fileID)
				return indexFile.getFileChannel();
		}
		// twins for ...
		for (PersistenceFile indexFile : indexFileList) {
			if (indexFile.fileID == fileID)
				return indexFile.getFileChannel();
		}
		return null; // 文件丢失??
	}

	// for Consumer
	public FileChannel getLogFileChannelByOffset(long offset) {
		int fileID = (int) (offset % Constants.FILE_SIZE);
		for (PersistenceFile indexFile : indexFileList) {
			if (indexFile.fileID == fileID)
				return indexFile.getFileChannel();
		}
		// twins for ...
		for (PersistenceFile indexFile : indexFileList) {
			if (indexFile.fileID == fileID)
				return indexFile.getFileChannel();
		}
		return null; // 文件丢失??
	}

	public int getBlockingMessageNumber() {
		return messageBlockQueue.size();
	}

	// for Producer
	public void flush() throws InterruptedException {
		close = true;
		// 1. update lastIndex
		lastFile.flush();
		// 2. flush writeIndexFileBuffer
		writeIndexFileBuffer.flush();
		// 3. flush writeLogFileBuffer
		writeLogFileBuffer.flush();
	}

}

class RegisterIndexService implements Runnable {
	private Topic bindTopic;

	public RegisterIndexService(Topic topic) {
		bindTopic = topic;
	}

	@Override
	public void run() {
		byte[] messageByte = null;
		while (true) {
			try {
				// 1
				if (messageByte == null) {
					messageByte = bindTopic.takeMessageFromQueue();
				}
				// 2
				long newOffset = bindTopic.appendIndex(messageByte.length);
				// 3
				long lastByteOffset = newOffset + messageByte.length - 1;
				// 跨 buffer 的, 分为两个放入 Queue
				if (newOffset / Constants.BUFFER_SIZE != lastByteOffset / Constants.BUFFER_SIZE) {
					int size1 = (int) (Constants.BUFFER_SIZE - newOffset % Constants.BUFFER_SIZE);
					byte[] part1 = new byte[size1], part2 = new byte[messageByte.length - size1];
					System.arraycopy(messageByte, 0, part1, 0, size1);
					System.arraycopy(messageByte, size1, part2, 0, part2.length);
					GlobalResource.WriteTaskBlockQueue.put(new WriteTask(part1, bindTopic.bucket, newOffset));
					GlobalResource.WriteTaskBlockQueue.put(new WriteTask(part2, bindTopic.bucket, newOffset + size1));
					System.out.println("WriteQueue2入" + GlobalResource.WriteTaskBlockQueue.size()); //// test
					// System.out.println("1a"); //// test
				} else {
					GlobalResource.WriteTaskBlockQueue.put(new WriteTask(messageByte, bindTopic.bucket, newOffset));
					System.out.println("WriteQueue1入" + GlobalResource.WriteTaskBlockQueue.size()); //// test
					// System.out.println("2a"); //// test
				}
				messageByte = null;
			} catch (Exception e) {
				// } catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
