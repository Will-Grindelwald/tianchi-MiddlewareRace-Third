package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * READ WRITE MappedByteBuffer Wrapper for Producer. 写缓存系统.
 */
public class WriteBuffer3 {

	private final String fileNamePrefix;

	private int fileID;
	// 当前映射块在整个 topic 中的 块号
	private final AtomicInteger blockNumber;
	private final List<PersistenceFile> fileList;
	private FileChannel mappedFileChannel = null;
	private MappedByteBuffer buffer;
	private boolean bufferNotFull; // 与 reMap 同步
	private final AtomicInteger bufferWrited;

	private final ReentrantLock bufferLock = new ReentrantLock();
	private final Condition bufferEmpty = bufferLock.newCondition();
	private final Condition bufferBlockNumber = bufferLock.newCondition();

	// for init
	private volatile boolean open = false;
	// for close
	private volatile boolean close = false;

	// 不带二级缓存, 第四个参数没用
	public WriteBuffer3(String fileNamePrefix, List<PersistenceFile> fileList, long offset) {
		this.fileNamePrefix = fileNamePrefix;
		this.fileList = fileList;
		fileID = (int) (offset / Constants.FILE_SIZE);
		blockNumber = new AtomicInteger((int) (offset / Constants.BUFFER_SIZE));
		bufferWrited = new AtomicInteger((int) offset);
	}

	public void init() {
		// get FileChannel
		for (PersistenceFile file : fileList) {
			if (file.fileID == fileID) {
				mappedFileChannel = file.getFileChannel();
				break;
			}
		}
		if (mappedFileChannel == null) {
			System.err.println("ERROR PersistenceFile 丢失");
			new Exception().printStackTrace();
			System.exit(0);
		}
		// init mappedBuffer
		try {
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
					blockNumber.get() % Constants.BLOCK_NUMBER * Constants.BUFFER_SIZE, Constants.BUFFER_SIZE);
			buffer.position((int) (bufferWrited.get() % Constants.BUFFER_SIZE));
			bufferNotFull = true;
			open = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * for logBuffer. 此 write 设计为`跳写` buffer. 且不会出现 bytes[] 要跨 buffer 写入,
	 * 这由外部来保证.
	 */
	public boolean write(byte[] bytes, long offset) throws InterruptedException {
		bufferLock.lock();
		if (!open)
			init();
		try {
			int targetBlockNumber = (int) (offset / Constants.BUFFER_SIZE);
			while (targetBlockNumber != blockNumber.get()) {
				// 若要写入的块非当前块, 则阻塞
				bufferBlockNumber.await();
			}
			while (!bufferNotFull) {
				// 与 reMap 同步, indexBuffer logBuffer 都需要
				bufferEmpty.await();
			}
			buffer.position((int) (offset % Constants.BUFFER_SIZE));
			buffer.put(bytes);
			if (bufferWrited.addAndGet(bytes.length) == Constants.BUFFER_SIZE) {
				bufferNotFull = false;
				bufferWrited.set(0);
				blockNumber.incrementAndGet();
				bufferBlockNumber.signalAll();
				GlobalResource.submitReMapTask(this::reMap);
			} else if (close) {
				buffer.force();
			}
			return true;
		} finally {
			bufferLock.unlock();
		}
	}

	// 将交由 GlobalResource.BufferReMapExecPool 执行
	public void reMap() {
		bufferLock.lock();
		try {
			// 1
			buffer.force();
			// 2
			BufferUtils.clean(buffer);
			// 3
			if (blockNumber.get() % Constants.BLOCK_NUMBER == 0) { // 换新文件
				PersistenceFile newFile = new PersistenceFile(fileList.get(0).path, ++fileID, fileNamePrefix);
				fileList.add(newFile);
				mappedFileChannel.force(false);
				// mappedFileChannel.close(); // 要不要关？
				mappedFileChannel = newFile.getFileChannel();
			}
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
					blockNumber.get() % Constants.BLOCK_NUMBER * Constants.BUFFER_SIZE, Constants.BUFFER_SIZE);
			// 与 commit(or write) 同步
			bufferNotFull = true;
			bufferEmpty.signalAll();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			bufferLock.unlock();
		}
	}

	public void flush() throws InterruptedException {
		bufferLock.lock();
		if (!close)
			close = true;
		try {
			buffer.force();
		} finally {
			bufferLock.unlock();
		}
	}

}
