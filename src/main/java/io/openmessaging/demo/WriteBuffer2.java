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
public class WriteBuffer2 {

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

	// for close
	private volatile boolean close = false;

	// 不带二级缓存, 第四个参数没用
	public WriteBuffer2(String fileNamePrefix, List<PersistenceFile> fileList, long offset, int bufferL2Size) {
		this.fileNamePrefix = fileNamePrefix;
		this.fileList = fileList;
		fileID = (int) (offset / Constants.FILE_SIZE);
		blockNumber = new AtomicInteger((int) (offset / Constants.BUFFER_SIZE));
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
			buffer.position((int) (offset % Constants.BUFFER_SIZE));
			bufferNotFull = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		bufferWrited = new AtomicInteger(buffer.position()); // indexBuffer 用不到
	}

	/**
	 * for indexBuffer. 此 write 设计为`顺序写` buffer. 且不会出现 bytes[] 要跨 buffer 写入,
	 * 这由外部来保证.
	 */
	public boolean write(byte[] bytes) throws InterruptedException {
		bufferLock.lock();
		try {
			while (!bufferNotFull) {
				// 与 reMap 同步, indexBuffer logBuffer 都需要
				bufferEmpty.await();
			}
			buffer.put(bytes);
			if (buffer.remaining() == 0) {
				bufferNotFull = false;
				blockNumber.incrementAndGet();
				GlobalResource.submitReMapTask(this::reMap);
			} else if (close) {
				flush();
			}
			return true;
		} finally {
			bufferLock.unlock();
		}
	}

	/**
	 * for logBuffer. 此 write 设计为`跳写` buffer. 且不会出现 bytes[] 要跨 buffer 写入,
	 * 这由外部来保证.
	 */
	public boolean write(byte[] bytes, long offset) throws InterruptedException {
		bufferLock.lock();
		try {
			int targetBlockNumber = (int) (offset / Constants.BUFFER_SIZE);
			while (targetBlockNumber != blockNumber.get()) {
				// 若要写入的块非当前块, 则阻塞
				System.out.println("1targetBlockNumber=" + targetBlockNumber); //// test
				System.out.println("1blockNumber=" + blockNumber); //// test
				System.out.println("1bufferWrited=" + bufferWrited.get()); //// test
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
				System.out.println("2blockNumber=" + blockNumber); //// test
				System.out.println("2bufferWrited=" + bufferWrited.get()); //// test
				bufferWrited.set(0);
				blockNumber.incrementAndGet();
				System.out.println("3blockNumber=" + blockNumber); //// test
				System.out.println("3bufferWrited=" + bufferWrited.get()); //// test
				bufferBlockNumber.signalAll();
				GlobalResource.submitReMapTask(this::reMap);
			} else if (close) {
				flush();
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
			System.out.println("c1"); //// test
			// 1
			buffer.force();
			// 2
			BufferUtils.clean(buffer);
			// 3
			if (blockNumber.get() % Constants.BLOCK_NUMBER == 0) { // 换新文件
				PersistenceFile newFile = new PersistenceFile(fileList.get(0).path, ++fileID, fileNamePrefix);
				fileList.add(newFile);
				mappedFileChannel.force(false);
				mappedFileChannel.close();
				mappedFileChannel = newFile.getFileChannel();
			}
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
					blockNumber.get() % Constants.BLOCK_NUMBER * Constants.BUFFER_SIZE, Constants.BUFFER_SIZE);
			// 与 commit(or write) 同步
			bufferNotFull = true;
			bufferEmpty.signalAll();
			System.out.println("c2"); //// test
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
