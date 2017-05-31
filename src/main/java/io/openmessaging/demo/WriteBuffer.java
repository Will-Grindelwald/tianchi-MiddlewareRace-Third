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
 * 注意: write(byte[]) commit(byte[], int, int) reMap() 三者通过 bufferLock 互斥
 */
public class WriteBuffer {

	private final String fileNamePrefix;

	private int fileID;
	// 当前映射块在整个 topic 中的 块号
	private final AtomicInteger blockNumber;
	private final List<PersistenceFile> fileList;
	private FileChannel mappedFileChannel = null;
	private MappedByteBuffer buffer;
	private boolean bufferNotFull; // 与 reMap 同步

	// buffer 二级缓存, 实现并发写
	// 同时标记是否带有二级缓存, indexFile buffer or logFile buffer
	private final int bufferL2Size;
	private final byte[] bufferL2;
	private final AtomicInteger bufferL2Count;

	private ReentrantLock bufferLock = new ReentrantLock();
	private Condition bufferEmpty = bufferLock.newCondition();
	private Condition bufferBlockNumber = bufferLock.newCondition();

	// for close
	private volatile boolean close = false;

	public WriteBuffer(String fileNamePrefix, List<PersistenceFile> fileList, long offset, int bufferL2Size) {
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
		// init 二级 缓存
		this.bufferL2Size = bufferL2Size;
		if (bufferL2Size != 0) {
			bufferL2 = new byte[bufferL2Size];
			bufferL2Count = new AtomicInteger(buffer.position());
		} else {
			bufferL2 = null;
			bufferL2Count = null;
		}
	}

	// case 1: indexBuffer 顺序提交 不会并发
	// case 2: logBuffer 写满 bufferL2 提交 不会并发
	/**
	 * 此 write 设计为顺序写 buffer. 且不会出现 bytes[] 要跨 buffer 写入, 这由外部来保证.
	 */
	public long write(byte[] bytes) throws InterruptedException {
		bufferLock.lock();
		while (!bufferNotFull) {
			// 与 reMap 同步, indexBuffer logBuffer 都需要
			bufferEmpty.await();
		}
		buffer.put(bytes);
		long ret = (long) buffer.position() + blockNumber.get() * Constants.BUFFER_SIZE;
		if (buffer.remaining() == 0) {
			bufferNotFull = false;
			if (bufferL2Size != 0) { // logBuffer
				bufferL2Count.set(0);
				// 唤醒那些要写下一块的
				blockNumber.incrementAndGet();
				System.out.println("2blockNumber=" + blockNumber.get());
				System.out.println("2bufferL2Count=" + bufferL2Count.get());
				System.out.println(Thread.currentThread().getName());
				bufferBlockNumber.signalAll();
			} else { // indexBuffer
				blockNumber.incrementAndGet();
			}
			GlobalResource.submitReMapTask(this::reMap);
		} else if (close) {
			// only indexBuffer will
			buffer.force();
		}
		bufferLock.unlock();
		return ret;
	}

	/**
	 * for logFileBuffer. 此 write 设计为并发写 bufferL2. 且不会出现 bytes[] 要跨 bufferL2 写入,
	 * 这由外部来保证.
	 */
	public boolean write(byte[] bytes, long offset) throws InterruptedException {
		if (bufferL2Size == 0)
			return false;
		bufferLock.lock();
		int targetBlockNumber = (int) (offset / Constants.BUFFER_SIZE);
		while (targetBlockNumber != blockNumber.get()) {
			System.out.println("1targetBlockNumber=" + targetBlockNumber);
			System.out.println("1blockNumber=" + blockNumber.get());
			System.out.println("1bufferL2Count=" + bufferL2Count.get());
			System.out.println(Thread.currentThread().getName());
			// 若要写入的块非当前块, 则阻塞
			bufferBlockNumber.await();
		}
		bufferLock.unlock();
		offset %= Constants.BUFFER_SIZE;
		System.arraycopy(bytes, 0, bufferL2, (int) offset, bytes.length);
		if (bufferL2Count.addAndGet(bytes.length) == bufferL2Size) {
			// 写满 bufferL2 提交, 是唯一的, 不会并发
			System.out.println("commit" + Thread.currentThread().getName());
			write(bufferL2);
		} else if (close) {
			// 未写满 bufferL2, 因 close 提交, 会并发！
			commit(bufferL2, (int) offset, bytes.length);
		}
		return true;
	}

	// case 1: logFileBuffer 未写满 bufferL2, 因 close 提交, 会并发, 用 synchronized 同步
	// case 2: flush
	/**
	 * 将 byte[] 的一部分写到到 buffer, 并 force
	 */
	private void commit(byte[] bytes, int offset, int length) throws InterruptedException {
		bufferLock.lock();
		while (!bufferNotFull) {
			// 与 reMap 同步, indexBuffer logBuffer 都需要
			bufferEmpty.await();
		}
		if (length != 0) {
			buffer.position(offset);
			buffer.put(bytes, offset, length);
			buffer.force();
			buffer.clear();
		} else { // for flush
			buffer.force();
		}
		bufferLock.unlock();
	}

	public void reMap() {
		bufferLock.lock();
		try {
			System.out.println("c1");
			// 1
			buffer.force();
			// 2
			BufferUtils.clean(buffer);
			// 3
			if (blockNumber.get() % Constants.BLOCK_NUMBER == 0) { // 换新文件
				PersistenceFile newFile = new PersistenceFile(fileList.get(0).path, ++fileID, fileNamePrefix);
				fileList.add(newFile);
				mappedFileChannel = newFile.getFileChannel();
			}
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
					blockNumber.get() % Constants.BLOCK_NUMBER * Constants.BUFFER_SIZE, Constants.BUFFER_SIZE);
			// 与 commit(or write) 同步
			bufferNotFull = true;
			bufferEmpty.signalAll();
			System.out.println("c2");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			bufferLock.unlock();
		}
	}

	public void flush() throws InterruptedException {
		close = true;
		commit(bufferL2, 0, 0);
	}

}
