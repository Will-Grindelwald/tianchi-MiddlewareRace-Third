package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class CommitLog {

	private String path;
	private IndexFile indexFile = null;
	private CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();

	private AtomicInteger shouldAppend = new AtomicInteger(0);
	private AtomicInteger logFileOffset = new AtomicInteger(0);
	
	private ConcurrentHashMap<Integer, AtomicInteger> countFlag = new ConcurrentHashMap<>();
	private byte[] loopBytes = new byte[Constants.BYTE_SIZE];

	public CommitLog(String path) {
		this.path = path;
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		this.indexFile = new IndexFile(path, "indexFile");
		for (String logFileName : file.list((dir, name) -> name.startsWith("LOG"))) {
			logFileList.add(new LogFile(path, logFileName));
		}
		for (int i = 0; i < 3; i++) {
			countFlag.put(i, new AtomicInteger(0));
		}

	}

	public LogFile getLastLogFile() {
		LogFile logFileLast = null;
		// 可能会有异常
		
		if (!logFileList.isEmpty()) {
			logFileLast = logFileList.get(logFileList.size() - 1);

		}
		else {
			logFileLast=new LogFile(path, "LOG" + "000000");
			logFileList.add(logFileLast);
		}
		return logFileLast;
	}

	public void getNewLogFile(String fileName) {
		logFileList.add(new LogFile(path, "LOG" + fileName));
	}

	public void appendMessage(byte[] messages) {
		int size = messages.length;
		// appendIndex是否返回Name待定
		String afterIndex = indexFile.appendIndex(size);
		String[] split = afterIndex.split(":");
		String logName = split[0];
		int offset = Integer.valueOf(split[1]);
		

		for (int i = offset; i < offset + size; i++) {
			int index = i % Constants.BYTE_SIZE;
			int appendId = shouldAppend.get();
			if (index == appendId * Constants.BUFFER_SIZE && (i / Constants.BYTE_SIZE) > 0) {
				appendMessage(loopBytes, logName);
			}

			else if (index >= 0 && index < Constants.BUFFER_SIZE) {
				countFlag.get(0).incrementAndGet();
			} else if (index >= Constants.BUFFER_SIZE && index < 2 * Constants.BUFFER_SIZE) {
				countFlag.get(1).incrementAndGet();
			} else if (index >= 2 * Constants.BUFFER_SIZE && index < Constants.BYTE_SIZE) {
				countFlag.get(2).incrementAndGet();
			}
			loopBytes[index] = messages[i - offset];

		}
//		appendMessage(loopBytes, offset);

	}

	private void appendMessage(byte[] messages, String logName) {
		int appendId = shouldAppend.get();
//		while (countFlag.get(appendId).get() != Constants.BUFFER_SIZE) {
//				System.out.print(Thread.currentThread().getName()+" ");
//				System.out.print(appendId+" ");
//				System.out.println(countFlag.get(appendId).get());
//		}
		while (countFlag.get(appendId).get() >= Constants.BUFFER_SIZE)
		{
			if (appendId == 2) {
				shouldAppend.set(0);
			} else {
				shouldAppend.incrementAndGet();
			}
			countFlag.get(appendId).set(0);
			
//			if(!getLastLogFile().getFileName().equals(logName)){
//				getNewLogFile(logName);
//			}
//			if (offset == 0) {
//				String name = getLastLogFile().getFileName();
//				getNewLogFile(name);
//			}
			if(logFileOffset.get()>4){
				getNewLogFile(logName);
				System.out.println(logName);
				logFileOffset.set(0);
			}
			else {
				logFileOffset.incrementAndGet();
			}
			
			getLastLogFile().doAppend(loopBytes);
		}
	}

	public void flush() {

	}

	public void getLogFileByOffset() {

	}

	public void getMessage() {

	}

	public Message getNewMessage(long offset) {
		byte[] index = indexFile.readIndexByOffset(offset);

		return null;
	}

	public FileChannel getIndexFileChannel() {
		// TODO
		return null;
	}

	public FileChannel getLogFileChannelByFileID(String fileID) {
		// TODO
		return null;
	}
}
