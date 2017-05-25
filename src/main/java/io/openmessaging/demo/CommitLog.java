package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class CommitLog {

	public static final long LOG_FILE_SIZE = 100 * 1024 * 1024;
	private static final int BUFFERSIZE = 20 * 1024 * 1024;
	private static final int BYTESIZE = 3* BUFFERSIZE;
	
	private static AtomicInteger shouldAppend=new AtomicInteger(0);
	private static ConcurrentHashMap<String, AtomicInteger> countFlag = new ConcurrentHashMap<>();
	
	private byte[] cirleBytes = new byte[BYTESIZE];

	private String path;
	private IndexFile indexFile = null;
	private CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();

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
			logFileList.add(new LogFile(path, logFileName, LOG_FILE_SIZE));
		}
	}

	public LogFile getLastLogFile() {
		LogFile logFileLast = null;
		// 可能会有异常
		if (!logFileList.isEmpty()) {
			logFileLast = logFileList.get(logFileList.size() - 1);

		}
		return logFileLast;
	}

	public void getNewLogFile(String fileName) {
		logFileList.add(new LogFile(path, "LOG"+fileName, LOG_FILE_SIZE));
	}

	// TODO 考虑用 nio 的 Scatter/Gather 重构 写 indexFile 写 LogFile
	public void appendMessage(byte[] messages) {
		int size = messages.length;
		
		//appendIndex是否返回Name待定
		String afterIndex = indexFile.appendIndex(size);
		String[] split = afterIndex.split(":");
		String logName = split[0];
		int offset = Integer.valueOf(split[1]);
		for (int i = offset; i <= offset + size; i++) {
		
		}
//			int index=i%BYTESIZE;
//			//TODO 合并提交方法
//			//提交第一部分
//			if(index==0 && i!=offset){
//				while(countFlag.get(0).get()==HALFBYTESIZE){
//					countFlag.get(0).set(0);
//					if(offset==0){
////						logFileList.get(logFileList.size()-1).getFileName();
//						getNewLogFile(logName);
//					}
//					logFileList.get(logFileList.size()-1).doAppend(cirleBytes,0,HALFBYTESIZE);
//				}
//			}
//			//提交第二部分
//			if(index==HALFBYTESIZE && (i/BYTESIZE)>0){
//				while(countFlag.get(1).get()==HALFBYTESIZE){
//					countFlag.get(1).set(0);
//					
//				}
//			}
//			else if(index>0&& index<BYTESIZE/2){
//				countFlag.get(0).incrementAndGet();
//			}
//			else if(index>=BYTESIZE/2){
//				countFlag.get(1).incrementAndGet();
//			}
//			cirleBytes[index] = messages[i - offset];
//		}

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

	public void putMessage(int size) {
		LogFile lastLogFile = getLastLogFile();

		indexFile.appendIndex(size);
	}

	public FileChannel getIndexFileChannel() {
		// TODO
		return null;
	}
}
