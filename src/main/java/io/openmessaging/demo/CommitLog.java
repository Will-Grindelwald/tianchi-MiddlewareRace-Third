package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// 全局唯一, 小心并发
public class CommitLog {

	private final String path;
	private final IndexFile indexFile;
	private final CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();

	private AtomicInteger shouldAppend = new AtomicInteger(0);
	private AtomicInteger logFileOffset = new AtomicInteger(0);
	private ConcurrentHashMap<Integer, AtomicInteger> countFlag = new ConcurrentHashMap<>();
	private byte[] loopBytes = new byte[Constants.BYTE_SIZE];
	
	private String writeName="000000";

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

	// for Producer
	public LogFile getLastLogFile() {
		LogFile logFileLast = null;
		// 可能会有异常

		if (!logFileList.isEmpty()) {
			logFileLast = logFileList.get(logFileList.size() - 1);

		} else {
			logFileLast = new LogFile(path, "LOG" + "000000");
			logFileList.add(logFileLast);
		}
		return logFileLast;
	}

	// for Producer
	public LogFile getNewLogFile(String fileName) {
		LogFile newFile=new LogFile(path, "LOG" + fileName);
		logFileList.add(newFile);
		return newFile;
	}
	public void appendMessage(byte[] messages){
		int size = messages.length;
		// appendIndex是否返回Name待定
		String afterIndex = indexFile.appendIndex(size);
		String[] split = afterIndex.split(":");
		String logName = split[0];
		System.out.println("a"+logName);
		int offset = Integer.valueOf(split[1]);
		if(offset==0 && logFileOffset.get()>0){
			flush(loopBytes);
			shouldAppend.set(0);
			logFileOffset.set(0);
		}
		if(offset==0){
			writeName=logName;
		}
		while(offset>=(Constants.BYTE_SIZE)){
			offset-=(Constants.BYTE_SIZE);
			
		}
		int sum=offset+size;
		if( (offset<Constants.BUFFER_SIZE) && (sum>Constants.BUFFER_SIZE)){
			if(logFileOffset.get()>0){
				while(countFlag.get(1).get()<Constants.BUFFER_SIZE){
					//等待
					System.out.println("wait1...");
				}
				appendMessage(loopBytes,writeName);
				countFlag.get(1).set(0);
				shouldAppend.incrementAndGet();
			}
			int first=Constants.BUFFER_SIZE-offset;
			int last=offset+size-Constants.BUFFER_SIZE;
			countFlag.get(0).addAndGet(first);
			countFlag.get(1).addAndGet(last);
			System.out.print(offset+" "+logFileOffset.get()+" ");
			System.out.println(size);
			System.arraycopy(messages, 0 ,loopBytes, offset, size);
		}
		else if( (offset<2*Constants.BUFFER_SIZE) && (sum>2*Constants.BUFFER_SIZE)){
			if(logFileOffset.get()>0){
				while(countFlag.get(2).get()<Constants.BUFFER_SIZE){
					//等待
					System.out.println("wait2...");
				}
				appendMessage(loopBytes,writeName);
				countFlag.get(2).set(0);
				shouldAppend.set(0);
			}
			
			int first=2*Constants.BUFFER_SIZE-offset;
			int last=offset+size-2*Constants.BUFFER_SIZE;
			countFlag.get(1).addAndGet(first);
			countFlag.get(2).addAndGet(last);
			System.arraycopy(messages, 0 ,loopBytes, offset, size);
		
		}
		else if( (offset<Constants.BYTE_SIZE) && (sum>Constants.BYTE_SIZE)){
			while(countFlag.get(0).get()<Constants.BUFFER_SIZE){
				//等待
				System.out.println(countFlag.get(0).get());
				System.out.println("wait0...");
			}
			//提交第一部分
			System.out.println("aa");
			System.out.println(sum);
			appendMessage(loopBytes,writeName);
			countFlag.get(0).set(0);
			shouldAppend.incrementAndGet();
			logFileOffset.incrementAndGet();
			int first=Constants.BYTE_SIZE-offset;
			int last=sum-Constants.BYTE_SIZE;
			countFlag.get(2).addAndGet(first);
			countFlag.get(0).addAndGet(last);
			System.arraycopy(messages, 0 ,loopBytes, offset, first);
			System.arraycopy(messages, first, loopBytes, 0, last);
			
		
		}
		else {
			if(sum<Constants.BUFFER_SIZE){
				countFlag.get(0).addAndGet(size);
			}
			else if(sum<=2*Constants.BUFFER_SIZE) {
				countFlag.get(1).addAndGet(size);
			}
			else if(sum<=Constants.BYTE_SIZE){
				countFlag.get(2).addAndGet(size);
			}

			System.out.print(offset+" "+logFileOffset.get()+" ");
			System.out.println(size);
			System.arraycopy(messages, 0 ,loopBytes, offset, size);
		}

		
		
	}

	//最后剩余的不足一个BufferSize大小的通过最后的刷盘写入
	// for Producer
//	public void appendMessage(byte[] messages) {
//		int size = messages.length;
//		// appendIndex是否返回Name待定
//		String afterIndex = indexFile.appendIndex(size);
//		String[] split = afterIndex.split(":");
//		String logName = split[0];
//		System.out.println("a"+logName);
//		int offset = Integer.valueOf(split[1]);
//		if(offset==0){
//			writeName=logName;
//		}
//		for (int i = offset; i < offset + size; i++) {
//			int index = i % Constants.BYTE_SIZE;
//			int appendId = shouldAppend.get();
//			if (index == appendId * Constants.BUFFER_SIZE && (i / Constants.BYTE_SIZE) > 0) {
//				appendMessage(loopBytes, writeName);
//			}
//
//			else if (index >= 0 && index < Constants.BUFFER_SIZE) {
//				countFlag.get(0).incrementAndGet();
//			} else if (index >= Constants.BUFFER_SIZE && index < 2 * Constants.BUFFER_SIZE) {
//				countFlag.get(1).incrementAndGet();
//			} else if (index >= 2 * Constants.BUFFER_SIZE && index < Constants.BYTE_SIZE) {
//				countFlag.get(2).incrementAndGet();
//			}
//			loopBytes[index] = messages[i - offset];
//
//		}
////		appendMessage(loopBytes, offset);
//
//	}
	//按照块大小写
//	public void appendMessage(byte[] messages){
//		int size = messages.length;
//		String afterIndex = indexFile.appendIndex(size);
//		String[] split = afterIndex.split(":");
//		String logName = split[0];
//		System.out.println("a"+logName);
//		int offset = Integer.valueOf(split[1]);
//		if(offset==0){
//			writeName=logName;
//		}
//		int loopCount=logFileOffset.get();
//		if(offset+size>= loopCount*Constants.BYTE_SIZE){
//			int appendId = shouldAppend.get();
//			while(countFlag.get(appendId).get() < Constants.BUFFER_SIZE){
//				System.out.println(countFlag.get(appendId).get());
//			}
//			while(countFlag.get(appendId).get() >= Constants.BUFFER_SIZE){
//				shouldAppend.updateAndGet(x->(x==2?0:++x));
////				if (appendId == 2) {
////					shouldAppend.set(0);
////				} else {
////					shouldAppend.incrementAndGet();
////				}
//				countFlag.get(appendId).set(0);
//				LogFile willWrite=getLastLogFile();
//				if(!(willWrite.getFileName().equals("LOG"+writeName))){
//					willWrite=getNewLogFile(writeName);
//				}
//				willWrite.doAppend(loopBytes);
//				appendId = shouldAppend.get();
//				
//			}
//			int last=loopCount*Constants.BYTE_SIZE-offset;
//			if(last<=Constants.BUFFER_SIZE){
//				countFlag.get(0).addAndGet(size);
//			}
//			else if(last<=2*Constants.BUFFER_SIZE) {
//				countFlag.get(1).addAndGet(size);
//			}
//			else if(last<=Constants.BYTE_SIZE){
//				countFlag.get(2).addAndGet(size);
//			}
//			System.arraycopy(messages, 0, loopBytes, offset, last);
//			System.arraycopy(messages, last, loopBytes, 0, offset+size-loopCount*Constants.BYTE_SIZE);
//			logFileOffset.incrementAndGet();
//		}
//		else{
//			if(offset<=Constants.BUFFER_SIZE){
//				countFlag.get(0).addAndGet(size);
//			}
//			else if(offset<=2*Constants.BUFFER_SIZE) {
//				countFlag.get(1).addAndGet(size);
//			}
//			else if(offset<=Constants.BYTE_SIZE){
//				countFlag.get(2).addAndGet(size);
//			}
//			System.arraycopy(messages, 0 ,loopBytes, offset, size);
//		}
//		
//	}
	
	

	// for Producer
	private void appendMessage(byte[] messages, String logName) {
		
		LogFile willWrite=getLastLogFile();
		if(!(willWrite.getFileName().equals("LOG"+logName))){
			willWrite=getNewLogFile(logName);
		}
		willWrite.doAppend(messages);
	}

	// for Consumer
	public FileChannel getIndexFileChannel() {
		return indexFile.getFileChannel();
	}

	// for Consumer
	public FileChannel getLogFileChannelByFileID(String fileID) {
		String fileName = "LOG" + fileID;
		for (LogFile logFile : logFileList) {
			if(fileName.equals(logFile.getFileName()))
				return logFile.getFileChannel();
		}
		// twins loop for ...
		for (LogFile logFile : logFileList) {
			if(fileName.equals(logFile.getFileName()))
				return logFile.getFileChannel();
		}
		return null; // ERROR 文件丢失？
	}

	// for Producer
	public void flush(byte[] messages) {
		int start=shouldAppend.get();
		while(countFlag.get(start).get()<=Constants.BUFFER_SIZE && countFlag.get(start).get()>0){
			appendMessage(messages,writeName);
			countFlag.get(start).set(0);
			start=shouldAppend.updateAndGet(x->x==2?0:++x);
		}
	}
}
