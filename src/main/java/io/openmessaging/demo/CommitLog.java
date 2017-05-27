package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// 一个 buchet 一个, 全局唯一, 小心并发
public class CommitLog {

	private final String path;
	private final IndexFile indexFile;
	private final CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();
	private final RandomAccessFile lastFile;

	private final AtomicInteger shouldAppend = new AtomicInteger(0);
//	private final AtomicInteger logFileOffset = new AtomicInteger(0);
	private ReentrantLock fileWriteLock = new ReentrantLock();
	private ConcurrentHashMap<Integer, AtomicInteger> countFlag = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Integer> shouldWirteName = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Integer> logFileOffset  = new ConcurrentHashMap<>();

	
	private byte[] loopBytes = new byte[Constants.BYTE_SIZE];
	
	private volatile int lastFileId;
	
	// 用于 flush
	private Index lastIndex;

	public CommitLog(String path) {
		this.path = path;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		// Last file
		File last = new File(path, "Last");
		try {
			if (!last.exists()) {
				last.createNewFile();
				lastFile = new RandomAccessFile(last, "rw");
				lastIndex = new Index(0, 0, 0);
			} else {
				lastFile = new RandomAccessFile(last, "rw");
				if(last.length() != Constants.INDEX_SIZE) {
					lastIndex = new Index(0, 0, 0);
				} else {
					int fileID = lastFile.readInt();
					int offset = lastFile.readInt();
					int size = lastFile.readInt();
					lastIndex = new Index(fileID, offset, size);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new ClientOMSException("Last create failure", e);
		}
		// indexFile
		indexFile = new IndexFile(path, "indexFile", lastIndex.size);
		// LogFiles
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
			logFileLast = new LogFile(path, "LOG" + "0");
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
	public void  appendMessage(byte[] messages){
//		fileWriteLock.lock();
//		int size = messages.length;
//		// appendIndex是否返回Name待定
//		String afterIndex = indexFile.appendIndex(size);
//		String[] split = afterIndex.split(":");
//		String logName = split[0];
//		int offset = Integer.valueOf(split[1]);
//		System.out.println("should write "+logName+"offset:"+offset);
//
//		if(offset==0 && logFileOffset.get()>0){	
//			commit(loopBytes);
//			shouldAppend.set(0);
//			logFileOffset.set(0);
//		}
//		if(offset==0){
//			writeName=logName;
//		}
//		while(offset>=(Constants.BYTE_SIZE)){
//			offset-=(Constants.BYTE_SIZE);
//		}
//		int sum=offset+size;
//		
//		if( (offset<Constants.BUFFER_SIZE) && (sum>Constants.BUFFER_SIZE)){
//			if(logFileOffset.get()>0){
//				while(countFlag.get(1).get()<Constants.BUFFER_SIZE){
//					//等待
//					System.out.println("wait1...");
//				}
//				appendMessage(loopBytes,writeName);
//				countFlag.get(1).set(0);
//				shouldAppend.incrementAndGet();
//			}
//			countFlag.get(0).addAndGet(Constants.BUFFER_SIZE-offset);
//			countFlag.get(1).addAndGet(offset+size-Constants.BUFFER_SIZE);
//			System.out.print(offset+" "+logFileOffset.get()+" ");
//			System.out.println(size);
//			System.arraycopy(messages, 0 ,loopBytes, offset, size);
//		}
//		else if( (offset<2*Constants.BUFFER_SIZE) && (sum>2*Constants.BUFFER_SIZE)){
//			if(logFileOffset.get()>0){
//				while(countFlag.get(2).get()<Constants.BUFFER_SIZE){
//					//等待
//					System.out.println("wait2...");
//				}
//				appendMessage(loopBytes,writeName);
//				countFlag.get(2).set(0);
//				shouldAppend.set(0);
//			}
//			countFlag.get(1).addAndGet(2*Constants.BUFFER_SIZE-offset);
//			countFlag.get(2).addAndGet(offset+size-2*Constants.BUFFER_SIZE);
//			System.arraycopy(messages, 0 ,loopBytes, offset, size);
//		
//		}
//		else if( (offset<Constants.BYTE_SIZE) && (sum>Constants.BYTE_SIZE)){
//			while(countFlag.get(0).get()<Constants.BUFFER_SIZE){
//				//等待
//				System.out.println(countFlag.get(0).get());
//				System.out.println("wait0...");
//			}
//			//提交第一部分
//			System.out.println(sum);
//			appendMessage(loopBytes,writeName);
//			countFlag.get(0).set(0);
//			shouldAppend.incrementAndGet();
//			logFileOffset.incrementAndGet();
//			int first=Constants.BYTE_SIZE-offset;
//			int last=sum-Constants.BYTE_SIZE;
//			countFlag.get(2).addAndGet(first);
//			countFlag.get(0).addAndGet(last);
//			System.arraycopy(messages, 0 ,loopBytes, offset, first);
//			System.arraycopy(messages, first, loopBytes, 0, last);	
//		}
//		else {
//			countFlag.get(0).updateAndGet(x->sum<Constants.BUFFER_SIZE?x+size:x);
//			countFlag.get(1).updateAndGet(x->(sum>=(Constants.BUFFER_SIZE)&&(sum<=2*Constants.BUFFER_SIZE))?x+size:x);
//			countFlag.get(2).updateAndGet(x->(sum>=(2*Constants.BUFFER_SIZE)&&(sum<=Constants.BYTE_SIZE))?x+size:x);
//			System.out.print(offset+" "+logFileOffset.get()+" ");
//			System.out.println(size);
//			System.arraycopy(messages, 0 ,loopBytes, offset, size);
//		}
//
//		fileWriteLock.unlock();
		
	}

	public Index appendIndex(int size) {
		// appendIndex是否返回Name待定
		return indexFile.appendIndex0(size);
	}

	public void appendMessage0(byte[] messages, int logFileID, int offset) {
		
		
		appendMessage(messages, logFileID);
//		int  size = messages.length;
//	
//		if(offset==0){
//			shouldWirteName.put(this.path, logFileID);
//		}
//		while (offset >= (Constants.BYTE_SIZE)) {
//			offset -= (Constants.BYTE_SIZE);
//		}
//		 int offset1=offset;
//		System.out.println(this.path+"should write "+logFileID+"offset:"+offset+"size:"+(offset+size));
//		
//		//存放不下
//		int sum=offset+size;
//		if(sum > Constants.BUFFER_SIZE){
//			System.out.println(this.path+"should write "+logFileID+"offset:"+offset+"size:"+(sum)+"分批");
//			System.arraycopy(messages, 0, loopBytes, offset, Constants.BUFFER_SIZE-offset);
//			logFileOffset.compute(this.path, (k,v)->v==null?Constants.BUFFER_SIZE-offset1:v+Constants.BUFFER_SIZE-offset1);
////			logFileOffset.addAndGet(Constants.BUFFER_SIZE-offset);
//			System.out.println(Constants.BUFFER_SIZE-offset+" "+lastFileId);
//			//之前的数组满了，提交写入
//			while(logFileOffset.get(this.path)<Constants.BUFFER_SIZE){
//				System.out.print(this.path);
//				System.out.println("wait,,1");
//			}
//			
//			appendMessage(loopBytes, shouldWirteName.get(this.path));
//			logFileOffset.compute(this.path, (k,v)->0);
//			System.arraycopy(messages, 0, loopBytes, 0, offset+size-Constants.BUFFER_SIZE);
//			lastFileId=logFileID;
//			logFileOffset.compute(this.path, (k,v)->v==null?offset1+size-Constants.BUFFER_SIZE:v+offset1+size-Constants.BUFFER_SIZE);
////			logFileOffset.addAndGet(offset+size-Constants.BUFFER_SIZE);
//		}
//		else if(sum <= Constants.BUFFER_SIZE){
//			System.arraycopy(messages, 0, loopBytes, offset,size);
////			logFileOffset.addAndGet(size);
//			logFileOffset.compute(this.path, (k,v)->v==null?size:v+size);
//			if(logFileOffset.get(this.path)==Constants.BUFFER_SIZE){
//				appendMessage(loopBytes,shouldWirteName.get(this.path));
//				logFileOffset.put(this.path, 0);
//			}
//		}
//		else{
//			System.arraycopy(messages, 0, loopBytes, offset,size);
//			logFileOffset.addAndGet(size);
//			if(logFileOffset.get()==Constants.BUFFER_SIZE){
//				appendMessage(loopBytes, lastFileId);
//				logFileOffset.set(0);
//			}
//		}
//		if(logFileOffset.updateAndGet(x->(Constants.BUFFER_SIZE-x)>=size?x+size:-1)==-1){
//			System.out.println("将要拆分两部分");
//			System.arraycopy(messages, 0, loopBytes, offset, Constants.BUFFER_SIZE-offset);
//			//之前的数组满了，提交写入
//			appendMessage(loopBytes, lastFileId);
//			logFileOffset.set(offset+size-Constants.BUFFER_SIZE);
//			System.arraycopy(messages, 0, loopBytes, 0, offset+size-Constants.BUFFER_SIZE);
//		}
//		//正好存满，提交
//		else if(logFileOffset.updateAndGet(x->(Constants.BUFFER_SIZE-x)==size?x+size:-1)==Constants.BUFFER_SIZE){
//			System.arraycopy(messages, 0, loopBytes, offset,size);
//			appendMessage(loopBytes, lastFileId);
//			logFileOffset.set(0);
//		}
//		else {
//			System.arraycopy(messages, 0, loopBytes, offset, size);
//			logFileOffset.addAndGet(size);
//		}
		
		
		
	
		
		
//		fileWriteLock.lock();
//		if(offset==0 && logFileOffset.get()>0){	
//			commit(loopBytes);
//			shouldAppend.set(0);
//			logFileOffset.set(0);
//		}
//		if (offset == 0) {
//			writeName = logFileID + "";
//		}
//		while (offset >= (Constants.BYTE_SIZE)) {
//			offset -= (Constants.BYTE_SIZE);
//
//		}
//		int sum = offset + size;
//		if ((offset < Constants.BUFFER_SIZE) && (sum > Constants.BUFFER_SIZE)) {
//			if (logFileOffset.get() > 0) {
//				while (countFlag.get(1).get() < Constants.BUFFER_SIZE) {
//					// 等待
//					System.out.println("wait1...");
//				}
//				appendMessage(loopBytes, writeName);
//				countFlag.get(1).set(0);
//				shouldAppend.incrementAndGet();
//			}
//			int first = Constants.BUFFER_SIZE - offset;
//			int last = offset + size - Constants.BUFFER_SIZE;
//			countFlag.get(0).addAndGet(first);
//			countFlag.get(1).addAndGet(last);
//			System.out.print(offset + " " + logFileOffset.get() + " ");
//			System.out.println(size);
//			System.arraycopy(messages, 0, loopBytes, offset, size);
//		} else if ((offset < 2 * Constants.BUFFER_SIZE) && (sum > 2 * Constants.BUFFER_SIZE)) {
//			if (logFileOffset.get() > 0) {
//				while (countFlag.get(2).get() < Constants.BUFFER_SIZE) {
//					// 等待
//					System.out.println("wait2...");
//				}
//				appendMessage(loopBytes, writeName);
//				countFlag.get(2).set(0);
//				shouldAppend.set(0);
//			}
//
//			int first = 2 * Constants.BUFFER_SIZE - offset;
//			int last = offset + size - 2 * Constants.BUFFER_SIZE;
//			countFlag.get(1).addAndGet(first);
//			countFlag.get(2).addAndGet(last);
//			System.arraycopy(messages, 0, loopBytes, offset, size);
//
//		} else if ((offset < Constants.BYTE_SIZE) && (sum > Constants.BYTE_SIZE)) {
//			while (countFlag.get(0).get() < Constants.BUFFER_SIZE) {
//				// 等待
//				System.out.println(countFlag.get(0).get());
//				System.out.println("wait0...");
//			}
//			// 提交第一部分
//			System.out.println("aa");
//			System.out.println(sum);
//			appendMessage(loopBytes, writeName);
//			countFlag.get(0).set(0);
//			shouldAppend.incrementAndGet();
//			logFileOffset.incrementAndGet();
//			int first = Constants.BYTE_SIZE - offset;
//			int last = sum - Constants.BYTE_SIZE;
//			countFlag.get(2).addAndGet(first);
//			countFlag.get(0).addAndGet(last);
//			System.arraycopy(messages, 0, loopBytes, offset, first);
//			System.arraycopy(messages, first, loopBytes, 0, last);
//
//		} else {
//			if (sum < Constants.BUFFER_SIZE) {
//				countFlag.get(0).addAndGet(size);
//			} else if (sum <= 2 * Constants.BUFFER_SIZE) {
//				countFlag.get(1).addAndGet(size);
//			} else if (sum <= Constants.BYTE_SIZE) {
//				countFlag.get(2).addAndGet(size);
//			}
//
//			System.out.print(offset + " " + logFileOffset.get() + " ");
//			System.out.println(size);
//			System.arraycopy(messages, 0, loopBytes, offset, size);
//		}
//		fileWriteLock.unlock();
	}

	// for Producer
	private void appendMessage(byte[] messages, int logName) {
		LogFile willWrite=getLastLogFile();
		if(!(willWrite.getFileName().equals("LOG"+logName))){
			willWrite=getNewLogFile(logName+"");
		}
		willWrite.doAppend(messages);
	}

	// for Consumer
	public FileChannel getIndexFileChannel() {
		return indexFile.getFileChannel();
	}

	// for Consumer
	public FileChannel getLogFileChannelByFileID(int fileID) {
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
	public void commit(byte[] messages) {
//		int start = shouldAppend.get();
//		while(countFlag.get(2).get()<Constants.BUFFER_SIZE ){
//		}
//		while(countFlag.get(0).get()<Constants.BUFFER_SIZE){
//		}
//		while(countFlag.get(start).get()<=Constants.BUFFER_SIZE && countFlag.get(start).get()>0){
//			appendMessage(messages, writeName);
//			countFlag.get(start).set(0);
//			start = shouldAppend.updateAndGet(x -> x == 2 ? 0 : ++x);
//		}
	}
	
	// for Producer
	public void commit(byte[] messages,boolean last) {
//		int start = shouldAppend.get();
//		if(!last){
//			while(countFlag.get(2).get()<Constants.BUFFER_SIZE ){
//			}
//			while(countFlag.get(0).get()<Constants.BUFFER_SIZE){
//			}
//		}
//
//		while(countFlag.get(start).get()<=Constants.BUFFER_SIZE ){
//			appendMessage(messages, writeName);
//			countFlag.get(start).set(0);
//			start = shouldAppend.updateAndGet(x -> x == 2 ? 0 : ++x);
//		}
	}

	public void flushIndex(Index lastIndex) {
		try {
			lastFile.seek(0);
			lastFile.writeInt(lastIndex.fileID);
			lastFile.writeInt(lastIndex.offset);
			lastFile.writeInt(lastIndex.size);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// for Producer
	// synchronized for lastIndex
	public synchronized void flush(Index lastIndexOfProducer) {
		// 1. update lastIndex
		if (lastIndex.fileID == lastIndexOfProducer.fileID) {
			if (lastIndex.offset < lastIndexOfProducer.offset) {
				lastIndex = lastIndexOfProducer;
				flushIndex(lastIndex);
			}
		} else if (lastIndex.fileID < lastIndexOfProducer.fileID) {
			lastIndex = lastIndexOfProducer;
			flushIndex(lastIndex);
		}
		// 2. flush Index
		

		// 3. flush LogFile
		
		
	}

}
