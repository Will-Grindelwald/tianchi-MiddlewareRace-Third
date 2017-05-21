package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore implements Serializable {

	private static final MessageStore INSTANCE = new MessageStore();

	public static MessageStore getInstance() {
		return INSTANCE;
	}

	private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

	private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

	private boolean flag=false;
	public synchronized void putMessage(String bucket, Message message, String path) {
		File f = new File(path + "/" + bucket);

		if (!f.exists()) {
			try {
				f.createNewFile();
				FileOutputStream out = new FileOutputStream(f);
				ObjectOutputStream ob = new ObjectOutputStream(out);
				ob.writeObject(message);
				ob.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {

			try {
				FileOutputStream out = new FileOutputStream(f, true);
				MyObjectOutputStream ob = new MyObjectOutputStream(out);
				ob.writeObject(message);
				ob.close();
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	
	public void readerMessage(String bucket,String path){
		File f = new File(path + "/" + bucket);
		System.out.println(f.toString());
		Message message = null;
		if (!f.exists()) {
			System.out.println("error");
			return ;
		} else {
			try {
				FileInputStream fIn = new FileInputStream(f);
				ObjectInputStream in = new ObjectInputStream(fIn);
				while (fIn.available() > 0) {
					message = (Message) in.readObject();
					if(!messageBuckets.containsKey(bucket)){
						messageBuckets.put(bucket, new ArrayList<>(1024));
					}
					ArrayList<Message> bucketlist=messageBuckets.get(bucket);
					bucketlist.add(message);
//					System.out.println(message);
				}
				in.close();
				fIn.close();

			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public synchronized Message pullMessage(String queue, String bucket, String path) {

		if(!flag){
			flag=true;
			readerMessage(bucket, path);
		}
		 ArrayList<Message> bucketList = messageBuckets.get(bucket);
		 if (bucketList == null) {
		 return null;
		 }
		 HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
		 if (offsetMap == null) {
		 offsetMap = new HashMap<>();
		 queueOffsets.put(queue, offsetMap);
		 }
		 int offset = offsetMap.getOrDefault(bucket, 0);
		 if (offset >= bucketList.size()) {
		 return null;
		 }
		 Message message = bucketList.get(offset);
		 offsetMap.put(bucket, ++offset);
		// System.out.println(message.toString());
		return message;
	}
}
