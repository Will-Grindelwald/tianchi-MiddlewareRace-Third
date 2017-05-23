package io.openmessaging.demo;

import io.openmessaging.Message;

public class MessageStore {

	// private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();
	// private Map<String, HashMap<String, Integer>> queueOffsets = new
	// HashMap<>();

	// private boolean flag = false;

	private String path;

	public void putMessage(String bucket, Message message) {
		// if (!messageBuckets.containsKey(bucket)) {
		// messageBuckets.put(bucket, new ArrayList<>(1024));
		// }
		// ArrayList<Message> bucketList = messageBuckets.get(bucket);
		// bucketList.add(message);
	}

	// public void writeMessage(String path) {
	// File f = new File(path + "/" + "messageAli");
	//
	// if (!f.exists()) {
	// try {
	// f.createNewFile();
	// FileOutputStream out = new FileOutputStream(f);
	// ObjectOutputStream ob = new ObjectOutputStream(out);
	// ob.writeObject(messageBuckets);
	// ob.close();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
	// // } else {
	// //
	// // try {
	// // FileOutputStream out = new FileOutputStream(f, true);
	// // MyObjectOutputStream ob = new MyObjectOutputStream(out);
	// // ob.writeObject(message);
	// // ob.close();
	// // out.close();
	// // } catch (IOException e) {
	// // e.printStackTrace();
	// // }
	// //
	// // }
	//
	// }

	// public void readerMessage(String bucket, String path) {
	// File f = new File(path + "/" + "messageAli");
	//
	// Message message = null;
	// if (!f.exists()) {
	// System.out.println("error");
	// return;
	// } else {
	// try {
	// FileInputStream fIn = new FileInputStream(f);
	// ObjectInputStream in = new ObjectInputStream(fIn);
	// messageBuckets = (Map<String, ArrayList<Message>>) in.readObject();
	// // while (fIn.available() > 0) {
	// // message = (Message) in.readObject();
	// // if (!messageBuckets.containsKey(bucket)) {
	// // messageBuckets.put(bucket, new ArrayList<>(1024));
	// // }
	// // ArrayList<Message> bucketlist = messageBuckets.get(bucket);
	// // bucketlist.add(message);
	// // // System.out.println(message);
	// // }
	// in.close();
	// fIn.close();
	//
	// } catch (IOException | ClassNotFoundException e) {
	// e.printStackTrace();
	// }
	// }
	// }

	public Message pullMessage(String queue, String bucket, String path) {

		// if (!flag) {
		// flag = true;
		// readerMessage(bucket, path);
		// }
		// ArrayList<Message> bucketList = messageBuckets.get(bucket);
		// if (bucketList == null) {
		// return null;
		// }
		// HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
		// if (offsetMap == null) {
		// offsetMap = new HashMap<>();
		// queueOffsets.put(queue, offsetMap);
		// }
		// int offset = offsetMap.getOrDefault(bucket, 0);
		// if (offset >= bucketList.size()) {
		// return null;
		// }
		// Message message = bucketList.get(offset);
		// offsetMap.put(bucket, ++offset);
		// // System.out.println(message.toString());
		// return message;
		return null;
	}
}
