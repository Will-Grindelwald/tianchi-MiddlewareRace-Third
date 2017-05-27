package io.openmessaging.official.tester;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTester {

    static Logger logger = LoggerFactory.getLogger(ProducerTester.class);
    //0表示默认;
    static AtomicInteger state = new AtomicInteger(0);
    static String errorMessage = "";

    static class ProducerTask extends Thread {
        String label = Thread.currentThread().getName();
        Random random = new Random();
        Producer producer = null;
        int sendNum = 0;
        Map<String, byte[]> offsets = new HashMap<>();
        public ProducerTask(String label) {
            this.label = label;
            init();
        }

        public void init() {
            //init producer
            try {
                Class kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class producerClass = Class.forName("io.openmessaging.demo.DefaultProducer");
                producer = (Producer) producerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (producer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
            } catch (Exception e) {
                logger.error("please check the package name and class name:", e);
            }
            //init offsets
            byte[] news=new byte[1024*256];
            for(int i=0;i<1024*256;i++){
            	news[i]='q';
            }
            for (int i = 0; i < 10; i++) {
                offsets.put("TOPIC_" + i,news);
                offsets.put("QUEUE_" + i,news);
            }

        }

        @Override
        public void run() {
            while (true) {
                try {
                    String queueOrTopic;
                    if (sendNum % 2== 0) {
                        queueOrTopic = "QUEUE_" + random.nextInt(2);
                    } else {
                        queueOrTopic = "TOPIC_" + random.nextInt(2);
                    }
                    Message message = producer.createBytesMessageToQueue(queueOrTopic, (offsets.get(queueOrTopic)));
                    logger.debug("queueOrTopic:{} offset:{}", queueOrTopic, label + "_" + offsets.get(queueOrTopic));
                    offsets.put(queueOrTopic, offsets.get(queueOrTopic));
                    producer.send(message);
                    sendNum++;
                    if (sendNum >= Constants.PRO_MAX) {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Error occurred in the sending process", e);
                    break;
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Thread[] ts = new Thread[Constants.PRO_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new ProducerTask(Constants.PRO_PRE + i);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
//        for (int i = 0; i < ts.length; i++) {
//            ts[i].join();
//        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        logger.info("Produce Finished, Cost {} ms", end - start);
    }
}
