package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message pull() {

        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized Message pullNoWait() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            Message message = messageStore.pullMessage(queue, bucket);
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    @Override public Message pullNoWait(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

}
