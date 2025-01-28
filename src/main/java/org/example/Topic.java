package org.example;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;



public class Topic {
    private String name;
    private BlockingQueue<Message> saf;
    private ConcurrentHashMap<String, Consumer> c;
    public Topic(String name) {
        this.name = name;
        this.saf = new PriorityBlockingQueue<>(11, (m1, m2) -> m1.getPriority().compareTo(m2.getPriority()));
        this.c = new ConcurrentHashMap<>();
    }
    public String getName() {
        return name;
    }
    public void registerConsumer(Consumer consumer) {
        c.put(consumer.getId(), consumer);
        new Thread(consumer).start();
    }
    public void publish() {
        while (!saf.isEmpty()) {
            Message message = saf.poll();
            for (Consumer consumer : c.values()) {
                consumer.receiveMessage(message);
            }
        }
    }
    public void sendMessage(Message message) {
        try {
            saf.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public Message getMessage() {
        try {
            return saf.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}