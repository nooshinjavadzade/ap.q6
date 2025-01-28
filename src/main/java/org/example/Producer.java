package org.example;
import java.util.Random;



public class Producer implements Runnable {
    private Topic topic;
    private String id;
    public Producer(String id, Topic topic) {
        this.id = id;
        this.topic = topic;
    }
    public String getId() {
        return id;
    }
    public Topic getTopic() {
        return topic;
    }
    @Override
    public void run() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            String content = "Message " + i;
            Message.Priority priority = Message.Priority.values()[random.nextInt(Message.Priority.values().length)];
            Message x = new Message(content, priority);
            topic.sendMessage(x);
            System.out.println("Producer " + id + " produced: " + content + " with priority " + priority);
            try {
                Thread.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}