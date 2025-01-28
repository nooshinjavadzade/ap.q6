package org.example;
public class Consumer implements Runnable {
    private String id;
    private Topic topic;
    public Consumer(String id, Topic topic) {
        this.id = id;
        this.topic = topic;
    }
    public String getId() {
        return id;
    }
    @Override
    public void run() {
        while (true) {
            Message y = topic.getMessage();
            if (y != null) {
                receiveMessage(y);
            }
        }
    }
    public void receiveMessage(Message message) {
        // Log reception
        System.out.println("consumer-" + id + " get message with priority " + message.getPriority() + " and content \"" + message.getContent() + "\"");
    }
}