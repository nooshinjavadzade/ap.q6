package org.example;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;




public class Main {
    private static ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Producer> producers = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Scanner cin = new Scanner(System.in);
        String input;
        while (true) {
            input = cin.nextLine();
            String[] in = input.split(" ");

            switch (in[0]) {
                case "register":
                    if (in[1].equals("producer")) {
                        registerProducer(in);
                    } else if (in[1].equals("consumer")) {
                        registerConsumer(in);
                    }
                    break;
                case "send":
                    sendMessage(in);
                    break;
                case "publish":
                    publishmessages(in);
                    break;
                default:
                    System.out.println("Unknown command");
            }
        }
    }
    private static void registerProducer(String[] parts) {
        String id = parts[3];
        String topicName = parts[5];
        if (producers.containsKey(id)) {
            System.out.println("Producer ID already exists.");
            return;
        }
        Topic t = topics.computeIfAbsent(topicName, Topic::new);
        Producer producer = new Producer(id, t);
        producers.put(id, producer);
        new Thread(producer).start();
    }
    private static void registerConsumer(String[] parts) {
        String id = parts[3];
        String topicName = parts[5];
        if (consumers.containsKey(id)) {
            System.out.println("Consumer ID already exists.");
            return;
        }
        if (!topics.containsKey(topicName)) {
            System.out.println("Invalid topic name.");
            return;
        }
        Topic t = topics.get(topicName);
        Consumer x = new Consumer(id, t);
        consumers.put(id, x);
        t.registerConsumer(x);
    }
    private static void sendMessage(String[] parts) {
        String producerId = parts[3];
        String priority = parts[5];
        String topicName = parts[7];
        String content = parts[9];
        if (!producers.containsKey(producerId)) {
            System.out.println("Producer ID does not exist.");
            return;
        }
        Producer producer = producers.get(producerId);
        if (!producer.getTopic().getName().equals(topicName)) {
            System.out.println("Producer cannot send message to this topic.");
            return;
        }
        Message.Priority messagePriority;
        try {
            messagePriority = Message.Priority.valueOf(priority.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid priority.");
            return;
        }
        Message mes = new Message(content, messagePriority);
        producer.getTopic().sendMessage(mes);
    }
    private static void publishmessages(String[] parts) {
        String topicName = parts[3];
        if (topics.containsKey(topicName)) {
            topics.get(topicName).publish();
        } else {
            System.out.println("Invalid topic name.");
        }
    }
}