package org.example;

import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Producer> producers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Scanner cin = new Scanner(System.in);
        String input;
        while (true) {
            input = cin.nextLine();
            String[] in = input.split(" ");

            switch (in[0]) {
                case "register":
                    if (in.length < 6) {
                        System.out.println("Insufficient arguments for register command.");
                        break;
                    }
                    if (in[1].equals("-r") && in[2].equals("producer")) {
                        registerProducer(in);
                    } else if (in[1].equals("-r") && in[2].equals("consumer")) {
                        registerConsumer(in);
                    } else {
                        System.out.println("Unknown entity type for registration.");
                    }
                    break;
                case "send":
                    if (in.length < 10) {
                        System.out.println("Insufficient arguments for send command.");
                        break;
                    }
                    sendMessage(in);
                    break;
                case "publish":
                    if (in.length < 3) {
                        System.out.println("Insufficient arguments for publish command.");
                        break;
                    }
                    publishMessages(in);
                    break;
                default:
                    System.out.println("Unknown command");
            }
        }
    }

    private static void registerProducer(String[] parts) {
        String id = parts[4]; // -i <id>
        String topicName = parts[6]; // -topic <topic-name>
        if (producers.containsKey(id)) {
            System.out.println("Producer ID already exists.");
            return;
        }
        Topic t = topics.computeIfAbsent(topicName, Topic::new);
        Producer producer = new Producer(id, t);
        producers.put(id, producer); // ذخیره Producer در Map
        System.out.println("Registered Producer with ID: " + id);
        new Thread(producer).start();
    }

    private static void registerConsumer(String[] parts) {
        String id = parts[4]; // -i <id>
        String topicName = parts[6]; // -topic <topic-name>
        if (consumers.containsKey(id)) {
            System.out.println("Consumer ID already exists.");
            return;
        }
        if (!topics.containsKey(topicName)) {
            System.out.println("Invalid topic name.");
            return;
        }
        Topic t = topics.get(topicName);
        Consumer consumer = new Consumer(id, t);
        consumers.put(id, consumer);
        t.registerConsumer(consumer);
    }

    private static void sendMessage(String[] parts) {
        String producerId = parts[3]; // -o <producer-ID>
        String priorityStr = parts[5]; // -p <priority>
        String topicName = parts[7]; // -t <topic>
        String content = parts[9]; // -content <content>

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
            messagePriority = Message.Priority.valueOf(priorityStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid priority.");
            return;
        }

        Message message = new Message(content, messagePriority);
        producer.getTopic().sendMessage(message); // پیام به Topic ارسال می‌شود
    }

    private static void publishMessages(String[] parts) {
        String topicName = parts[2]; // -t <topic-name>
        if (topics.containsKey(topicName)) {
            topics.get(topicName).publish();
        } else {
            System.out.println("Invalid topic name.");
        }
    }

    // کلاس داخلی Message
    static class Message {
        public enum Priority {
            HIGH, MEDIUM, LOW
        }

        private final String content;
        private final Priority priority;

        public Message(String content, Priority priority) {
            this.content = content;
            this.priority = priority;
        }

        public String getContent() {
            return content;
        }

        public Priority getPriority() {
            return priority;
        }
    }

    // کلاس داخلی Topic
    static class Topic {
        private final String name;
        private final BlockingQueue<Message> messageQueue = new PriorityBlockingQueue<>(11, Comparator.comparing(Message::getPriority).reversed());
        private final List<Consumer> consumers = new ArrayList<>();

        public Topic(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void registerConsumer(Consumer consumer) {
            consumers.add(consumer);
        }

        public void sendMessage(Message message) {
            messageQueue.offer(message); // پیام به صف اضافه می‌شود
        }

        public void publish() {
            while (!messageQueue.isEmpty()) {
                Message message = messageQueue.poll(); // پیام از صف گرفته می‌شود
                for (Consumer consumer : consumers) {
                    consumer.receiveMessage(message);
                }
            }
        }
    }

    // کلاس داخلی Producer
    static class Producer implements Runnable {
        private final String id;
        private final Topic topic;

        public Producer(String id, Topic topic) {
            this.id = id;
            this.topic = topic;
        }

        public Topic getTopic() {
            return topic;
        }

        @Override
        public void run() {
            // Producer logic can be implemented here if needed.
        }
    }

    // کلاس داخلی Consumer
    static class Consumer {
        private final String id;
        private final Topic topic;

        public Consumer(String id, Topic topic) {
            this.id = id;
            this.topic = topic;
        }

        public void receiveMessage(Message message) {
            System.out.printf("consumer-%s get message with priority %s and content \"%s\"%n", id, message.getPriority(),
                    message.getContent());
        }
    }
}