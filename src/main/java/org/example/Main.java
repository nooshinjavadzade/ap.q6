package org.example;

import java.util.*;
import java.util.concurrent.*;

//nooshin javadzade 402105857











public class Main {
    private static final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Producer> producers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();
    public static void main(String[] args) {
        System.out.println("hi, please enter your command and enter it correctly :)");
        Scanner cin = new Scanner(System.in);
        String input;
        boolean mewo = true;
        while (mewo) {
            input = cin.nextLine();
            String[] in = input.split(" ");
            switch (in[0]) {
                case "register":
                    if (in.length < 6) {
                        System.out.println("please enter correct");
                        break;
                    }
                    if (in[1].equals("-r") && in[2].equals("producer")) {
                        fProducer(in);
                    } else if (in[1].equals("-r") && in[2].equals("consumer")) {
                       fConsumer(in);
                    } else {
                        System.out.println("please enter correct");
                    }
                    break;
                case "send":
                    if (in.length < 10) {
                        System.out.println("please enter correct");
                        break;
                    }
                    sendMessage(in);
                    break;
                case "publish":
                    if (in.length < 3) {
                        System.out.println("please enter correct");
                        break;
                    }
                    publishMessages(in);
                    break;
                default:
                    System.out.println("please inter correct");
            }
        }
    }
    private static void fProducer(String[] parts) {
        String id = parts[4];
        String topicName = parts[6];
        if (producers.containsKey(id)) {
            System.out.println("Producer id already exists");
            return;
        }
        Topic t = topics.computeIfAbsent(topicName, Topic::new);
        Producer producer = new Producer(id, t);
        producers.put(id, producer);
        System.out.println("ok");
        new Thread(producer).start();
    }

    private static void fConsumer(String[] parts) {
        String id = parts[4];
        String topicName = parts[6];
        if (consumers.containsKey(id)) {
            System.out.println("Consumer id already exists.");
            return;
        }
        if (!topics.containsKey(topicName)) {
            System.out.println("Invalid topic name.");
            return;
        }
        Topic t = topics.get(topicName);
        Consumer consumer = new Consumer(id, t);
        consumers.put(id, consumer);
        System.out.println("ok");
        t.registerConsumer(consumer);
    }

    private static void sendMessage(String[] parts) {
        String producerid = parts[3];
        String priorityStr = parts[5];
        String topicName = parts[7];
        StringBuilder contentBuilder = new StringBuilder();
        for (int i = 9; i < parts.length; i++) {
            contentBuilder.append(parts[i]).append(" ");
        }
        String content = contentBuilder.toString().trim();
        if (!producers.containsKey(producerid)) {
            System.out.println("Producer ID does not exist.");
            return;
        }
        Producer r = producers.get(producerid);
        if (!r.getTopic().getName().equals(topicName)) {
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
        r.getTopic().sendMessage(message);
        System.out.println("ok");
    }

    private static void publishMessages(String[] parts) {
        String topicName = parts[2];
        if (topics.containsKey(topicName)) {
            topics.get(topicName).publish();
        } else {
            System.out.println("Invalid topic name.");
        }
    }
    static class Message {
        public enum Priority {
            HIGH, MEDIUM, LOW
        }
        private final String content;
        private final Priority o;

        public Message(String content, Priority priority) {
            this.content = content;
            this.o = priority;
        }
        public String getContent() {
            return content;
        }
        public Priority getPriority() {
            return o;
        }
    }
    static class Topic {
        private final String name;
        private final BlockingQueue<Message> messageQueue = new PriorityBlockingQueue<>(11, new Comparator<Message>() {
            @Override
            public int compare(Message m1, Message m2) {
                return m1.getPriority().ordinal() - m2.getPriority().ordinal();
            }
        });
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
            messageQueue.offer(message);
        }

        public void publish() {
            while (!messageQueue.isEmpty()) {
                Message message = messageQueue.poll();
                for (Consumer consumer : consumers) {
                    consumer.receiveMessage(message);
                }
            }
        }
    }
    static class Producer implements Runnable {
        private final String id;
        private final Topic topic;
        public Producer(String x, Topic y) {
            id = x;
            topic = y;
        }
        public Topic getTopic() {
            return topic;
        }
        @Override
        public void run() {
        }
    }

    static class Consumer {
        private final String id;
        private final Topic topic;
        public Consumer(String x, Topic y) {
            id = x;
            topic = y;
        }
        public void receiveMessage(Message message) {
            System.out.printf("consumer-%s get message with priority %s and content %s %n", id, message.getPriority(),
                    message.getContent());
        }
    }
}