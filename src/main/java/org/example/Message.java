package org.example;

public class Message{
    public enum Priority {
        LOW, MEDIUM, HIGH
    }
    private String content;
    private Priority olaviat;
    public Message(String content, Priority x) {
        this.content = content;
        this.olaviat = x;
    }
    public Priority getPriority() {
        return olaviat;
    }
    public String getContent() {
        return content;
    }

}
