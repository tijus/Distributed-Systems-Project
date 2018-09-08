package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by edexworld on 3/4/18.
 */

public class KeyValueQueue implements Comparable<KeyValueQueue> {

    String message;
    Double priority;

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setPriority(Double priority) {
        this.priority = priority;
    }

    public Double getPriority() {
        return priority;
    }

    @Override
    public int compareTo(KeyValueQueue another) {
        // Sorting by priority
        return this.getPriority().compareTo(another.getPriority());
    }
}
