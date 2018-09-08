package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by edexworld on 3/7/18.
 */

public class CountSequence implements Comparable<CountSequence> {

    Integer processId;
    Double priority;

    public Integer getProcessId() {
        return processId;
    }

    public void setProcessId(Integer processId) {
        this.processId = processId;
    }

    public Double getPriority() {
        return priority;
    }

    public void setPriority(Double priority) {
        this.priority = priority;
    }

    @Override
    public int compareTo(CountSequence another) {
        return this.getProcessId().compareTo(another.getProcessId());
    }
}
