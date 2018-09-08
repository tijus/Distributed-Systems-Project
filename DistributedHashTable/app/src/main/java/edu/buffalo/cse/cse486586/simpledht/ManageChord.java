package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

public class ManageChord {

    String predecessor;
    String successor;

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public String getSuccessor() {
        return successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }
}
