package com.meizu.bigdata.cetus.anyloader.java.model;

public class DataPath {

    private String path = null;

    private int sequence = -1;

    public DataPath(String path, int sequence) {
        this.path = path;
        this.sequence = sequence;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getSequence() {
        return sequence;
    }
}
