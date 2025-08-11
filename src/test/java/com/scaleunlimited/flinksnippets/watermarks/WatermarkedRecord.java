package com.scaleunlimited.flinksnippets.watermarks;

public class WatermarkedRecord {

    private int id;
    private long timestamp;

    public WatermarkedRecord() {}

    public WatermarkedRecord(int id) {
        this(id, id * 100);
    }

    public WatermarkedRecord(int id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
