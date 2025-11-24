package com.streamforge.pipeline.model;

public class OrderEvent {

    private String eventId;
    private String userId;
    private String storeId;
    private double amount;
    private long eventTimeMs;

    public OrderEvent() {
    }

    public OrderEvent(String eventId, String userId, String storeId, double amount, long eventTimeMs) {
        this.eventId = eventId;
        this.userId = userId;
        this.storeId = storeId;
        this.amount = amount;
        this.eventTimeMs = eventTimeMs;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getEventTimeMs() {
        return eventTimeMs;
    }

    public void setEventTimeMs(long eventTimeMs) {
        this.eventTimeMs = eventTimeMs;
    }
}

