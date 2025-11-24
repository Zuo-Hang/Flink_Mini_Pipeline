package com.streamforge.pipeline.model;

public class OrderMetric {

    private String windowStart;
    private String windowEnd;
    private String storeId;
    private long orderCnt;
    private double gmvTotal;
    private long userCnt;

    public OrderMetric() {
    }

    public OrderMetric(String windowStart, String windowEnd, String storeId, long orderCnt, double gmvTotal, long userCnt) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.storeId = storeId;
        this.orderCnt = orderCnt;
        this.gmvTotal = gmvTotal;
        this.userCnt = userCnt;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public long getOrderCnt() {
        return orderCnt;
    }

    public void setOrderCnt(long orderCnt) {
        this.orderCnt = orderCnt;
    }

    public double getGmvTotal() {
        return gmvTotal;
    }

    public void setGmvTotal(double gmvTotal) {
        this.gmvTotal = gmvTotal;
    }

    public long getUserCnt() {
        return userCnt;
    }

    public void setUserCnt(long userCnt) {
        this.userCnt = userCnt;
    }
}

