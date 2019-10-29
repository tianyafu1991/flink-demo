package com.tianyafu.course06;

public class Sales {

    private String transactionId;
    private String customerId;
    private String itemId;
    private Double amountPaId;

    public Sales() {
    }

    public Sales(String transactionId, String customerId, String itemId, Double amountPaId) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.itemId = itemId;
        this.amountPaId = amountPaId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Double getAmountPaId() {
        return amountPaId;
    }

    public void setAmountPaId(Double amountPaId) {
        this.amountPaId = amountPaId;
    }

    @Override
    public String toString() {
        return "Sales{" +
                "transactionId='" + transactionId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", amountPaId=" + amountPaId +
                '}';
    }
}
