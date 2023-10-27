package com.rev.messagepriorityqueue.model;

public class Message {
	
	private long messageId;
	private String sourceChannelId;
	private byte[] payload;


	public long getMessageId() {
		return messageId;
	}
	
	public void setMessageId(long messageId) {
		this.messageId = messageId;
	}
	
	public String getSourceChannelId() {
		return sourceChannelId;
	}
	
	public void setSourceChannelId(String sourceChannelId) {
		this.sourceChannelId = sourceChannelId;
	}
	
	public byte[] getPayload() {
		return payload;
	}
	
	public void setPayload(byte[] payload) {
		this.payload = payload;
	}
	
	@Override
	public String toString() {
		return "[Message=" + messageId + ", Channel=" + sourceChannelId + "]";
	}
	
}
