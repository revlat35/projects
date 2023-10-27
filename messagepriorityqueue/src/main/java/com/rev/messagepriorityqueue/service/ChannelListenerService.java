package com.rev.messagepriorityqueue.service;

import java.util.Queue;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.provider.ChannelListener;

public class ChannelListenerService implements ChannelListener {
	
	private OrderedMessageQueueService omqService;
	
	public void registerChannelListener(OrderedMessageQueueService omqService) {
		this.omqService = omqService;
	}
	
	@Override
	public void messageReceived(Message m) {
		omqService.queueMessage(m);
	}
	
	public Queue<Message> getOutQueue() {
		return omqService.getOutQueue();
	}
}
