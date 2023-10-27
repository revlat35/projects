package com.rev.messagepriorityqueue.provider;

import java.util.Queue;

import com.rev.messagepriorityqueue.model.Message;

public interface OrderedMessageQueueProvider {
	Queue<Message> getOutQueue();
	void queueMessage(Message m);
}
