package com.rev.messagepriorityqueue.provider;

import com.rev.messagepriorityqueue.model.Message;

public interface ChannelListener {
	public void messageReceived(Message m);
}
