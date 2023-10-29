package com.rev.messagepriorityqueue;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.service.ChannelListenerService;
import com.rev.messagepriorityqueue.service.MessageGeneratorService;
import com.rev.messagepriorityqueue.service.OrderedMessageQueueService;

public class MessageRunner {

	
	private static final int countOfMessages = 30;
	private static final int outQueueCapacity = 5;

	public static void main(String[] args) {
		 OrderedMessageQueueService omqService = new OrderedMessageQueueService(outQueueCapacity);
		 ChannelListenerService channelListener = new ChannelListenerService();
		 channelListener.registerChannelListener(omqService);
		 
		
		//service to produce random Messages with random messageID, sourceChannelId and payload
		MessageGeneratorService messageGenerator = new MessageGeneratorService();
	    
	    //produce random messages
	    for(int i=0;i<countOfMessages;i++) {
	    	Message message = messageGenerator.generateRandomMessage(i);
	    	channelListener.messageReceived(message);
	    }

	}

}
