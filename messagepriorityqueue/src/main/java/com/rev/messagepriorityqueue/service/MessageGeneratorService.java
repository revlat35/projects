package com.rev.messagepriorityqueue.service;
 
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.rev.messagepriorityqueue.model.Message;

public class MessageGeneratorService {
	
	private static final List<String> SOURCE_CHANNEL = Arrays.asList("cn-01", "cn-02", "cn-03", "cn-04");
	
	/*
	 * Method to generate random message
	 */
	public Message generateRandomMessage(int messageId) {
		Message message = new Message();
		Random randomGenerator = new Random();
		//long messageId = randomGenerator.nextLong();
	    byte[] byteArr = new byte[10];
	    
		
		message.setMessageId(messageId++);
		message.setSourceChannelId(SOURCE_CHANNEL.get(randomGenerator.nextInt(SOURCE_CHANNEL.size())));
	    
		randomGenerator.nextBytes(byteArr);
	    message.setPayload(byteArr);
		
		return message;
	}
}
