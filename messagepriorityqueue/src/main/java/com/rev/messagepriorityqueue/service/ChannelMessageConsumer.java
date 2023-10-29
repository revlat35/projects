package com.rev.messagepriorityqueue.service;

import java.util.Queue;

import com.rev.messagepriorityqueue.model.Message;

public class ChannelMessageConsumer implements Runnable {

	private Queue<Message> messageQueue;
	private Queue<Message> outQueue;
	private int transferSize;

	
	public ChannelMessageConsumer(Queue<Message> messageQueue, Queue<Message> outQueue, int transferSize) {
		this.messageQueue = messageQueue;
		this.outQueue = outQueue;
		this.transferSize = transferSize;
	}
  
	@Override
	public void run() {
		System.out.println("---------------------------------START QUEUE MESSAGE-----------------------------------");
        while (transferSize > 0) {
        	if(!messageQueue.isEmpty()) {
        		Message message = messageQueue.peek();
    			System.out.println("Message to queue:" + message.getMessageId());
    			boolean isAdded = outQueue.offer(message);
    			System.out.println("OUTQUEUE :" + printQueueItems(outQueue));
    			if (isAdded) {
    				messageQueue.poll();
    				System.out.println("Successfully added: " + message.getMessageId());
    			} else {
    				System.out.println("FAILED TO QUEUE!!!!!!!!!!!!!!" + message.getMessageId());
    			}
    			System.out.println("OUTQUEUE size: " + outQueue.size());
    			transferSize--;
        	}
		}
        System.out.println("---------------------------------END QUEUE MESSAGE-----------------------------------");
	}
	
	public static String printQueueItems(Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		return str;
	}

}
