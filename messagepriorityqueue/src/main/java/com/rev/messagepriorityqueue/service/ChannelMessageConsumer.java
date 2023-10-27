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
		while (transferSize > 0) {
			Message message = messageQueue.peek();
			boolean isAdded = outQueue.offer(message);
			System.out.println("OUTQUEUE :" + printQueueItems(outQueue));
			if (isAdded) {
				messageQueue.poll();
				System.out.println("OUTQUEUE SIZE: " + outQueue.size());
			} else {
				System.out.println("FAILED TO QUEUE: !!!!!!!!!!!!!!" + message.toString());
				System.out.println("OUTQUEUE SIZE: " + outQueue.size());
			}
			transferSize--;
		}
	}
	
	public static String printQueueItems(Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		return str;
	}

}
