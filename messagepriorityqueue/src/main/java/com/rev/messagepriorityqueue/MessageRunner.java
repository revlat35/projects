package com.rev.messagepriorityqueue;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.service.ChannelListenerService;
import com.rev.messagepriorityqueue.service.MessageGeneratorService;
import com.rev.messagepriorityqueue.service.OrderedMessageQueueService;

public class MessageRunner {

	
	private static final int countOfMessages = 1;
	

	public static void main(String[] args) {
		 OrderedMessageQueueService omqService = new OrderedMessageQueueService();
		 ChannelListenerService channelListener = new ChannelListenerService();
		 channelListener.registerChannelListener(omqService);
		 
		 
		 //pool of messages produced
		 ExecutorService messageProducerPool = Executors.newCachedThreadPool();
		 //pool of messages consumed
		 ExecutorService messageConsumerPool = Executors.newCachedThreadPool();

		
		//loop to produce random Messages with random messageID, sourceChannelId and payload
		MessageGeneratorService messageGenerator = new MessageGeneratorService();
	    Random random = new Random();

		
		
		
		//add the message to the messageProducerPool
		messageProducerPool.submit(() -> {
			int cnt = 1;
			while(true) {
				try {
					Message message = messageGenerator.generateRandomMessage(cnt++);
					channelListener.messageReceived(message);
					Thread.sleep(random.nextInt(5000));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		//consume messages from the outqueue
		messageConsumerPool.submit(() -> {
			while(true) {
				try {
					Thread.sleep(random.nextInt(1000));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Queue<Message> outQueue = channelListener.getOutQueue();
				 if (!outQueue.isEmpty()) {
					Message removedMessage = outQueue.poll();
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
					System.out.println("PROCESSED: >>>>>" + removedMessage.getMessageId() + "<<<<<");
					System.out.println("OUTQUEUE :" + printQueueItems(outQueue));
					System.out.println("OUTQUEUE SIZE: " + outQueue.size());
					System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
				} else {
					System.out.println("OUTQUEUE is EMPTY!");
				}
			}
		});
		messageProducerPool.shutdown();
		messageConsumerPool.shutdown();
	}
	
	
	public static String printQueueItems(Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		return str;
	}

}
