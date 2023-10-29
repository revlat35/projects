package com.rev.messagepriorityqueue.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.provider.ChannelListener;

public class ChannelListenerService implements ChannelListener {
	
	private OrderedMessageQueueService omqService;
	//pool of messages for queuing
	private ExecutorService messageListener;
	 //pool of messages produced
	private ExecutorService messageProducerPool;
	 //pool of messages consumed
	private ExecutorService messageConsumerPool;
	private Random random = new Random();
	 
	private final Map<String, Integer> transferSizeMap = new HashMap<String, Integer>() {{
 		put("cn-01", 1);
    	put("cn-02", 1);
    	put("cn-03", 3);
    	put("cn-04", 5);
 	}};
	
	public void registerChannelListener(OrderedMessageQueueService omqService) {
		this.omqService = omqService;
		this.messageListener = Executors.newCachedThreadPool();
		this.messageProducerPool = Executors.newCachedThreadPool();
		this.messageConsumerPool = Executors.newCachedThreadPool();
		
		//consume messages from the outqueue
		System.out.println("Starting Channel Message Listener...");
		
		//start listening for messages in the channels
		messageListener.submit(() -> {
			while(true) {
				Thread.sleep(2000);
				HashMap<String,Queue<Message>> messageQueueChannel = omqService.getMessageQueueChannel();
				if (!isMessageQueueChannelEmpty(messageQueueChannel)) {
					
					for(Entry<String,Queue<Message>> channel : messageQueueChannel.entrySet()) {
						Queue<Message> mq = (Queue<Message>) channel.getValue();
						if (!mq.isEmpty()) {
							omqService.consumeMessage(mq, omqService.getOutQueue(),
									transferSizeMap.get(channel.getKey()));
						}
					}
				}
			}
		});
		
		//consume messages from the outqueue
		messageConsumerPool.submit(() -> {
			while(true) {
				try {
					Thread.sleep(random.nextInt(2000));
					Queue<Message> outQueue = omqService.getOutQueue();
					System.out.println("---------------------------------START CONSUME MESSAGE-----------------------------------");
					 if (!outQueue.isEmpty()) {
						System.out.println("OUTQUEUE:" + printQueueItems(outQueue));
						Message processedMessage = outQueue.poll();
						System.out.println("Message consumed: [" + processedMessage.getMessageId() + "]");
					} else {
						printQueueItems(omqService.getMessageQueueChannel());
						System.out.println("OUTQUEUE is EMPTY!");
					}
					 System.out.println("---------------------------------END CONSUME MESSAGE-----------------------------------");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		messageListener.shutdown();
		messageConsumerPool.shutdown();
	}
	
	public static boolean isMessageQueueChannelEmpty(HashMap<String,Queue<Message>> messageQueueChannel) {
		boolean isEmpty = true;
		for(Entry<String,Queue<Message>> channel : messageQueueChannel.entrySet()) {
			if (!channel.getValue().isEmpty()) {
				isEmpty = false;
			}
		}
		System.out.println("Is messageQueueChannel empty?  : " + isEmpty);
		return isEmpty;
	}
	
	public static String printQueueItems(Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		return str;
	}
	
	public static void printQueueItems(HashMap<String,Queue<Message>> messageQueueChannel) {
		String str = "";
		for(Entry<String,Queue<Message>> item : messageQueueChannel.entrySet()) {
			str = "";
			for(Message m : item.getValue()) {
				str = str.concat("[" + m.getMessageId() + "]");
			}
			System.out.println(item.getKey() + " :" + str);
		}
	}
	
	@Override
	public void messageReceived(Message m) {
		messageProducerPool.submit(() -> {
			try {
				Thread.sleep(random.nextInt(500));
				omqService.queueMessage(m);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
	}
}
