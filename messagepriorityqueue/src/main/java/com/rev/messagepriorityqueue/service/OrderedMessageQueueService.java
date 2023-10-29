package com.rev.messagepriorityqueue.service;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.provider.OrderedMessageQueueProvider;

public class OrderedMessageQueueService implements OrderedMessageQueueProvider {

	private static ExecutorService channelListenerPool;
	private Queue<Message> outQueue;
	private HashMap<String,Queue<Message>> messageQueueChannel;
	private Lock lock;
 	
 	public HashMap<String, Queue<Message>> getMessageQueueChannel() {
		return messageQueueChannel;
	}
	
	
	public OrderedMessageQueueService(int outQueueCapacity) {
		this.outQueue = new LinkedBlockingQueue<Message>(outQueueCapacity);
		this.messageQueueChannel = new HashMap<String,Queue<Message>>();
		this.lock = new ReentrantLock();
		channelListenerPool = Executors.newCachedThreadPool();
	}


	@Override
	public void queueMessage(Message m) {
		lock.lock();
		try {
			System.out.println("---------------------------------START PRODUCE MESSAGE-----------------------------------");
			System.out.println("Produced Message: " + m.toString());
			
			//check first if the channel is already in the HashMap
			if (!messageQueueChannel.containsKey(m.getSourceChannelId())) {
				messageQueueChannel.put(m.getSourceChannelId(), new LinkedBlockingQueue<Message>());
			}
			messageQueueChannel.get(m.getSourceChannelId()).add(m);
			printQueueItems(messageQueueChannel);
			System.out.println("---------------------------------END PRODUCE MESSAGE-----------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
		
	} 
	
	public static void printQueueItems(HashMap<String,Queue<Message>> messageQueueChannel) {
		String str = "";
		for(Entry<String,Queue<Message>> channel : messageQueueChannel.entrySet()) {
			str = "";
			for(Message m : channel.getValue()) {
				str = str.concat("[" + m.getMessageId() + "]");
			}
			System.out.println(channel.getKey() + " :" + str);
		}
	}
	
	public static String printOutQueueItems(Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		return str;
	}
	
	public void consumeMessage(Queue<Message> messageQueue, Queue<Message> outQueue, int transferSize) {
		lock.lock(); 
		try {
			ChannelMessageConsumer consumer =
			          new ChannelMessageConsumer(messageQueue, outQueue, transferSize);
			 channelListenerPool.submit(consumer);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Queue<Message> getOutQueue() {
		return outQueue;
	}
	


}
